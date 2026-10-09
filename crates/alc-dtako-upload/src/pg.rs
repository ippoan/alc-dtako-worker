//! [`crate::repo::sql`] の定数を流す tokio-postgres の実装 (ippoan/alc-vein-worker の `pg.rs` の形。
//! Refs ippoan/rust-alc-api#725)。直下の worker と、組み込みの PostgreSQL に流すテスト (`tests/sql_db.rs`) が、
//! 同じこの実装を使う。**接続は持たない** (張るのは worker とテスト)。ログも出さない (出すのは呼び手)。
//!
//! SQL は [`crate::repo::sql`] の定数だけを、共通 crate `alc-worker-db` の `TenantTx` が出す
//! **型付きの名前なしの文** (`query_typed` 系) で流す。名前付き prepared statement は
//! Hyperdrive 経由で接続が切れるので使わない (`TenantTx` に口が無い)。引数の型の並びが書かれるのは、
//! このファイルの 1 か所だけ (`$n` の意味は `repo::sql` の各定数の doc)。
//!
//! ## RLS と 2 つの層
//!
//! - **段の関数** (`&mut PgClient` を取る): `PgClient::tenant_tx` を 1 回開く = 1 関数 1 トランザクション。順は `BEGIN` →
//!   `alc_worker_db::SET_TENANT` (`set_config('app.current_tenant_id', $1, true)` と search_path) → 本文 → `COMMIT`。
//!   戻り値は `TxOutput` (owned な型) に限られ、`Row` を transaction の外へは持ち出せない。
//! - **文の関数** (`&TenantTx` を取る): transaction を開かず、段の関数の中から呼ぶ。`TenantTx` は `tenant_tx` の中でしか
//!   手に入らないので、テナントを設定していない transaction では呼べない。
//!
//! アップロードの取り込みの段は、[`create_upload`] → [`set_upload_zip_key`] → [`prepare_upload`] → [`apply_upload`]
//! (失敗したら [`mark_upload_failed`])。[`apply_upload`] は「運行の入れ替え → 日別の要再計算の印 → 完了の印」を 1 つの
//! transaction で行う (途中で落ちたら、運行も印も履歴も元のまま)。取り込みは日別を書かない (Refs ippoan/alc-dtako-worker#23)。
//! 日別は再計算の口が [`save_daily_hours_in_tx`] で保存し、同じ transaction の中で、その 乗務員 × 月 の印を消す。

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use alc_compare::upload_daily::DailyHours;
use alc_compare::DayKey;
use alc_csv_parser::kudgivt::KudgivtRow;
use alc_csv_parser::kudguri::KudguriRow;
use alc_csv_parser::operation_changes::{
    compose_snapshot, record_driver_cd, snapshot_changed, OperationMinutes,
};
use alc_csv_parser::work_segments::{default_classification, EventClass};
use alc_worker_db::{PgClient, TenantTx, TxOutput};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::repo::sql;

/// [`sql::SELECT_UPLOAD_ZIP_KEY`]。行が無い・`r2_zip_key` が NULL は、どちらも `Ok(None)`。
pub async fn upload_zip_key(
    pg: &mut PgClient,
    tenant_id: Uuid,
    upload_id: Uuid,
) -> Result<Option<String>, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::SELECT_UPLOAD_ZIP_KEY,
                    &[(&upload_id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(row.and_then(|r| r.get(0)))
        })
    })
    .await
}

/// [`sql::MARK_HAS_KUDGIVT`]。返すのは `RETURNING` の値そのまま (重複を除かない。呼び手が集合にする)。
/// `unko_nos` が空なら transaction を開かずに空を返す。
pub async fn mark_has_kudgivt(
    pg: &mut PgClient,
    tenant_id: Uuid,
    unko_nos: Vec<String>,
) -> Result<Vec<String>, tokio_postgres::Error> {
    if unko_nos.is_empty() {
        return Ok(Vec::new());
    }
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::MARK_HAS_KUDGIVT,
                    &[(&tenant_id, Type::UUID), (&unko_nos, Type::TEXT_ARRAY)],
                )
                .await?;
            Ok(rows.into_iter().map(|r| r.get(0)).collect())
        })
    })
    .await
}

/// [`sql::LIST_UPLOADS_NEEDING_SPLIT`]。`(id, filename)` を新しい順に返す。
pub async fn uploads_needing_split(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<(Uuid, String)>, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_UPLOADS_NEEDING_SPLIT, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
        })
    })
    .await
}

// ---- アップロードの取り込み ----

/// 月の再計算の対象の運行 1 行 ([`sql::LIST_OPERATIONS_FOR_RECALC`])。
#[derive(Debug, Clone, PartialEq)]
pub struct RecalcOperationRow {
    pub unko_no: String,
    pub reading_date: NaiveDate,
    pub operation_date: Option<NaiveDate>,
    pub departure_at: Option<DateTime<Utc>>,
    pub return_at: Option<DateTime<Utc>>,
    pub driver_cd: Option<String>,
    pub total_distance: Option<f64>,
    pub drive_time_general: Option<i32>,
    pub drive_time_highway: Option<i32>,
    pub drive_time_bypass: Option<i32>,
}

impl TxOutput for RecalcOperationRow {}

/// [`sql::LIST_OPERATIONS_FOR_RECALC`]。`fetch_end` は月末の翌日 (範囲は両端を含む)。
pub async fn operations_for_recalc(
    pg: &mut PgClient,
    tenant_id: Uuid,
    month_start: NaiveDate,
    fetch_end: NaiveDate,
) -> Result<Vec<RecalcOperationRow>, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 3] = [
                (&tenant_id, Type::UUID),
                (&month_start, Type::DATE),
                (&fetch_end, Type::DATE),
            ];
            let rows = tx
                .query_typed(sql::LIST_OPERATIONS_FOR_RECALC, &params)
                .await?;
            let row = |r: tokio_postgres::Row| RecalcOperationRow {
                unko_no: r.get(0),
                reading_date: r.get(1),
                operation_date: r.get(2),
                departure_at: r.get(3),
                return_at: r.get(4),
                driver_cd: r.get(5),
                total_distance: r.get(6),
                drive_time_general: r.get(7),
                drive_time_highway: r.get(8),
                drive_time_bypass: r.get(9),
            };
            Ok(rows.into_iter().map(row).collect())
        })
    })
    .await
}

/// 乗務員 1 人の乗務員CD ([`sql::SELECT_DRIVER_CD`]) と、月の再計算の対象の運行
/// ([`sql::LIST_DRIVER_OPERATIONS_FOR_RECALC`]。`driver_cd` には引いた乗務員CD を入れる) を 1 transaction で。
/// 乗務員が無い・乗務員CD が NULL なら `None` (運行を引かない)。`fetch_end` は月末の翌日。
pub async fn driver_operations_for_recalc(
    pg: &mut PgClient,
    tenant_id: Uuid,
    driver_id: Uuid,
    month_start: NaiveDate,
    fetch_end: NaiveDate,
) -> Result<Option<(String, Vec<RecalcOperationRow>)>, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 2] =
                [(&driver_id, Type::UUID), (&tenant_id, Type::UUID)];
            let driver_cd = tx.query_typed_opt(sql::SELECT_DRIVER_CD, &params).await?;
            let Some(driver_cd) = driver_cd.and_then(|r| r.get::<_, Option<String>>(0)) else {
                return Ok(None);
            };
            let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 4] = [
                (&tenant_id, Type::UUID),
                (&driver_id, Type::UUID),
                (&month_start, Type::DATE),
                (&fetch_end, Type::DATE),
            ];
            let rows = tx
                .query_typed(sql::LIST_DRIVER_OPERATIONS_FOR_RECALC, &params)
                .await?;
            let row = |r: tokio_postgres::Row| RecalcOperationRow {
                unko_no: r.get(0),
                reading_date: r.get(1),
                operation_date: r.get(2),
                departure_at: r.get(3),
                return_at: r.get(4),
                driver_cd: Some(driver_cd.clone()),
                total_distance: r.get(5),
                drive_time_general: r.get(6),
                drive_time_highway: r.get(7),
                drive_time_bypass: r.get(8),
            };
            let ops = rows.into_iter().map(row).collect();
            Ok(Some((driver_cd, ops)))
        })
    })
    .await
}

/// 日別の保存の段 (1 transaction): [`save_daily_hours_with`] を流し、`clear` が在れば同じ transaction の中で
/// [`clear_recalc_pending`] を流す (保存が落ちたら印も残る)。再計算が乗務員ごとに呼ぶ。
pub async fn save_daily_hours_in_tx(
    pg: &mut PgClient,
    tenant_id: Uuid,
    daily: HashMap<DayKey, DailyHours>,
    all_unko_nos: Arc<Vec<String>>,
    clear: Option<PendingClear>,
) -> Result<(), tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            save_daily_hours_with(tx, tenant_id, &daily, &all_unko_nos).await?;
            if let Some(clear) = clear {
                clear_recalc_pending(tx, tenant_id, &clear).await?;
            }
            Ok(())
        })
    })
    .await
}

/// 計算し直した 乗務員 × 月 の印の消し方 ([`sql::DELETE_RECALC_PENDING`])。乗務員は `driver_id` か `driver_cd` で指す。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingClear {
    /// 月初
    pub month: NaiveDate,
    /// 乗務員の id (乗務員ごとの再計算・印の口。運行の `driver_id` と同じもの)
    pub driver_id: Option<Uuid>,
    /// 乗務員CD (月の全員の再計算。その CD の乗務員の印)
    pub driver_cd: Option<String>,
    /// これより後に付いた印は消さない (計算の間に別の取り込みが付けた印を残すため)
    pub before: DateTime<Utc>,
}

/// [`sql::DELETE_RECALC_PENDING`] (transaction は開かない。呼ぶのは [`save_daily_hours_in_tx`] の 1 か所)。返すのは消した数。
pub async fn clear_recalc_pending(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    clear: &PendingClear,
) -> Result<u64, tokio_postgres::Error> {
    let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 5] = [
        (&tenant_id, Type::UUID),
        (&clear.month, Type::DATE),
        (&clear.before, Type::TIMESTAMPTZ),
        (&clear.driver_id, Type::UUID),
        (&clear.driver_cd, Type::TEXT),
    ];
    tx.execute_typed(sql::DELETE_RECALC_PENDING, &params).await
}

/// [`sql::SELECT_NOW`] (DB の今の時刻。再計算の口が運行を読み始める前に引き、印を消す条件にする)。
pub async fn db_now(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<DateTime<Utc>, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move { Ok(tx.query_typed_one(sql::SELECT_NOW, &[]).await?.get(0)) })
    })
    .await
}

/// 日別の「要再計算」の印 1 つ ([`sql::LIST_RECALC_PENDING`])。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMark {
    pub driver_id: Uuid,
    /// 月初
    pub month: NaiveDate,
    pub created_at: DateTime<Utc>,
    /// 印を読んだ時刻 (どの行も同じ値)
    pub read_at: DateTime<Utc>,
}

impl TxOutput for PendingMark {}

/// [`sql::LIST_RECALC_PENDING`]。テナントの印の全部 (月・乗務員の順)。
pub async fn recalc_pending_marks(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<PendingMark>, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_RECALC_PENDING, &[(&tenant_id, Type::UUID)])
                .await?;
            let mark = |r: tokio_postgres::Row| PendingMark {
                driver_id: r.get(0),
                month: r.get(1),
                created_at: r.get(2),
                read_at: r.get(3),
            };
            Ok(rows.into_iter().map(mark).collect())
        })
    })
    .await
}

/// 日付の月初。
pub fn month_of(date: NaiveDate) -> NaiveDate {
    date.with_day(1).expect("the first day of a month")
}

/// 乗務員 × 運行の読取日・運行日の月 (乗務員が無ければ空。月末の運行なら 2 つ)。
fn recalc_keys(
    driver_id: Option<Uuid>,
    reading_date: NaiveDate,
    operation_date: Option<NaiveDate>,
) -> Vec<(Uuid, NaiveDate)> {
    let dates = [Some(reading_date), operation_date].into_iter().flatten();
    let months = dates.map(month_of);
    driver_id.map_or_else(Vec::new, |id| months.map(|m| (id, m)).collect())
}

/// 印を付け直す (transaction は開かない): [`sql::DELETE_RECALC_PENDING_FOR_MARKS`] → [`sql::INSERT_RECALC_PENDING`]。
/// 既に印が在っても時刻が新しくなるので、再計算の口が印を読んだ後に付いた印は、その口の「読んだ時刻まで」の消去に当たらない
/// (ON CONFLICT DO NOTHING だけだと古い時刻のまま消され、計算に入らなかった変化の印が無くなる)。空なら DB を触らない。
pub async fn insert_recalc_pending(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    marks: &BTreeSet<(Uuid, NaiveDate)>,
) -> Result<u64, tokio_postgres::Error> {
    if marks.is_empty() {
        return Ok(0);
    }
    let driver_ids: Vec<Uuid> = marks.iter().map(|(id, _)| *id).collect();
    let months: Vec<NaiveDate> = marks.iter().map(|(_, month)| *month).collect();
    let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 3] = [
        (&tenant_id, Type::UUID),
        (&driver_ids, Type::UUID_ARRAY),
        (&months, Type::DATE_ARRAY),
    ];
    tx.execute_typed(sql::DELETE_RECALC_PENDING_FOR_MARKS, &params)
        .await?;
    tx.execute_typed(sql::INSERT_RECALC_PENDING, &params).await
}

/// 履歴の一覧の 1 行 ([`sql::LIST_UPLOADS`])。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadRow {
    pub id: Uuid,
    pub filename: String,
    pub status: String,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub r2_zip_key: Option<String>,
}

impl TxOutput for UploadRow {}

/// やり直し待ち・失敗の履歴の一覧の 1 行 ([`sql::LIST_PENDING_UPLOADS`])。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingUploadRow {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub filename: String,
    pub status: String,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl TxOutput for PendingUploadRow {}

/// [`sql::LIST_UPLOADS`]。
pub async fn list_uploads(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<UploadRow>, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_UPLOADS, &[(&tenant_id, Type::UUID)])
                .await?;
            let row = |r: tokio_postgres::Row| UploadRow {
                id: r.get(0),
                filename: r.get(1),
                status: r.get(2),
                error_message: r.get(3),
                created_at: r.get(4),
                r2_zip_key: r.get(5),
            };
            Ok(rows.into_iter().map(row).collect())
        })
    })
    .await
}

/// [`sql::LIST_PENDING_UPLOADS`]。
pub async fn list_pending_uploads(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<PendingUploadRow>, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_PENDING_UPLOADS, &[(&tenant_id, Type::UUID)])
                .await?;
            let row = |r: tokio_postgres::Row| PendingUploadRow {
                id: r.get(0),
                tenant_id: r.get(1),
                filename: r.get(2),
                status: r.get(3),
                error_message: r.get(4),
                created_at: r.get(5),
            };
            Ok(rows.into_iter().map(row).collect())
        })
    })
    .await
}

/// [`sql::SELECT_UPLOAD_DOWNLOAD`] → `(r2_zip_key, filename)`。行が無ければ `None` (key が NULL の行は `Some((None, _))`)。
pub async fn upload_download(
    pg: &mut PgClient,
    tenant_id: Uuid,
    upload_id: Uuid,
) -> Result<Option<(Option<String>, String)>, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 2] =
                [(&upload_id, Type::UUID), (&tenant_id, Type::UUID)];
            let row = tx
                .query_typed_opt(sql::SELECT_UPLOAD_DOWNLOAD, &params)
                .await?;
            Ok(row.map(|r| (r.get(0), r.get(1))))
        })
    })
    .await
}

/// 履歴の `tenant_id` の外部キー制約の名前 (テナントが存在しないときに当たる)。
const UPLOAD_HISTORY_TENANT_FK: &str = "dtako_upload_history_tenant_id_fkey";

/// 変更記録の `reason` (上げ直し)。
const REASON_REUPLOAD: &str = "reupload";

/// [`create_upload`] の失敗。
#[derive(Debug)]
pub enum CreateUploadError {
    /// テナントが存在しない (履歴の `tenant_id` の外部キーに当たった)
    TenantNotFound,
    /// それ以外の DB の失敗
    Db(tokio_postgres::Error),
}

fn create_upload_error(e: tokio_postgres::Error) -> CreateUploadError {
    let missing_tenant = e.as_db_error().is_some_and(|db| {
        db.code() == &SqlState::FOREIGN_KEY_VIOLATION
            && db.constraint() == Some(UPLOAD_HISTORY_TENANT_FK)
    });
    if missing_tenant {
        CreateUploadError::TenantNotFound
    } else {
        CreateUploadError::Db(e)
    }
}

/// [`sql::INSERT_UPLOAD_HISTORY`]。履歴を作り、その id を返す。
pub async fn create_upload(
    pg: &mut PgClient,
    tenant_id: Uuid,
    filename: String,
) -> Result<Uuid, CreateUploadError> {
    let created = pg
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 2] =
                    [(&tenant_id, Type::UUID), (&filename, Type::TEXT)];
                let row = tx
                    .query_typed_one(sql::INSERT_UPLOAD_HISTORY, &params)
                    .await?;
                Ok(row.get(0))
            })
        })
        .await;
    created.map_err(create_upload_error)
}

/// [`sql::UPDATE_UPLOAD_ZIP_KEY`]。**単独の transaction** (後の段が落ちても key は残る)。返すのは更新した行数
/// (履歴が無い・別テナントの履歴なら 0)。
pub async fn set_upload_zip_key(
    pg: &mut PgClient,
    tenant_id: Uuid,
    upload_id: Uuid,
    r2_zip_key: String,
) -> Result<u64, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::UPDATE_UPLOAD_ZIP_KEY,
                &[
                    (&r2_zip_key, Type::TEXT),
                    (&upload_id, Type::UUID),
                    (&tenant_id, Type::UUID),
                ],
            )
            .await
        })
    })
    .await
}

/// [`sql::MARK_UPLOAD_FAILED`]。`label` は呼び手が渡す固定の語 (生のエラー文を入れない)。返すのは更新した行数。
pub async fn mark_upload_failed(
    pg: &mut PgClient,
    tenant_id: Uuid,
    upload_id: Uuid,
    label: String,
) -> Result<u64, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::MARK_UPLOAD_FAILED,
                &[
                    (&label, Type::TEXT),
                    (&upload_id, Type::UUID),
                    (&tenant_id, Type::UUID),
                ],
            )
            .await
        })
    })
    .await
}

/// [`sql::UPSERT_OFFICE`]。cd が空なら DB を引かずに `None`。
pub async fn upsert_office(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    office_cd: &str,
    office_name: &str,
) -> Result<Option<Uuid>, tokio_postgres::Error> {
    if office_cd.is_empty() {
        return Ok(None);
    }
    let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 3] = [
        (&tenant_id, Type::UUID),
        (&office_cd, Type::TEXT),
        (&office_name, Type::TEXT),
    ];
    let row = tx.query_typed_one(sql::UPSERT_OFFICE, &params).await?;
    Ok(Some(row.get(0)))
}

/// [`sql::UPSERT_VEHICLE`]。cd が空なら DB を引かずに `None`。
pub async fn upsert_vehicle(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    vehicle_cd: &str,
    vehicle_name: &str,
) -> Result<Option<Uuid>, tokio_postgres::Error> {
    if vehicle_cd.is_empty() {
        return Ok(None);
    }
    let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 3] = [
        (&tenant_id, Type::UUID),
        (&vehicle_cd, Type::TEXT),
        (&vehicle_name, Type::TEXT),
    ];
    let row = tx.query_typed_one(sql::UPSERT_VEHICLE, &params).await?;
    Ok(Some(row.get(0)))
}

/// 運行の `driver_id` に入れる乗務員を解決する。cd が空なら DB を引かずに `None`。
///
/// 乗務員CD は `code` 列に入っていることも `driver_cd` 列に入っていることも在るので、順に当てる (1 つの `OR` にはしない):
/// 1. [`sql::SELECT_EMPLOYEE_BY_CODE`] で当たり、その行が `driver_cd` を持っていれば、その id
/// 2. 当たったが `driver_cd` が NULL なら [`sql::FILL_EMPLOYEE_DRIVER_CD`] で埋める。1 行以上更新できれば、その id
///    (同じ driver_cd を持つ別の生存行が在ると 0 行。そのときは下へ)
/// 3. [`sql::SELECT_EMPLOYEE_BY_DRIVER_CD`] で当たれば、その id
/// 4. [`sql::INSERT_EMPLOYEE`] で作る。作れれば、その id
/// 5. 一意の制約に当たって作れなかったら、もう一度 [`sql::SELECT_EMPLOYEE_BY_DRIVER_CD`] (無ければ `None`)
pub async fn upsert_driver(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    driver_cd: &str,
    driver_name: &str,
) -> Result<Option<Uuid>, tokio_postgres::Error> {
    if driver_cd.is_empty() {
        return Ok(None);
    }
    let key: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 2] =
        [(&tenant_id, Type::UUID), (&driver_cd, Type::TEXT)];

    let by_code = tx
        .query_typed_opt(sql::SELECT_EMPLOYEE_BY_CODE, &key)
        .await?;
    if let Some(row) = by_code {
        let id: Uuid = row.get(0);
        let existing_driver_cd: Option<String> = row.get(1);
        if existing_driver_cd.is_some() {
            return Ok(Some(id));
        }
        let fill: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 3] = [
            (&tenant_id, Type::UUID),
            (&driver_cd, Type::TEXT),
            (&id, Type::UUID),
        ];
        let updated = tx
            .execute_typed(sql::FILL_EMPLOYEE_DRIVER_CD, &fill)
            .await?;
        if updated > 0 {
            return Ok(Some(id));
        }
    }

    let existing = tx
        .query_typed_opt(sql::SELECT_EMPLOYEE_BY_DRIVER_CD, &key)
        .await?;
    if let Some(row) = existing {
        return Ok(Some(row.get(0)));
    }

    let new: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 3] = [
        (&tenant_id, Type::UUID),
        (&driver_cd, Type::TEXT),
        (&driver_name, Type::TEXT),
    ];
    let inserted = tx.query_typed_opt(sql::INSERT_EMPLOYEE, &new).await?;
    if let Some(row) = inserted {
        return Ok(Some(row.get(0)));
    }

    // 一意の制約に当たって入らなかった (= 同じ driver_cd の行を誰かが入れた) ので引き直す
    let raced = tx
        .query_typed_opt(sql::SELECT_EMPLOYEE_BY_DRIVER_CD, &key)
        .await?;
    Ok(raced.map(|row| row.get(0)))
}

/// [`sql::SELECT_OPERATION_EXISTS`]。
pub async fn operation_exists(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    unko_no: &str,
    crew_role: i32,
) -> Result<bool, tokio_postgres::Error> {
    let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 3] = [
        (&tenant_id, Type::UUID),
        (&unko_no, Type::TEXT),
        (&crew_role, Type::INT4),
    ];
    let row = tx
        .query_typed_one(sql::SELECT_OPERATION_EXISTS, &params)
        .await?;
    Ok(row.get(0))
}

/// [`sql::SELECT_EVENT_CLASSIFICATIONS`]。`(event_cd, 分類の文字列)` の一覧。
pub async fn load_event_classifications(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
) -> Result<Vec<(String, String)>, tokio_postgres::Error> {
    let rows = tx
        .query_typed(
            sql::SELECT_EVENT_CLASSIFICATIONS,
            &[(&tenant_id, Type::UUID)],
        )
        .await?;
    Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// [`sql::INSERT_EVENT_CLASSIFICATION`]。在れば何もしない。
pub async fn insert_event_classification(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    event_cd: &str,
    event_name: &str,
    classification: &str,
) -> Result<(), tokio_postgres::Error> {
    let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 4] = [
        (&tenant_id, Type::UUID),
        (&event_cd, Type::TEXT),
        (&event_name, Type::TEXT),
        (&classification, Type::TEXT),
    ];
    tx.execute_typed(sql::INSERT_EVENT_CLASSIFICATION, &params)
        .await?;
    Ok(())
}

/// 分類を読み、KUDGIVT に出てくる未登録のイベントCD を既定の分類で足す (KUDGIVT の行の順に 1 回ずつ)。
/// 返すのは、読んだ分類と足した分類を合わせた `(event_cd, 分類の文字列)` の一覧。
pub async fn load_or_init_classifications(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    kudgivt_rows: &[KudgivtRow],
) -> Result<Vec<(String, String)>, tokio_postgres::Error> {
    let mut classifications = load_event_classifications(tx, tenant_id).await?;
    let mut seen: HashSet<String> = classifications.iter().map(|(cd, _)| cd.clone()).collect();
    for row in kudgivt_rows {
        if seen.contains(&row.event_cd) {
            continue;
        }
        seen.insert(row.event_cd.clone());
        let (classification, _) = default_classification(&row.event_cd);
        insert_event_classification(
            tx,
            tenant_id,
            &row.event_cd,
            &row.event_name,
            classification,
        )
        .await?;
        classifications.push((row.event_cd.clone(), classification.to_string()));
    }
    Ok(classifications)
}

/// KUDGURI の 1 行ぶんの準備の結果 ([`prepare_upload`])。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedRow {
    pub office_id: Option<Uuid>,
    pub vehicle_id: Option<Uuid>,
    pub driver_id: Option<Uuid>,
    /// この行の運行 (運行NO・crew_role) が既に在るか。DB に在る、または同じ zip の中で先の行に同じものが出た
    pub exists: bool,
}

/// [`prepare_upload`] の結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedUpload {
    /// KUDGURI の行と同じ順・同じ数
    pub rows: Vec<PreparedRow>,
    /// `(event_cd, 分類の文字列)`。DB に在ったものと、今回足したもの
    pub classifications: Vec<(String, String)>,
}

impl TxOutput for PreparedUpload {}

impl PreparedUpload {
    /// 分類を `event_cd → EventClass` にする (日別の集計に渡す形)。
    pub fn classification_map(&self) -> HashMap<String, EventClass> {
        let pairs = self.classifications.iter();
        pairs
            .map(|(cd, cls)| (cd.clone(), EventClass::from_classification_str(cls)))
            .collect()
    }
}

/// 取り込みの準備 (1 transaction): **KUDGURI の行の順に** 営業所・車輌・乗務員を解決して運行が既に在るかを見て、
/// 続けて分類を読み、未登録のイベントCD を足す。
pub async fn prepare_upload(
    pg: &mut PgClient,
    tenant_id: Uuid,
    rows: Arc<Vec<KudguriRow>>,
    kudgivt_rows: Arc<Vec<KudgivtRow>>,
) -> Result<PreparedUpload, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let mut prepared = Vec::with_capacity(rows.len());
            let mut seen: HashSet<(String, i32)> = HashSet::new();
            for row in rows.iter() {
                let office_id =
                    upsert_office(tx, tenant_id, &row.office_cd, &row.office_name).await?;
                let vehicle_id =
                    upsert_vehicle(tx, tenant_id, &row.vehicle_cd, &row.vehicle_name).await?;
                let driver_id =
                    upsert_driver(tx, tenant_id, &row.driver_cd, &row.driver_name).await?;
                let in_db = operation_exists(tx, tenant_id, &row.unko_no, row.crew_role).await?;
                // 同じ zip の中の重複: 先の行が入った後なので、後の行から見れば「既に在る」
                let first_in_zip = seen.insert((row.unko_no.clone(), row.crew_role));
                prepared.push(PreparedRow {
                    office_id,
                    vehicle_id,
                    driver_id,
                    exists: in_db || !first_in_zip,
                });
            }
            let classifications =
                load_or_init_classifications(tx, tenant_id, &kudgivt_rows).await?;
            Ok(PreparedUpload {
                rows: prepared,
                classifications,
            })
        })
    })
    .await
}

/// 運行 1 件を入れ替えるのに、KUDGURI の行の外から要る値。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationInput {
    pub office_id: Option<Uuid>,
    pub vehicle_id: Option<Uuid>,
    pub driver_id: Option<Uuid>,
    /// 前回の分数 (保存先の旧 KUDGIVT から)。取れなかったら `None` (分数は比較から外し、before に印を残す)
    pub before_minutes: Option<OperationMinutes>,
    /// 今回の zip の KUDGIVT から出した分数
    pub after_minutes: OperationMinutes,
    /// KUDGURI の行の外に、日別に効く変化が在る (前回の KUDGIVT と今回の KUDGIVT が違う・前回が読めない・やり直し)。
    /// 既に在る運行でも、この運行の乗務員 × 月 に印を付ける
    pub recalc: bool,
}

/// [`sql::SELECT_OPERATION_SNAPSHOT`] で 1 運行・1 crew_role の snapshot を読む (無ければ `None`)。
async fn operation_snapshot(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    unko_no: &str,
    crew_role: i32,
) -> Result<Option<serde_json::Value>, tokio_postgres::Error> {
    let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 3] = [
        (&tenant_id, Type::UUID),
        (&unko_no, Type::TEXT),
        (&Some(crew_role), Type::INT4),
    ];
    let mut rows = tx
        .query_typed(sql::SELECT_OPERATION_SNAPSHOT, &params)
        .await?;
    Ok(rows.pop().map(|r| r.get(1)))
}

/// 日時の壁時計を、そのまま UTC の時刻として渡す (`TIMESTAMPTZ` の列へ)。
fn wall_clock_utc(at: Option<NaiveDateTime>) -> Option<chrono::DateTime<chrono::Utc>> {
    at.map(|naive| naive.and_utc())
}

/// 運行 1 件 (運行NO・crew_role) を入れ替え、前の行と違えば変更記録を 1 件残す。返すのは、記録を残したか。
///
/// 順: 旧 snapshot → [`sql::DELETE_OPERATION`] → [`sql::INSERT_OPERATION`] → **旧が在ったときだけ** 新 snapshot と比べ、
/// 違えば [`sql::INSERT_OPERATION_CHANGE`]。初回の取り込み (旧なし) は記録しない。
/// snapshot への分数の足し方・比べ方・記録に載せる乗務員CD は `alc_csv_parser::operation_changes` (backend と共有)。
pub async fn replace_operation(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    upload_id: Uuid,
    row: &KudguriRow,
    input: &OperationInput,
) -> Result<bool, tokio_postgres::Error> {
    let unko_no = row.unko_no.as_str();
    let crew_role = row.crew_role;
    let before = operation_snapshot(tx, tenant_id, unko_no, crew_role).await?;
    let before = before.map(|v| compose_snapshot(v, input.before_minutes.as_ref()));

    let key: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 3] = [
        (&tenant_id, Type::UUID),
        (&unko_no, Type::TEXT),
        (&crew_role, Type::INT4),
    ];
    tx.execute_typed(sql::DELETE_OPERATION, &key).await?;

    let r2_key_prefix = format!("{}/unko/{}", tenant_id, row.unko_no);
    let departure_at = wall_clock_utc(row.departure_at);
    let return_at = wall_clock_utc(row.return_at);
    let garage_out_at = wall_clock_utc(row.garage_out_at);
    let garage_in_at = wall_clock_utc(row.garage_in_at);
    let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 23] = [
        (&tenant_id, Type::UUID),
        (&row.unko_no, Type::TEXT),
        (&row.crew_role, Type::INT4),
        (&row.reading_date, Type::DATE),
        (&row.operation_date, Type::DATE),
        (&input.office_id, Type::UUID),
        (&input.vehicle_id, Type::UUID),
        (&input.driver_id, Type::UUID),
        (&departure_at, Type::TIMESTAMPTZ),
        (&return_at, Type::TIMESTAMPTZ),
        (&garage_out_at, Type::TIMESTAMPTZ),
        (&garage_in_at, Type::TIMESTAMPTZ),
        (&row.meter_start, Type::FLOAT8),
        (&row.meter_end, Type::FLOAT8),
        (&row.total_distance, Type::FLOAT8),
        (&row.drive_time_general, Type::INT4),
        (&row.drive_time_highway, Type::INT4),
        (&row.drive_time_bypass, Type::INT4),
        (&row.safety_score, Type::FLOAT8),
        (&row.economy_score, Type::FLOAT8),
        (&row.total_score, Type::FLOAT8),
        (&row.raw_data, Type::JSONB),
        (&r2_key_prefix, Type::TEXT),
    ];
    tx.execute_typed(sql::INSERT_OPERATION, &params).await?;

    let Some(before) = before else {
        return Ok(false);
    };
    let after = operation_snapshot(tx, tenant_id, unko_no, crew_role).await?;
    let after = after.map(|v| compose_snapshot(v, Some(&input.after_minutes)));
    let after = after.unwrap_or(serde_json::Value::Null);
    if !snapshot_changed(&before, &after) {
        return Ok(false);
    }
    let driver_cd = record_driver_cd(Some(&before), Some(&after));
    let change: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 8] = [
        (&tenant_id, Type::UUID),
        (&unko_no, Type::TEXT),
        (&crew_role, Type::INT4),
        (&driver_cd, Type::TEXT),
        (&upload_id, Type::UUID),
        (&REASON_REUPLOAD, Type::TEXT),
        (&before, Type::JSONB),
        (&after, Type::JSONB),
    ];
    tx.execute_typed(sql::INSERT_OPERATION_CHANGE, &change)
        .await?;
    Ok(true)
}

/// 入れ替える前の運行の (乗務員, 読取日, 運行日)。キーは (運行NO, crew_role)。
type RecalcKeys = HashMap<(String, i32), (Option<Uuid>, NaiveDate, Option<NaiveDate>)>;

/// [`sql::LIST_OPERATION_RECALC_KEYS`] (transaction は開かない)。
async fn operation_recalc_keys(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    rows: &[KudguriRow],
) -> Result<RecalcKeys, tokio_postgres::Error> {
    let unko_nos: Vec<&str> = rows.iter().map(|r| r.unko_no.as_str()).collect();
    let params: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 2] =
        [(&tenant_id, Type::UUID), (&unko_nos, Type::TEXT_ARRAY)];
    let found = tx
        .query_typed(sql::LIST_OPERATION_RECALC_KEYS, &params)
        .await?;
    let key = |r: tokio_postgres::Row| ((r.get(0), r.get(1)), (r.get(2), r.get(3), r.get(4)));
    Ok(found.into_iter().map(key).collect())
}

/// KUDGURI の行の順に [`replace_operation`] を流す (transaction は開かない。呼び手の transaction の中で使う)。
/// `inputs` は `rows` と同じ順・同じ数。返すのは流した行数 (運行NO の種類の数ではなく、行の数) と、
/// 日別の要再計算の印を付ける (乗務員, 月初):
/// - 新しい運行 (入れ替える前に無い) → 今回の乗務員 × 月
/// - 既に在る運行で、snapshot が変わった・乗務員か日付が変わった・[`OperationInput::recalc`] → 今回の乗務員 × 月。
///   乗務員か日付が変わったなら、前の乗務員 × 前の月にも (前の乗務員の日別の行が残るため)
///
/// 乗務員の id は運行の `driver_id` (乗務員ごとの再計算の口が引くのと同じ)。無い運行には付けない。
pub async fn replace_operations_in(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    upload_id: Uuid,
    rows: &[KudguriRow],
    inputs: &[OperationInput],
) -> Result<(i32, BTreeSet<(Uuid, NaiveDate)>), tokio_postgres::Error> {
    let previous = operation_recalc_keys(tx, tenant_id, rows).await?;
    let mut operations_count = 0i32;
    let mut marks = BTreeSet::new();
    for (row, input) in rows.iter().zip(inputs) {
        let changed = replace_operation(tx, tenant_id, upload_id, row, input).await?;
        operations_count += 1;
        let current = (input.driver_id, row.reading_date, row.operation_date);
        let before = previous.get(&(row.unko_no.clone(), row.crew_role)).copied();
        let moved = before.filter(|before| *before != current);
        if before.is_none() || changed || input.recalc || moved.is_some() {
            marks.extend(recalc_keys(current.0, current.1, current.2));
        }
        if let Some((driver_id, reading_date, operation_date)) = moved {
            marks.extend(recalc_keys(driver_id, reading_date, operation_date));
        }
    }
    Ok((operations_count, marks))
}

/// 日別の保存先の乗務員を引く: [`sql::SELECT_EMPLOYEE_ID_BY_CODE`] → 無ければ [`sql::SELECT_EMPLOYEE_BY_DRIVER_CD`]。
/// **読むだけ** (`driver_cd` を埋めない・作らない)。[`upsert_driver`] とは別の id を返しうるので、1 つにまとめない。
pub async fn get_employee_id_by_driver_cd(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    driver_cd: &str,
) -> Result<Option<Uuid>, tokio_postgres::Error> {
    let key: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 2] =
        [(&tenant_id, Type::UUID), (&driver_cd, Type::TEXT)];
    let by_code = tx
        .query_typed_opt(sql::SELECT_EMPLOYEE_ID_BY_CODE, &key)
        .await?;
    if let Some(row) = by_code {
        return Ok(Some(row.get(0)));
    }
    let by_driver_cd = tx
        .query_typed_opt(sql::SELECT_EMPLOYEE_BY_DRIVER_CD, &key)
        .await?;
    Ok(by_driver_cd.map(|row| row.get(0)))
}

/// 全日エントリの運行NO を、[`DayKey`] の順に重複なしで並べる (日エントリの運行NO だけを消す対象にするとき)。
pub fn daily_unko_nos(daily: &HashMap<DayKey, DailyHours>) -> Vec<String> {
    let mut entries: Vec<(&DayKey, &DailyHours)> = daily.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let mut all_unko_nos: Vec<String> = Vec::new();
    for (_, hours) in entries {
        for unko_no in &hours.unko_nos {
            if !all_unko_nos.contains(unko_no) {
                all_unko_nos.push(unko_no.clone());
            }
        }
    }
    all_unko_nos
}

/// 日別の労働時間とセグメントを保存する (transaction は開かない。呼び手の transaction の中で使う)。
/// `daily` は `alc_compare::upload_daily::compute_daily_hours` の出力そのまま (ここでは計算しない)。
/// `all_unko_nos` = 手順 2 で消す対象の運行NO (再計算は月の運行のもの。日エントリの運行NO だけなら [`daily_unko_nos`])。
///
/// 1. 日エントリの乗務員CD のうち空でないものの id を [`get_employee_id_by_driver_cd`] で引く (同じ CD は 1 回)。
///    id が引けない CD と空の CD の日エントリは、消す対象にも保存の対象にもしない。
/// 2. 引けた乗務員ごとに、`all_unko_nos` で [`sql::DELETE_SEGMENTS_BY_UNKO_NOS`] → [`sql::DELETE_DAILY_HOURS_BY_UNKO_NOS`]
///    (帰属日が変わっても古い行が残らないように)。
/// 3. 日エントリを **[`DayKey`] の順** (乗務員CD・日・開始時刻) に保存する: [`sql::DELETE_DAILY_HOURS_EXACT`] →
///    [`sql::INSERT_DAILY_WORK_HOURS`] → [`sql::DELETE_SEGMENTS_BY_DATE`] → [`sql::INSERT_SEGMENT`]。
///    `DELETE_SEGMENTS_BY_DATE` は **(乗務員, 日) ごとに最初の 1 回だけ**流す (同じ乗務員・同じ日に日エントリが 2 つ以上
///    在っても、先に入れたセグメントを消さない)。
pub async fn save_daily_hours_with(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    daily: &HashMap<DayKey, DailyHours>,
    all_unko_nos: &[String],
) -> Result<(), tokio_postgres::Error> {
    let mut entries: Vec<(&DayKey, &DailyHours)> = daily.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    let mut driver_ids: HashMap<&str, Option<Uuid>> = HashMap::new();
    let mut targets: Vec<Uuid> = Vec::new();
    for ((driver_cd, _, _), _) in &entries {
        if !driver_cd.is_empty() && !driver_ids.contains_key(driver_cd.as_str()) {
            let id = get_employee_id_by_driver_cd(tx, tenant_id, driver_cd).await?;
            driver_ids.insert(driver_cd, id);
            targets.extend(id.filter(|id| !targets.contains(id)));
        }
    }

    for driver_id in &targets {
        let stale: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 3] = [
            (&tenant_id, Type::UUID),
            (driver_id, Type::UUID),
            (&all_unko_nos, Type::TEXT_ARRAY),
        ];
        tx.execute_typed(sql::DELETE_SEGMENTS_BY_UNKO_NOS, &stale)
            .await?;
        tx.execute_typed(sql::DELETE_DAILY_HOURS_BY_UNKO_NOS, &stale)
            .await?;
    }

    let mut cleared: HashSet<(Uuid, NaiveDate)> = HashSet::new();
    for ((driver_cd, work_date, start_time), hours) in entries {
        let Some(driver_id) = driver_ids.get(driver_cd.as_str()).copied().flatten() else {
            continue;
        };

        let exact: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 4] = [
            (&tenant_id, Type::UUID),
            (&driver_id, Type::UUID),
            (work_date, Type::DATE),
            (start_time, Type::TIME),
        ];
        tx.execute_typed(sql::DELETE_DAILY_HOURS_EXACT, &exact)
            .await?;

        let total_drive_minutes = hours.saved_total_drive_minutes();
        let late_night_minutes = hours.saved_late_night_minutes();
        let day: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 18] = [
            (&tenant_id, Type::UUID),
            (&driver_id, Type::UUID),
            (work_date, Type::DATE),
            (start_time, Type::TIME),
            (&hours.total_work_minutes, Type::INT4),
            (&total_drive_minutes, Type::INT4),
            (&hours.rest_event_minutes, Type::INT4),
            (&late_night_minutes, Type::INT4),
            (&hours.drive_minutes, Type::INT4),
            (&hours.cargo_minutes, Type::INT4),
            (&hours.total_distance, Type::FLOAT8),
            (&hours.operation_count, Type::INT4),
            (&hours.unko_nos, Type::TEXT_ARRAY),
            (&hours.overlap_drive_minutes, Type::INT4),
            (&hours.overlap_cargo_minutes, Type::INT4),
            (&hours.overlap_break_minutes, Type::INT4),
            (&hours.overlap_restraint_minutes, Type::INT4),
            (&hours.ot_late_night_minutes, Type::INT4),
        ];
        tx.execute_typed(sql::INSERT_DAILY_WORK_HOURS, &day).await?;

        if cleared.insert((driver_id, *work_date)) {
            let date: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 3] = [
                (&tenant_id, Type::UUID),
                (&driver_id, Type::UUID),
                (work_date, Type::DATE),
            ];
            tx.execute_typed(sql::DELETE_SEGMENTS_BY_DATE, &date)
                .await?;
        }

        for seg in &hours.segments {
            let start_at = seg.start_at.and_utc();
            let end_at = seg.end_at.and_utc();
            let segment: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 12] = [
                (&tenant_id, Type::UUID),
                (&driver_id, Type::UUID),
                (work_date, Type::DATE),
                (&seg.unko_no, Type::TEXT),
                (&seg.segment_index, Type::INT4),
                (&start_at, Type::TIMESTAMPTZ),
                (&end_at, Type::TIMESTAMPTZ),
                (&seg.work_minutes, Type::INT4),
                (&seg.labor_minutes, Type::INT4),
                (&seg.late_night_minutes, Type::INT4),
                (&seg.drive_minutes, Type::INT4),
                (&seg.cargo_minutes, Type::INT4),
            ];
            tx.execute_typed(sql::INSERT_SEGMENT, &segment).await?;
        }
    }
    Ok(())
}

/// [`apply_upload`] の失敗。
#[derive(Debug)]
pub enum ApplyUploadError {
    /// KUDGURI の行と入力の数が合わない (DB には触れていない)
    LengthMismatch,
    Db(tokio_postgres::Error),
}

impl ApplyUploadError {
    /// ログに出せる label (識別子も生の文も含まない。DB の失敗は `alc_worker_db::kind`)。
    pub fn kind(&self) -> String {
        match self {
            Self::LengthMismatch => "length_mismatch".to_owned(),
            Self::Db(e) => alc_worker_db::kind(e),
        }
    }
}

/// 取り込みの本体 (1 transaction): [`replace_operations_in`] → [`insert_recalc_pending`] (日別の要再計算の印) →
/// [`sql::MARK_UPLOAD_COMPLETED`] (`operations_count` は流した行数)。返すのも、その行数。日別は書かない。
/// `rows` と `inputs` の数が違えば、DB を触る前に [`ApplyUploadError::LengthMismatch`] を返す。
pub async fn apply_upload(
    pg: &mut PgClient,
    tenant_id: Uuid,
    upload_id: Uuid,
    rows: Arc<Vec<KudguriRow>>,
    inputs: Vec<OperationInput>,
) -> Result<i32, ApplyUploadError> {
    if rows.len() != inputs.len() {
        return Err(ApplyUploadError::LengthMismatch);
    }
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let (operations_count, marks) =
                replace_operations_in(tx, tenant_id, upload_id, &rows, &inputs).await?;
            insert_recalc_pending(tx, tenant_id, &marks).await?;
            let completed: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 3] = [
                (&operations_count, Type::INT4),
                (&upload_id, Type::UUID),
                (&tenant_id, Type::UUID),
            ];
            tx.execute_typed(sql::MARK_UPLOAD_COMPLETED, &completed)
                .await?;
            Ok(operations_count)
        })
    })
    .await
    .map_err(ApplyUploadError::Db)
}
