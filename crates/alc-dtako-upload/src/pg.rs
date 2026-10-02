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
//! アップロードの取り込みの段は、[`create_upload`] → [`set_upload_zip_key`] → [`prepare_upload`] → [`replace_operations`]
//! (失敗したら [`mark_upload_failed`])。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alc_csv_parser::kudgivt::KudgivtRow;
use alc_csv_parser::kudguri::KudguriRow;
use alc_csv_parser::operation_changes::{
    compose_snapshot, record_driver_cd, snapshot_changed, OperationMinutes,
};
use alc_csv_parser::work_segments::{default_classification, EventClass};
use alc_worker_db::{PgClient, TenantTx, TxOutput};
use chrono::NaiveDateTime;
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

/// KUDGURI の行の順に [`replace_operation`] を流す (transaction は開かない。呼び手の transaction の中で使う)。
/// `inputs` は `rows` と同じ順・同じ数。返すのは流した行数 (運行NO の種類の数ではなく、行の数)。
pub async fn replace_operations_in(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    upload_id: Uuid,
    rows: &[KudguriRow],
    inputs: &[OperationInput],
) -> Result<i32, tokio_postgres::Error> {
    let mut operations_count = 0i32;
    for (row, input) in rows.iter().zip(inputs) {
        replace_operation(tx, tenant_id, upload_id, row, input).await?;
        operations_count += 1;
    }
    Ok(operations_count)
}

/// 運行の入れ替え (1 transaction): [`replace_operations_in`] を流し、流した行数を返す。
pub async fn replace_operations(
    pg: &mut PgClient,
    tenant_id: Uuid,
    upload_id: Uuid,
    rows: Arc<Vec<KudguriRow>>,
    inputs: Vec<OperationInput>,
) -> Result<i32, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(
            async move { replace_operations_in(tx, tenant_id, upload_id, &rows, &inputs).await },
        )
    })
    .await
}
