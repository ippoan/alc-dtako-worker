//! アップロードの取り込みの流れ: zip を保存先に置き、運行と日別を DB に入れ、運行NO ごとの CSV に分割する
//! (Refs ippoan/rust-alc-api#725)。backend (ippoan/rust-alc-api) の `POST /api/upload` と同じ仕事・同じ順。
//! axum の口は [`crate::routes`] (ここが axum から借りるのは `Bytes` の型だけ)。
//!
//! 段の順 ([`ingest_upload`]):
//! 1. 履歴を作る ([`pg::create_upload`])
//! 2. zip を保存先に置く (key = `{テナント}/uploads/{履歴の id}/{filename}`)
//! 3. key を履歴に記録する (単独の transaction。後の段が落ちても残る)
//! 4. zip を展開して KUDGURI と KUDGIVT を読む (KUDGURI が 0 行なら、KUDGIVT が無くても運行 0 件として進む)
//! 5. 準備 ([`pg::prepare_upload`])
//! 6. 「既に在る」運行だけ、分割済みの旧 KUDGIVT を保存先から読んで前回の分数を出す (取れなければ `None`。取り込みは続ける)。
//!    今回の分数と日別は、共有の関数 (`alc_csv_parser::operation_changes`・`alc_compare::upload_daily`) で出す
//! 7. 運行の入れ替え + 日別の保存 + 完了の印 ([`pg::apply_upload`]。1 transaction)
//! 8. 分割 ([`split_upload`])。丸ごと失敗したら待って、全体を最大 [`PUT_RETRY_ATTEMPTS`] 回。尽きても取り込みは成功のまま
//!
//! 2〜7 が失敗したら、履歴に失敗の印 ([`pg::mark_upload_failed`]) を付けてから失敗を返す。保存先の await を DB の
//! transaction の中に挟まない (接続の lock を取って `pg` の関数を呼び、返ったら離してから保存先へ)。
//!
//! **失敗の文・ログ・履歴の `error_message` は固定の語だけ** ([`IngestError::label`])。parser や DB の生の文・入力の値・
//! key・運行NO・テナント ID を出さない。

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use alc_compare::upload_daily::compute_daily_hours;
use alc_csv_parser::kudgivt::{parse_kudgivt_for_crew, KudgivtRow};
use alc_csv_parser::kudguri::KudguriRow;
use alc_csv_parser::operation_changes::{minutes_for, minutes_from_events, OperationMinutes};
use alc_worker_db::PgClient;
use axum::body::Bytes;
use futures_util::lock::Mutex;
use uuid::Uuid;

use crate::archive::{Archive, ArchiveError, MAX_UNCOMPRESSED_BYTES};
use crate::pg::{self, CreateUploadError, OperationInput, PreparedRow};
use crate::split::{split_upload, LogLevel, LogSink, SplitOutcome};
use crate::store::{ObjectStore, Sleeper, PUT_RETRY_ATTEMPTS, PUT_RETRY_DELAYS_MS};

/// 取り込みの上限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestLimits {
    /// zip の非圧縮サイズの合計の上限 (超える zip は入力の誤り)
    pub max_uncompressed_bytes: u64,
}

impl Default for IngestLimits {
    fn default() -> Self {
        Self {
            max_uncompressed_bytes: MAX_UNCOMPRESSED_BYTES,
        }
    }
}

/// [`ingest_upload`] の結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    pub upload_id: Uuid,
    /// 流した KUDGURI の行数 (運行NO の種類の数ではない)
    pub operations_count: i32,
    /// 分割の結果。分割が回を使い切って失敗したら `put_failed = 1` で、運行NO の一覧は空
    pub split: SplitOutcome,
}

/// [`ingest_upload`] の失敗。`Db`・`Storage` はこちら側の失敗、ほかは入力の誤り。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestError {
    /// テナントが存在しない (履歴は作られていない)
    TenantNotFound,
    /// zip として開けない、または展開できないエントリが在る
    InvalidZip,
    /// zip の非圧縮サイズの合計が上限を超える
    ZipTooLarge,
    /// 名前に KUDGURI を含むエントリが無い
    KudguriNotFound,
    /// KUDGURI を読めない (必須の列が無い・読取日が日付でない など)
    KudguriInvalid,
    /// KUDGURI に行が在るのに、名前に KUDGIVT を含むエントリが無い
    KudgivtNotFound,
    /// KUDGIVT を読めない
    KudgivtInvalid,
    /// DB の失敗。中身は `alc_worker_db::kind` の label (SQLSTATE か、接続の失敗の種類)
    Db(String),
    /// zip を保存先に置けない
    Storage,
}

impl IngestError {
    /// 固定の語 (応答の本文・履歴の `error_message` に使う。識別子も生の文も含まない)。
    pub fn label(&self) -> &'static str {
        match self {
            Self::TenantNotFound => "tenant_not_found",
            Self::InvalidZip => "invalid_zip",
            Self::ZipTooLarge => "zip_too_large",
            Self::KudguriNotFound => "kudguri_not_found",
            Self::KudguriInvalid => "kudguri_invalid",
            Self::KudgivtNotFound => "kudgivt_not_found",
            Self::KudgivtInvalid => "kudgivt_invalid",
            Self::Db(_) => "db",
            Self::Storage => "storage",
        }
    }

    /// 入力の誤りか (そうでなければ、こちら側の失敗)。
    pub fn is_input_error(&self) -> bool {
        !matches!(self, Self::Db(_) | Self::Storage)
    }
}

/// 段の名前と kind だけ (ログ用)。
impl fmt::Display for IngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Db(kind) => write!(f, "db ({kind})"),
            other => f.write_str(other.label()),
        }
    }
}

impl std::error::Error for IngestError {}

fn db_error(e: tokio_postgres::Error) -> IngestError {
    IngestError::Db(alc_worker_db::kind(&e))
}

fn archive_error(e: ArchiveError) -> IngestError {
    match e {
        ArchiveError::Invalid => IngestError::InvalidZip,
        ArchiveError::TooLarge => IngestError::ZipTooLarge,
    }
}

/// zip 1 つを取り込む (段は module の doc)。
#[allow(clippy::too_many_arguments)]
pub async fn ingest_upload(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    sleeper: &dyn Sleeper,
    log: &LogSink,
    limits: IngestLimits,
    tenant_id: Uuid,
    filename: String,
    zip_bytes: Bytes,
) -> Result<IngestOutcome, IngestError> {
    let created = {
        let mut client = pg.lock().await;
        pg::create_upload(&mut client, tenant_id, filename.clone()).await
    };
    let upload_id = created.map_err(|e| match e {
        CreateUploadError::TenantNotFound => IngestError::TenantNotFound,
        CreateUploadError::Db(e) => db_error(e),
    })?;

    let applied = store_and_apply(
        pg, store, log, limits, tenant_id, upload_id, &filename, zip_bytes,
    );
    let operations_count = match applied.await {
        Ok(count) => count,
        Err(e) => {
            // 印を付けること自体の失敗は握る (返すのは元の失敗)
            let mut client = pg.lock().await;
            let label = e.label().to_owned();
            let _ = pg::mark_upload_failed(&mut client, tenant_id, upload_id, label).await;
            return Err(e);
        }
    };

    let split = split_with_retry(pg, store, sleeper, log, tenant_id, upload_id).await;
    Ok(IngestOutcome {
        upload_id,
        operations_count,
        split,
    })
}

/// 段 2〜7。返すのは流した行数。
#[allow(clippy::too_many_arguments)]
async fn store_and_apply(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    log: &LogSink,
    limits: IngestLimits,
    tenant_id: Uuid,
    upload_id: Uuid,
    filename: &str,
    zip_bytes: Bytes,
) -> Result<i32, IngestError> {
    // 2〜3: filename は加工せず key に入れる (backend と同じ key)
    let key = format!("{tenant_id}/uploads/{upload_id}/{filename}");
    let put = store.put(&key, zip_bytes.to_vec(), "application/zip");
    put.await.map_err(|_| IngestError::Storage)?;
    let recorded = {
        let mut client = pg.lock().await;
        pg::set_upload_zip_key(&mut client, tenant_id, upload_id, key).await
    };
    recorded.map_err(db_error)?;

    // 4: 読み終えたら zip を手放す
    let (rows, kudgivt_rows) = read_rows(&zip_bytes, limits)?;
    drop(zip_bytes);
    let (rows, kudgivt_rows) = (Arc::new(rows), Arc::new(kudgivt_rows));

    // 5
    let prepared = {
        let mut client = pg.lock().await;
        pg::prepare_upload(&mut client, tenant_id, rows.clone(), kudgivt_rows.clone()).await
    };
    let prepared = prepared.map_err(db_error)?;

    // 6
    let before = before_minutes(store, log, tenant_id, &rows, &prepared.rows).await;
    let inputs: Vec<OperationInput> = rows
        .iter()
        .zip(&prepared.rows)
        .zip(before)
        .map(|((row, p), before_minutes)| OperationInput {
            office_id: p.office_id,
            vehicle_id: p.vehicle_id,
            driver_id: p.driver_id,
            before_minutes,
            after_minutes: minutes_for(&kudgivt_rows, &row.unko_no, row.crew_role),
        })
        .collect();
    // フェリーは空 (アップロードの時点では、まだ保存先に分割されていない。backend と同じ)
    let classifications = prepared.classification_map();
    let daily = compute_daily_hours(&rows, &kudgivt_rows, &classifications, &HashMap::new());
    drop(kudgivt_rows);

    // 7
    let applied = {
        let mut client = pg.lock().await;
        pg::apply_upload(&mut client, tenant_id, upload_id, rows, inputs, daily).await
    };
    applied.map_err(|e| IngestError::Db(e.kind()))
}

/// zip を展開して KUDGURI と KUDGIVT の行を読む。全エントリを展開できることも確かめる (展開できないエントリが在れば失敗)。
/// 手元に残すのは、名前に `KUDGURI`・`KUDGIVT` を含むエントリの中身だけ。
fn read_rows(
    zip_bytes: &[u8],
    limits: IngestLimits,
) -> Result<(Vec<KudguriRow>, Vec<KudgivtRow>), IngestError> {
    let mut archive =
        Archive::open(zip_bytes, limits.max_uncompressed_bytes).map_err(archive_error)?;
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    for index in 0..archive.len() {
        let (name, bytes) = archive.read_entry(index).map_err(archive_error)?;
        let upper = name.to_uppercase();
        if upper.contains("KUDGURI") || upper.contains("KUDGIVT") {
            files.push((name, bytes));
        }
    }
    let rows = alc_csv_parser::kudguri_rows_in(&files).ok_or(IngestError::KudguriNotFound)?;
    let rows = rows.map_err(|_| IngestError::KudguriInvalid)?;
    if rows.is_empty() {
        return Ok((rows, Vec::new()));
    }
    let kudgivt_rows =
        alc_csv_parser::kudgivt_rows_in(&files).ok_or(IngestError::KudgivtNotFound)?;
    let kudgivt_rows = kudgivt_rows.map_err(|_| IngestError::KudgivtInvalid)?;
    Ok((rows, kudgivt_rows))
}

/// 行ごとの前回の分数 (`rows` と同じ順・同じ数)。「既に在る」行だけ、分割済みの旧 KUDGIVT
/// (`{テナント}/unko/{運行NO}/KUDGIVT.csv`) を運行NO ごとに 1 回読む。読めない・無い・parse できないは `None`。
async fn before_minutes(
    store: &dyn ObjectStore,
    log: &LogSink,
    tenant_id: Uuid,
    rows: &[KudguriRow],
    prepared: &[PreparedRow],
) -> Vec<Option<OperationMinutes>> {
    let mut old: HashMap<&str, Option<Vec<u8>>> = HashMap::new();
    let mut before = Vec::with_capacity(rows.len());
    let mut unavailable = 0usize;
    for (row, p) in rows.iter().zip(prepared) {
        if !p.exists {
            before.push(None);
            continue;
        }
        if !old.contains_key(row.unko_no.as_str()) {
            let key = format!("{tenant_id}/unko/{}/KUDGIVT.csv", row.unko_no);
            let bytes = store.get(&key).await.ok().flatten();
            old.insert(&row.unko_no, bytes);
        }
        let bytes = old[row.unko_no.as_str()].as_deref();
        let events = bytes.and_then(|b| parse_kudgivt_for_crew(b, row.crew_role).ok());
        let minutes = events.map(|events| minutes_from_events(&events));
        unavailable += usize::from(minutes.is_none());
        before.push(minutes);
    }
    if unavailable > 0 {
        let message = format!("upload: previous KUDGIVT unavailable for {unavailable} row(s)");
        log(LogLevel::Warn, &message);
    }
    before
}

/// 分割を、丸ごと失敗したら待ってやり直す (回数と待ちは PUT のやり直しと同じ値 = backend と同じ)。
/// 回を使い切ったら `put_failed = 1` を返す (運行NO は分からないので空のまま)。
async fn split_with_retry(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    sleeper: &dyn Sleeper,
    log: &LogSink,
    tenant_id: Uuid,
    upload_id: Uuid,
) -> SplitOutcome {
    for attempt in 1..=PUT_RETRY_ATTEMPTS {
        let e = match split_upload(pg, store, sleeper, log, tenant_id, upload_id).await {
            Ok(outcome) => return outcome,
            Err(e) => e,
        };
        let message = format!("upload: split failed ({attempt}/{PUT_RETRY_ATTEMPTS}): {e}");
        log(LogLevel::Warn, &message);
        if let Some(ms) = PUT_RETRY_DELAYS_MS.get(attempt - 1) {
            sleeper.sleep_ms(*ms).await;
        }
    }
    SplitOutcome {
        put_failed: 1,
        succeeded_unko_nos: Vec::new(),
        failed_unko_nos: Vec::new(),
    }
}
