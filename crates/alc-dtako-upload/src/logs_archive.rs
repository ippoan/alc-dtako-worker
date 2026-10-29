//! 車両の現在地のログ (dtakologs) の退避と書き戻し (W9、Refs ippoan/rust-alc-api#680)。
//! rust-alc-api の archive Job (`src/archive/dtako.rs` の `dtako-archive` と `dtako-restore`) を移したもの。
//!
//! - [`run`]: 定時実行 (直下の worker の `scheduled`)。保持期間 ([`RETENTION_DAYS`] 日) より前の日の行を、
//!   (テナント, 日付) ごとに gzip の JSON Lines にして保存先に置き、目録 ([`MANIFEST_KEY`]) に載せてから DB から消す
//! - `POST /internal/archive/restore` (body `{date}`): 保存先の 1 日ぶんを DB に書き戻す。テナントは tenant ヘッダー
//!   (`require_tenant_header`) から取り、body では受けない。auth-worker の MCP ツールから Service Binding でだけ呼ばれる
//!   (auth-worker の振り分け表はこの path を拾わない)
//!
//! **保存先の形 (key・目録・見出しの行・gzip) は rust-alc-api と同じ** (過去の退避と互換。画面の
//! [`crate::screen::archived_dtakologs`] も同じものを読む)。DB は、テナントを列挙してからテナントごとの transaction
//! (RLS の tenant_isolation) で読み消しする ([`crate::pg::archive_old_dates`] ほか)。rust が使っていた migration 084 の
//! archive_* 関数と archive_mode のポリシーは使わない (どちらも alc_api_app だけのもの。migration 192)。
//!
//! rust-alc-api からの違い (消した行を退避から取りこぼさないため):
//! - 退避するのは、消す対象 (保持期間より前の日) だけ。rust は今日を含む全部の日を退避していたので、退避の後に届いた行は
//!   退避されないまま消えていた
//! - 消すのは、目録の行数と DB の行数が合う日だけ。合わない日 (目録に無い・退避の後に行が増えた) は退避し直す
//! - 目録が読めない (壊れている) ときは何もしない (rust は空の目録で上書きしていた)。目録の書き込みは 1 回の実行で 1 度
//! - 1 回の実行で退避する日は [`MAX_DATES_PER_RUN`] まで (CPU の上限)。残りは次の実行
//! - 表の形の控え (`schema_v1.json`) は書かない (rust が置いたものが在り、書き戻しは比べずに流していた)
//!
//! ログは件数と段の名前だけ (テナント ID・日付・key は出さない)。

use std::collections::BTreeMap;
use std::io::{Read, Write};

use alc_core_wasm::TenantId;
use alc_worker_db::PgClient;
use axum::extract::rejection::JsonRejection;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::pg::{self, ArchiveDate};
use crate::routes::DtakoState;
use crate::split::{LogLevel, LogSink};
use crate::store::ObjectStore;

/// 退避の目録 (テナント → 日付 → 置き場)。画面の読み取りと同じ key。
pub const MANIFEST_KEY: &str = crate::screen::ARCHIVE_MANIFEST_KEY;

/// DB に残す日数 (UTC の今日 − この日数 より前の日を退避して消す。rust-alc-api と同じ)。
pub const RETENTION_DAYS: i64 = 7;

/// 1 日ぶんを DB から読むときの 1 回の件数 (rust-alc-api と同じ)。
pub const FETCH_BATCH: i64 = 10_000;

/// 1 回の実行で退避する (テナント, 日付) の上限。残りは次の実行。
pub const MAX_DATES_PER_RUN: usize = 20;

/// 書き戻しで 1 文に載せる行数 (rust-alc-api と同じ)。
pub const RESTORE_BATCH: usize = 500;

/// 退避の目録 (rust-alc-api の `Manifest` と同じ形)。
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Manifest {
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub archived_dates: BTreeMap<String, BTreeMap<String, ArchivedDateInfo>>,
}

/// 目録の 1 日ぶん (rust-alc-api の `ArchivedDateInfo` と同じ形)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchivedDateInfo {
    pub row_count: usize,
    pub archived_at: String,
    pub r2_key: String,
}

impl Manifest {
    /// 目録に載っていて、行数が `d` と合う日か。
    fn matches(&self, d: &ArchiveDate) -> bool {
        let info = self
            .archived_dates
            .get(&d.tenant_id.to_string())
            .and_then(|dates| dates.get(&d.date));
        info.is_some_and(|i| i.row_count as i64 == d.row_count)
    }
}

/// 1 日ぶんの置き場 (rust-alc-api の `make_r2_key` と同じ)。`date` は `YYYY-MM-DD`。
pub fn r2_key(tenant_id: &str, date: &str) -> String {
    format!(
        "archive/alc_api/dtakologs/{}/{}/{}/{}.jsonl.gz",
        tenant_id,
        &date[..4],
        &date[5..7],
        &date[8..10]
    )
}

/// 退避して消す日の境目 (この日より前)。UTC の今日 − [`RETENTION_DAYS`] 日。
pub fn cutoff(now: DateTime<Utc>) -> String {
    (now - TimeDelta::days(RETENTION_DAYS))
        .date_naive()
        .to_string()
}

/// 見出しの行 + 1 行 1 つの JSON の本文 (rust-alc-api の `build_jsonl_buffer` と同じ)。
pub fn jsonl(header: &Value, rows: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    for v in std::iter::once(header).chain(rows) {
        buf.extend_from_slice(v.to_string().as_bytes());
        buf.push(b'\n');
    }
    buf
}

/// gzip にする (メモリの上なので失敗しない)。
pub fn gzip(data: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(data).expect("gzip into memory");
    enc.finish().expect("gzip into memory")
}

/// 1 回の実行の結果 (件数だけ)。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunSummary {
    /// 退避した日の数と行数
    pub archived: usize,
    pub archived_rows: usize,
    /// 消した日の数と行数
    pub deleted: usize,
    pub deleted_rows: i64,
    /// 退避か削除に失敗した日の数 (次の実行でやり直す)
    pub failed: usize,
    /// 上限で次の実行へ回した日の数
    pub deferred: usize,
}

impl RunSummary {
    pub fn line(&self) -> String {
        format!(
            "dtakologs archive: archived {} days ({} rows), deleted {} days ({} rows), failed {}, deferred {}",
            self.archived,
            self.archived_rows,
            self.deleted,
            self.deleted_rows,
            self.failed,
            self.deferred
        )
    }
}

/// 定時実行の本体。目録を読めない・DB の一覧を引けない・目録を書けないときは `None` (何も消さない)。
pub async fn run(
    pg: &mut PgClient,
    store: &dyn ObjectStore,
    log: &LogSink,
    now: DateTime<Utc>,
) -> Option<RunSummary> {
    let mut manifest = match store.get(MANIFEST_KEY).await {
        Ok(None) => Manifest::default(),
        Ok(Some(bytes)) => match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(_) => {
                log(LogLevel::Error, "dtakologs archive: manifest unreadable");
                return None;
            }
        },
        Err(e) => {
            log(LogLevel::Error, &format!("dtakologs archive: {e}"));
            return None;
        }
    };
    let old = match pg::archive_old_dates(pg, cutoff(now)).await {
        Ok(old) => old,
        Err(e) => {
            let kind = alc_worker_db::kind(&e);
            log(LogLevel::Error, &format!("dtakologs archive: db ({kind})"));
            return None;
        }
    };

    let mut summary = RunSummary::default();
    let stale: Vec<&ArchiveDate> = old.iter().filter(|d| !manifest.matches(d)).collect();
    summary.deferred = stale.len().saturating_sub(MAX_DATES_PER_RUN);
    let archived_at = now.to_rfc3339();
    for d in stale.into_iter().take(MAX_DATES_PER_RUN) {
        let tenant = d.tenant_id.to_string();
        let rows = match pg::archive_rows(pg, d.tenant_id, d.date.clone(), FETCH_BATCH).await {
            Ok(rows) => rows.0,
            Err(e) => {
                let kind = alc_worker_db::kind(&e);
                log(
                    LogLevel::Warn,
                    &format!("dtakologs archive: fetch db ({kind})"),
                );
                summary.failed += 1;
                continue;
            }
        };
        let header = json!({
            "_archive_header": true,
            "schema_version": 1,
            "table": "alc_api.dtakologs",
            "tenant_id": tenant,
            "date": d.date,
            "archived_at": archived_at,
        });
        let key = r2_key(&tenant, &d.date);
        let body = gzip(&jsonl(&header, &rows));
        if let Err(e) = store.put(&key, body, "application/gzip").await {
            log(LogLevel::Warn, &format!("dtakologs archive: {e}"));
            summary.failed += 1;
            continue;
        }
        let info = ArchivedDateInfo {
            row_count: rows.len(),
            archived_at: archived_at.clone(),
            r2_key: key,
        };
        let dates = manifest.archived_dates.entry(tenant);
        dates.or_default().insert(d.date.clone(), info);
        summary.archived += 1;
        summary.archived_rows += rows.len();
    }
    if summary.archived > 0 {
        manifest.updated_at = archived_at;
        let body = serde_json::to_vec_pretty(&manifest).expect("manifest to json");
        if let Err(e) = store.put(MANIFEST_KEY, body, "application/json").await {
            log(LogLevel::Error, &format!("dtakologs archive: manifest {e}"));
            return None;
        }
    }

    for d in old.iter().filter(|d| manifest.matches(d)) {
        match pg::archive_delete_date(pg, d.tenant_id, d.date.clone()).await {
            Ok(n) => {
                summary.deleted += 1;
                summary.deleted_rows += n as i64;
            }
            Err(e) => {
                let kind = alc_worker_db::kind(&e);
                log(
                    LogLevel::Warn,
                    &format!("dtakologs archive: delete db ({kind})"),
                );
                summary.failed += 1;
            }
        }
    }
    Some(summary)
}

pub fn router() -> Router<DtakoState> {
    Router::new().route("/internal/archive/restore", post(restore))
}

/// `POST /internal/archive/restore` の body。テナントは受けない (知らない欄は 400)。
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreRequest {
    /// `YYYY-MM-DD`
    pub date: String,
}

/// 保存先の 1 日ぶんが書き戻せない理由。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveFileError {
    /// gzip でない・文字列でない・JSON でない行が在る
    Invalid,
    /// 見出しか行の tenant_id が、書き戻すテナントと違う (無いも含む)
    TenantMismatch,
}

/// 1 日ぶんの gzip の JSON Lines を、書き戻す行にする (見出しの行は除く)。
/// 見出しと全部の行の `tenant_id` が `tenant_id` と一致しない限り、1 行も返さない
/// (tenant_id は行の JSON から入る。RLS の WITH CHECK も止めるが、ここで先に断って 1 行も書かない)。
pub fn restore_rows(gz: &[u8], tenant_id: Uuid) -> Result<Vec<Value>, ArchiveFileError> {
    let mut text = String::new();
    flate2::read::GzDecoder::new(gz)
        .read_to_string(&mut text)
        .map_err(|_| ArchiveFileError::Invalid)?;
    let want = tenant_id.to_string();
    let mut rows = Vec::new();
    for line in text.lines().filter(|l| !l.is_empty()) {
        let v: Value = serde_json::from_str(line).map_err(|_| ArchiveFileError::Invalid)?;
        if v.get("tenant_id").and_then(Value::as_str) != Some(want.as_str()) {
            return Err(ArchiveFileError::TenantMismatch);
        }
        if v.get("_archive_header").is_none() {
            rows.push(v);
        }
    }
    Ok(rows)
}

fn valid_date(date: &str) -> bool {
    date.len() == 10 && NaiveDate::parse_from_str(date, "%Y-%m-%d").is_ok()
}

type ApiError = (StatusCode, Json<Value>);

fn api_error(status: StatusCode, code: &str) -> ApiError {
    (status, Json(json!({ "error": code })))
}

async fn restore(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    body: Result<Json<RestoreRequest>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    let Ok(Json(req)) = body else {
        return Err(api_error(StatusCode::BAD_REQUEST, "invalid_body"));
    };
    if !valid_date(&req.date) {
        return Err(api_error(StatusCode::BAD_REQUEST, "invalid_date"));
    }
    let key = r2_key(&tenant_id.to_string(), &req.date);
    let gz = match state.store.get(&key).await {
        Ok(Some(gz)) => gz,
        Ok(None) => return Err(api_error(StatusCode::NOT_FOUND, "not_found")),
        Err(e) => {
            (state.log)(LogLevel::Error, &format!("archive restore failed: {e}"));
            return Err(api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ));
        }
    };
    let rows = match restore_rows(&gz, tenant_id) {
        Ok(rows) => rows,
        Err(ArchiveFileError::Invalid) => {
            return Err(api_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_archive",
            ))
        }
        Err(ArchiveFileError::TenantMismatch) => {
            return Err(api_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "tenant_mismatch",
            ))
        }
    };
    let restored = rows.len();
    let batches = rows
        .chunks(RESTORE_BATCH)
        .map(|c| Value::Array(c.to_vec()))
        .collect();
    let result = {
        let mut pg = state.pg.lock().await;
        pg::archive_restore(&mut pg, tenant_id, batches).await
    };
    if let Err(e) = result {
        let kind = alc_worker_db::kind(&e);
        (state.log)(
            LogLevel::Error,
            &format!("archive restore failed: db ({kind})"),
        );
        return Err(api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
        ));
    }
    Ok(Json(json!({ "restored": restored })))
}
