//! 口 (Refs ippoan/rust-alc-api#725)。
//!
//! - `GET /uploads` → テナントの履歴の一覧 (新しい順に 50 件)
//! - `GET /internal/pending` → やり直し待ち・失敗の履歴の一覧 (新しい順に 50 件)
//! - `GET /internal/download/{upload_id}` → 履歴の zip をそのまま返す
//! - `POST /upload` → デジタコの zip (multipart の `file` field) を取り込む ([`crate::ingest::ingest_upload`])
//! - `POST /internal/rerun/{upload_id}` → 既に保存先に在る zip を、もう一度取り込む ([`crate::ingest::rerun_upload`])
//! - `POST /recalculate?year=&month=` → 月の全員の日別を計算し直す ([`crate::recalc`])。応答は `text/event-stream`
//! - `POST /recalculate-driver?year=&month=&driver_id=` → 乗務員 1 人の月の日別を計算し直す。応答は `text/event-stream`
//! - `POST /recalculate-drivers` (JSON `{year, month, driver_ids}`) → 乗務員の一括。応答は `text/event-stream`
//! - `POST /split-csv/{upload_id}` → アップロード 1 件を分割する ([`crate::split::split_upload`])
//! - `POST /split-csv-all` → 未分割の運行が在るテナントの、分割の元にできるアップロードを新しい順に
//!   最大 [`SPLIT_CSV_ALL_LIMIT`] 件、1 件ずつ分割し直す。応答は `text/event-stream` (1 件ごとに `progress`、終わりに `done`)
//!
//! 認可は `TenantId` の Extension だけを見る (直下の worker が `alc_core_wasm::require_tenant_header` の layer で入れる。
//! 役割は auth-worker の許可表が決める)。DB は、テナントを設定した接続で、テナントでも絞って引く。

use std::convert::Infallible;
use std::sync::Arc;

use alc_core_wasm::TenantId;
use alc_worker_db::PgClient;
use axum::body::{Body, Bytes};
use axum::extract::multipart::MultipartRejection;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Multipart, Path, Query, State};
use axum::http::header::{HeaderName, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::SecondsFormat;
use futures_util::lock::Mutex;
use futures_util::stream;
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::ingest::{ingest_upload, rerun_upload, IngestError, IngestLimits, IngestOutcome};
use crate::pg::{self, PendingUploadRow, UploadRow};
use crate::recalc::{next_recalc_event, RecalcRun};
use crate::split::{split_upload, LogLevel, LogSink, SplitError};
use crate::store::{ObjectStore, Sleeper};
use crate::timing::{Clock, StageTimer};

/// 応答に載せる運行NO の一覧の上限 (超えたぶんは切り、総数は `*_total` に載せる)。backend と同じ値。
pub const SPLIT_UNKO_NOS_DISPLAY_LIMIT: usize = 500;

/// 一括分割が 1 回のリクエストで処理するアップロードの数の上限 (新しい方から)。backend と同じ値。
pub const SPLIT_CSV_ALL_LIMIT: usize = 50;

/// 口の State。直下の worker が、リクエストごとに繋いだ接続と R2 の binding から作る (テストは偽物を差す)。
#[derive(Clone)]
pub struct DtakoState {
    /// `tenant_tx` に `&mut PgClient` が要るので async の Mutex で包む
    pub pg: Arc<Mutex<PgClient>>,
    pub store: Arc<dyn ObjectStore>,
    pub sleeper: Arc<dyn Sleeper>,
    /// 段ごとの所要を測る時計 (取り込みの口が `Server-Timing` に載せる)
    pub clock: Arc<dyn Clock>,
    pub log: LogSink,
}

/// `POST /upload` が受ける body の上限 (axum の既定は 2MB)。
pub const UPLOAD_BODY_LIMIT: usize = 20 * 1024 * 1024;

pub fn tenant_router() -> Router<DtakoState> {
    tenant_router_with(IngestLimits::default())
}

/// 取り込みの上限を指定して作る ([`tenant_router`] は既定の値)。
pub fn tenant_router_with(limits: IngestLimits) -> Router<DtakoState> {
    let upload = post(
        move |state: State<DtakoState>,
              tenant: Extension<TenantId>,
              multipart: Result<Multipart, MultipartRejection>| {
            upload(limits, state, tenant, multipart)
        },
    );
    let rerun = post(
        move |state: State<DtakoState>, tenant: Extension<TenantId>, upload_id: Path<Uuid>| {
            rerun(limits, state, tenant, upload_id)
        },
    );
    Router::new()
        .route("/uploads", get(list_uploads))
        .route("/internal/pending", get(list_pending_uploads))
        .route("/internal/download/{upload_id}", get(download))
        .route(
            "/upload",
            upload.layer(DefaultBodyLimit::max(UPLOAD_BODY_LIMIT)),
        )
        .route("/internal/rerun/{upload_id}", rerun)
        .route("/split-csv/{upload_id}", post(split_csv))
        .route("/split-csv-all", post(split_csv_all))
        .route("/recalculate", post(recalculate))
        .route("/recalculate-driver", post(recalculate_driver))
        .route("/recalculate-drivers", post(recalculate_drivers))
}

type ApiError = (StatusCode, Json<Value>);

/// 応答の本文は固定の語だけ (原因は載せない。500 の原因は `log` に、段の名前と kind だけを出す)。
fn split_error(log: &LogSink, e: SplitError) -> ApiError {
    if e == SplitError::NotFound {
        return (StatusCode::NOT_FOUND, Json(json!({ "error": "not_found" })));
    }
    log(LogLevel::Error, &format!("split-csv failed: {e}"));
    let body = json!({ "error": "internal_error" });
    (StatusCode::INTERNAL_SERVER_ERROR, Json(body))
}

fn not_found() -> ApiError {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "not_found" })))
}

/// こちら側の失敗 (500)。本文は固定の語で、原因は `log` に `what` と段の名前 (と kind) だけを出す。
fn internal_error(log: &LogSink, what: &str, stage: &str) -> ApiError {
    log(LogLevel::Error, &format!("{what} failed: {stage}"));
    let body = json!({ "error": "internal_error" });
    (StatusCode::INTERNAL_SERVER_ERROR, Json(body))
}

fn db_failure(log: &LogSink, what: &str, e: &tokio_postgres::Error) -> ApiError {
    internal_error(log, what, &format!("db ({})", alc_worker_db::kind(e)))
}

/// 履歴の一覧の 1 件。**field の順 = 本文のキーの順** (rust-alc-api の同じ口に合わせている)。
/// `created_at` は UTC の RFC 3339 (末尾 `Z`。小数は在るぶんだけ 3 桁ずつ)。
#[derive(Serialize)]
struct UploadJson {
    created_at: String,
    error: Option<String>,
    filename: String,
    id: Uuid,
    r2_zip_key: Option<String>,
    status: String,
}

impl From<UploadRow> for UploadJson {
    fn from(row: UploadRow) -> Self {
        Self {
            created_at: row.created_at.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            error: row.error_message,
            filename: row.filename,
            id: row.id,
            r2_zip_key: row.r2_zip_key,
            status: row.status,
        }
    }
}

/// やり直し待ち・失敗の履歴の一覧の 1 件。**field の順 = 本文のキーの順** (rust-alc-api の同じ口に合わせている)。
/// `created_at` は RFC 3339 (末尾 `+00:00`)。
#[derive(Serialize)]
struct PendingUploadJson {
    created_at: String,
    error_message: Option<String>,
    filename: String,
    id: Uuid,
    status: String,
    tenant_id: Uuid,
}

impl From<PendingUploadRow> for PendingUploadJson {
    fn from(row: PendingUploadRow) -> Self {
        Self {
            created_at: row.created_at.to_rfc3339(),
            error_message: row.error_message,
            filename: row.filename,
            id: row.id,
            status: row.status,
            tenant_id: row.tenant_id,
        }
    }
}

/// ヘッダーのテナントの履歴の一覧 (テナントを設定した接続で流し、`WHERE tenant_id` でも絞る)。
async fn list_uploads(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<UploadJson>>, ApiError> {
    let rows = {
        let mut client = state.pg.lock().await;
        pg::list_uploads(&mut client, tenant_id).await
    };
    let rows = rows.map_err(|e| db_failure(&state.log, "uploads", &e))?;
    Ok(Json(rows.into_iter().map(UploadJson::from).collect()))
}

/// ヘッダーのテナントの、やり直し待ち・失敗の履歴の一覧。
async fn list_pending_uploads(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<PendingUploadJson>>, ApiError> {
    let rows = {
        let mut client = state.pg.lock().await;
        pg::list_pending_uploads(&mut client, tenant_id).await
    };
    let rows = rows.map_err(|e| db_failure(&state.log, "pending", &e))?;
    Ok(Json(
        rows.into_iter().map(PendingUploadJson::from).collect(),
    ))
}

/// ダウンロードの filename: 履歴の filename から ASCII の英数字と `.`・`-`・`_` だけを残す。空になったら `download.zip`。
pub fn safe_download_filename(filename: &str) -> String {
    let keep = |c: &char| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_');
    let safe: String = filename.chars().filter(keep).collect();
    if safe.is_empty() {
        return "download.zip".to_owned();
    }
    safe
}

/// 履歴の zip をそのまま返す。行が無い・zip の key が入っていないは 404、保存先に無い・読めないは 500。
async fn download(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(upload_id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let row = {
        let mut client = state.pg.lock().await;
        pg::upload_download(&mut client, tenant_id, upload_id).await
    };
    let row = row.map_err(|e| db_failure(&state.log, "download", &e))?;
    let Some((Some(key), filename)) = row else {
        return Err(not_found());
    };
    let bytes = state.store.get(&key).await.ok().flatten();
    let bytes = bytes.ok_or_else(|| internal_error(&state.log, "download", "storage"))?;
    let disposition = format!(
        "attachment; filename=\"{}\"",
        safe_download_filename(&filename)
    );
    let headers = [
        (CONTENT_TYPE, "application/zip".to_owned()),
        (CONTENT_DISPOSITION, disposition),
    ];
    Ok((headers, bytes).into_response())
}

fn bad_request(label: &'static str) -> ApiError {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": label })))
}

/// 対象が無いは 404、入力の誤りは 400 と固定の語。こちら側の失敗は 500 (原因は `log` に、`what` と段の名前と kind だけを出す)。
fn ingest_error(log: &LogSink, what: &str, e: IngestError) -> ApiError {
    if e == IngestError::NotFound {
        return (StatusCode::NOT_FOUND, Json(json!({ "error": e.label() })));
    }
    if !e.is_internal() {
        return bad_request(e.label());
    }
    log(LogLevel::Error, &format!("{what} failed: {e}"));
    let body = json!({ "error": "internal_error" });
    (StatusCode::INTERNAL_SERVER_ERROR, Json(body))
}

/// 取り込みの応答 (backend の `POST /api/upload` と同じ 8 フィールド。アップロードとやり直しで同じ形)。
///
/// **field の順 = 本文のキーの順** (backend と同じ順)。呼び手に、本文の先頭の決まった長さだけを取っておき、そこから
/// `upload_id` を読むものが在るので、`upload_id` を先頭に、長くなりうる運行NO の一覧を後ろに置く (順を変えない)。
#[derive(Serialize)]
struct UploadResponse {
    upload_id: Uuid,
    operations_count: i32,
    status: &'static str,
    split_failed: usize,
    split_unko_nos: Vec<String>,
    split_unko_nos_total: usize,
    split_failed_unko_nos: Vec<String>,
    split_failed_unko_nos_total: usize,
}

fn ingest_response(outcome: IngestOutcome) -> Json<UploadResponse> {
    let (split_unko_nos, split_unko_nos_total) = alc_csv_parser::cap_sorted(
        outcome.split.succeeded_unko_nos,
        SPLIT_UNKO_NOS_DISPLAY_LIMIT,
    );
    let (split_failed_unko_nos, split_failed_unko_nos_total) =
        alc_csv_parser::cap_sorted(outcome.split.failed_unko_nos, SPLIT_UNKO_NOS_DISPLAY_LIMIT);

    Json(UploadResponse {
        upload_id: outcome.upload_id,
        operations_count: outcome.operations_count,
        status: "completed",
        split_failed: outcome.split.put_failed,
        split_unko_nos,
        split_unko_nos_total,
        split_failed_unko_nos,
        split_failed_unko_nos_total,
    })
}

/// multipart から `file` field を読む → (filename, 中身)。filename が無ければ `upload.zip` (backend と同じ)。
/// 失敗は固定の語 (multipart として読めない・body が上限を超える = `invalid_multipart` / `file` が無い = `no_file`)。
async fn read_file(mut multipart: Multipart) -> Result<(String, Bytes), &'static str> {
    loop {
        let field = multipart.next_field().await;
        let Some(field) = field.map_err(|_| "invalid_multipart")? else {
            return Err("no_file");
        };
        if field.name() == Some("file") {
            let filename = field.file_name().unwrap_or("upload.zip").to_string();
            let bytes = field.bytes().await.map_err(|_| "invalid_multipart")?;
            return Ok((filename, bytes));
        }
    }
}

/// 取り込みの結果を応答にし、終えた段の所要を `Server-Timing` に載せる (成功でも失敗でも。載せるのは段の名前と数字だけ)。
fn timed_response(
    log: &LogSink,
    what: &str,
    timer: &StageTimer<'_>,
    outcome: Result<IngestOutcome, IngestError>,
) -> Response {
    let mut response = match outcome {
        Ok(outcome) => ingest_response(outcome).into_response(),
        Err(e) => ingest_error(log, what, e).into_response(),
    };
    let timing = timer.server_timing();
    if let Some(value) = timing.and_then(|v| HeaderValue::from_str(&v).ok()) {
        let name = HeaderName::from_static("server-timing");
        response.headers_mut().insert(name, value);
    }
    response
}

/// 応答は [`ingest_response`]。
async fn upload(
    limits: IngestLimits,
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    multipart: Result<Multipart, MultipartRejection>,
) -> Result<Response, ApiError> {
    let multipart = multipart.map_err(|_| bad_request("invalid_multipart"))?;
    let (filename, zip_bytes) = read_file(multipart).await.map_err(bad_request)?;
    let (store, sleeper) = (state.store.as_ref(), state.sleeper.as_ref());
    let mut timer = StageTimer::new(state.clock.as_ref());
    let ingest = ingest_upload(
        &state.pg, store, sleeper, &state.log, limits, &mut timer, tenant_id, filename, zip_bytes,
    );
    let outcome = ingest.await;
    Ok(timed_response(&state.log, "upload", &timer, outcome))
}

/// 応答は [`ingest_response`] (`upload_id` は path の id)。やり直せるのは、ヘッダーのテナントの履歴だけ (ほかは 404)。
async fn rerun(
    limits: IngestLimits,
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(upload_id): Path<Uuid>,
) -> Response {
    let (store, sleeper) = (state.store.as_ref(), state.sleeper.as_ref());
    let mut timer = StageTimer::new(state.clock.as_ref());
    let rerun = rerun_upload(
        &state.pg, store, sleeper, &state.log, limits, &mut timer, tenant_id, upload_id,
    );
    let outcome = rerun.await;
    timed_response(&state.log, "rerun", &timer, outcome)
}

/// 分割 1 件の応答 (backend の `POST /api/split-csv/{upload_id}` と同じ 7 フィールド)。**field の順 = 本文のキーの順**。
#[derive(Serialize)]
struct SplitResponse {
    status: &'static str,
    upload_id: Uuid,
    split_failed: usize,
    split_unko_nos: Vec<String>,
    split_unko_nos_total: usize,
    split_failed_unko_nos: Vec<String>,
    split_failed_unko_nos_total: usize,
}

/// 応答は [`SplitResponse`]。
async fn split_csv(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(upload_id): Path<Uuid>,
) -> Result<Json<SplitResponse>, ApiError> {
    let (store, sleeper) = (state.store.as_ref(), state.sleeper.as_ref());
    let outcome = split_upload(&state.pg, store, sleeper, &state.log, tenant_id, upload_id)
        .await
        .map_err(|e| split_error(&state.log, e))?;
    let (split_unko_nos, split_unko_nos_total) =
        alc_csv_parser::cap_sorted(outcome.succeeded_unko_nos, SPLIT_UNKO_NOS_DISPLAY_LIMIT);
    let (split_failed_unko_nos, split_failed_unko_nos_total) =
        alc_csv_parser::cap_sorted(outcome.failed_unko_nos, SPLIT_UNKO_NOS_DISPLAY_LIMIT);

    Ok(Json(SplitResponse {
        status: "ok",
        upload_id,
        split_failed: outcome.put_failed,
        split_unko_nos,
        split_unko_nos_total,
        split_failed_unko_nos,
        split_failed_unko_nos_total,
    }))
}

/// 一括分割の進み具合 (stream の状態)。
struct SplitAllRun {
    state: DtakoState,
    tenant_id: Uuid,
    step: SplitAllStep,
    /// 候補の総数 (上限で切る前)
    candidates: usize,
    /// 今回処理する数 = min(candidates, [`SPLIT_CSV_ALL_LIMIT`])
    total: usize,
    /// これから処理するアップロード (id, filename)。新しい順
    queue: std::vec::IntoIter<(Uuid, String)>,
    success: usize,
    failed: usize,
}

enum SplitAllStep {
    /// 候補をまだ引いていない
    List,
    /// 1 件ずつ処理している
    Split,
    /// `done` か `error` を出し終えた
    Finished,
}

/// event 1 個ぶんの本文 (`data: <JSON>` と空行)。
fn sse_frame(event: Value) -> String {
    format!("data: {event}\n\n")
}

/// stream の次の event を作る。処理を 1 件終えるごとに 1 個返す (まとめて出さない)。
async fn next_split_all_event(mut run: SplitAllRun) -> Option<(String, SplitAllRun)> {
    if matches!(run.step, SplitAllStep::Finished) {
        return None;
    }
    if matches!(run.step, SplitAllStep::List) {
        let listed = {
            let mut client = run.state.pg.lock().await;
            pg::uploads_needing_split(&mut client, run.tenant_id).await
        };
        let uploads = match listed {
            Ok(uploads) => uploads,
            Err(e) => {
                // message は固定の語 (原因は log に、段の名前と kind だけ)
                let message = format!("split-csv-all failed: db ({})", alc_worker_db::kind(&e));
                (run.state.log)(LogLevel::Error, &message);
                run.step = SplitAllStep::Finished;
                let event = json!({ "event": "error", "message": "internal_error" });
                return Some((sse_frame(event), run));
            }
        };
        run.candidates = uploads.len();
        run.total = run.candidates.min(SPLIT_CSV_ALL_LIMIT);
        let limited: Vec<(Uuid, String)> = uploads.into_iter().take(SPLIT_CSV_ALL_LIMIT).collect();
        run.queue = limited.into_iter();
        run.step = SplitAllStep::Split;
    }
    let Some((upload_id, filename)) = run.queue.next() else {
        run.step = SplitAllStep::Finished;
        // backend の `split_csv_all_handler` と同じ式
        let processed = run.success + run.failed;
        let skipped = run.candidates.saturating_sub(processed);
        let event = json!({
            "event": "done", "candidates": run.candidates, "total": processed,
            "success": run.success, "failed": run.failed, "skipped": skipped
        });
        return Some((sse_frame(event), run));
    };
    let st = &run.state;
    let (store, sleeper) = (st.store.as_ref(), st.sleeper.as_ref());
    let split = split_upload(&st.pg, store, sleeper, &st.log, run.tenant_id, upload_id).await;
    match split {
        // 置けなかった CSV が在っても (`put_failed > 0`) 成功に数える (backend と同じ)
        Ok(_) => run.success += 1,
        Err(e) => {
            let message = format!("split-csv-all: split failed: {e}");
            (run.state.log)(LogLevel::Warn, &message);
            run.failed += 1;
        }
    }
    let current = run.success + run.failed;
    let event = json!({
        "event": "progress", "current": current, "total": run.total, "filename": filename
    });
    Some((sse_frame(event), run))
}

/// `POST /recalculate` の query。
#[derive(serde::Deserialize)]
struct RecalcQuery {
    year: i32,
    month: u32,
}

/// 応答は `text/event-stream` (一括分割の口と同じ形)。処理は応答の stream の中で 1 歩ずつ進む ([`next_recalc_event`])。
/// HTTP は 200 で、失敗は stream の中の `error`。
async fn recalculate(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(query): Query<RecalcQuery>,
) -> Response {
    recalc_response(recalc_run(&state, tenant_id, query.year, query.month))
}

fn recalc_run(state: &DtakoState, tenant_id: Uuid, year: i32, month: u32) -> RecalcRun {
    let (pg, store, log) = (state.pg.clone(), state.store.clone(), state.log.clone());
    RecalcRun::new(pg, store, log, tenant_id, year, month)
}

/// 再計算の event を応答の stream の中で 1 歩ずつ進める。
fn recalc_response(run: RecalcRun) -> Response {
    let events = stream::unfold(run, |run| async {
        let (event, run) = next_recalc_event(run).await?;
        Some((sse_frame(event), run))
    });
    event_stream_response(events)
}

/// `POST /recalculate-driver` の query。
#[derive(serde::Deserialize)]
struct RecalcDriverQuery {
    year: i32,
    month: u32,
    driver_id: Uuid,
}

/// 乗務員 1 人の再計算。query が読めなければ、その status で `{"error":"invalid_query"}` (入力の値を返さない)。
async fn recalculate_driver(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    query: Result<Query<RecalcDriverQuery>, QueryRejection>,
) -> Response {
    let query = match query {
        Ok(Query(query)) => query,
        Err(e) => return (e.status(), Json(json!({ "error": "invalid_query" }))).into_response(),
    };
    let run = recalc_run(&state, tenant_id, query.year, query.month);
    recalc_response(run.driver(query.driver_id))
}

/// `POST /recalculate-drivers` の body。
#[derive(serde::Deserialize)]
struct RecalcDriversBody {
    year: i32,
    month: u32,
    driver_ids: Vec<Uuid>,
}

/// 乗務員の一括の再計算。body が読めなければ、axum の JSON の拒否と同じ status で `{"error":"invalid_body"}`
/// (入力の値を返さない)。
async fn recalculate_drivers(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    body: Result<Json<RecalcDriversBody>, JsonRejection>,
) -> Response {
    let body = match body {
        Ok(Json(body)) => body,
        Err(e) => return (e.status(), Json(json!({ "error": "invalid_body" }))).into_response(),
    };
    let run = recalc_run(&state, tenant_id, body.year, body.month);
    recalc_response(run.drivers(body.driver_ids))
}

/// `text/event-stream` の応答 (`Cache-Control: no-cache`・`X-Accel-Buffering: no`)。
fn event_stream_response(
    events: impl futures_util::Stream<Item = String> + Send + 'static,
) -> Response {
    let body = Body::from_stream(stream::StreamExt::map(events, Ok::<_, Infallible>));
    let headers = [
        (CONTENT_TYPE, "text/event-stream"),
        (CACHE_CONTROL, "no-cache"),
        (HeaderName::from_static("x-accel-buffering"), "no"),
    ];
    (headers, body).into_response()
}

/// 応答は `text/event-stream`。1 件処理するごとに `progress`、終わりに `done` (候補の取得に失敗したら `error`) を出す。
async fn split_csv_all(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Response {
    let run = SplitAllRun {
        state,
        tenant_id,
        step: SplitAllStep::List,
        candidates: 0,
        total: 0,
        queue: Vec::new().into_iter(),
        success: 0,
        failed: 0,
    };
    let events = stream::unfold(run, next_split_all_event);
    event_stream_response(events)
}
