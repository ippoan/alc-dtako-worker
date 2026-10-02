//! 口 (Refs ippoan/rust-alc-api#725)。
//!
//! - `POST /split-csv/{upload_id}` → アップロード 1 件を分割する ([`crate::split::split_upload`])
//!
//! 認可は `TenantId` の Extension だけを見る (直下の worker が `alc_core_wasm::require_tenant_header` の layer で入れる。
//! 役割は auth-worker の許可表が決める)。DB は、テナントを設定した接続で、テナントでも絞って引く。

use std::sync::Arc;

use alc_core_wasm::TenantId;
use alc_worker_db::PgClient;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use futures_util::lock::Mutex;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::split::{split_upload, LogLevel, LogSink, SplitError};
use crate::store::{ObjectStore, Sleeper};

/// 応答に載せる運行NO の一覧の上限 (超えたぶんは切り、総数は `*_total` に載せる)。backend と同じ値。
pub const SPLIT_UNKO_NOS_DISPLAY_LIMIT: usize = 500;

/// 口の State。直下の worker が、リクエストごとに繋いだ接続と R2 の binding から作る (テストは偽物を差す)。
#[derive(Clone)]
pub struct DtakoState {
    /// `tenant_tx` に `&mut PgClient` が要るので async の Mutex で包む
    pub pg: Arc<Mutex<PgClient>>,
    pub store: Arc<dyn ObjectStore>,
    pub sleeper: Arc<dyn Sleeper>,
    pub log: LogSink,
}

pub fn tenant_router() -> Router<DtakoState> {
    Router::new().route("/split-csv/{upload_id}", post(split_csv))
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

/// 応答は backend の `POST /api/split-csv/{upload_id}` と同じ 7 フィールド。
async fn split_csv(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(upload_id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let (store, sleeper) = (state.store.as_ref(), state.sleeper.as_ref());
    let outcome = split_upload(&state.pg, store, sleeper, &state.log, tenant_id, upload_id)
        .await
        .map_err(|e| split_error(&state.log, e))?;
    let (split_unko_nos, split_unko_nos_total) =
        alc_csv_parser::cap_sorted(outcome.succeeded_unko_nos, SPLIT_UNKO_NOS_DISPLAY_LIMIT);
    let (split_failed_unko_nos, split_failed_unko_nos_total) =
        alc_csv_parser::cap_sorted(outcome.failed_unko_nos, SPLIT_UNKO_NOS_DISPLAY_LIMIT);

    Ok(Json(json!({
        "status": "ok",
        "upload_id": upload_id,
        "split_failed": outcome.put_failed,
        "split_unko_nos": split_unko_nos,
        "split_unko_nos_total": split_unko_nos_total,
        "split_failed_unko_nos": split_failed_unko_nos,
        "split_failed_unko_nos_total": split_failed_unko_nos_total,
    })))
}
