//! デジタコの運行 CSV のアップロード・分割の Worker (Refs ippoan/rust-alc-api#725)。crates/alc-dtako-upload の口
//! (`tenant_router`、axum) を workers-rs (`http` / `axum` feature) に載せる。型は ippoan/alc-vein-worker と同じ。
//! DB への経路 (staging = Workers VPC の先の PgBouncer、本番 = Hyperdrive) は [`db::connect`] の 1 か所で出し分ける。
//! monolith と同じく `/api` 付きでも受ける。
//!
//! 口は `POST /split-csv/{upload_id}` (アップロード 1 件の分割) の 1 本。口と分割の流れは crates/alc-dtako-upload に在り、
//! ここが足すのは DB の接続・R2 の binding ([`r2`])・ログの出し先だけ。
//!
//! **この Worker は JWT を検証せず、auth-worker が付け直した tenant ヘッダーを信頼する**
//! (`alc_core_wasm::require_tenant_header`)。本番の到達経路は auth-worker からの Service Binding
//! だけで、`workers_dev` / `preview_urls` / `routes` を持たない (wrangler.toml と
//! scripts/check-exposure.sh が保証する。ippoan/rust-alc-api#556 と同じ穴を開けないため)。
//! staging だけはテストから叩くため `workers_dev = true` で、その workers.dev は Cloudflare Access で
//! 保護する (Access を通らないリクエストは Worker に届かない。README 参照)。

mod db;
mod r2;
mod tcp;

use std::sync::Arc;

use alc_core_wasm::require_tenant_header;
use alc_dtako_upload::routes::{tenant_router, DtakoState};
use alc_dtako_upload::split::{LogLevel, LogSink};
use axum::body::Body;
use axum::http::{HeaderValue, Response, StatusCode};
use axum::{middleware, Router};
use futures_util::lock::Mutex;
use tower_service::Service;
use worker::{
    console_error, console_warn, event, Context, Date, Env, HttpRequest, Result,
    WorkerVersionMetadata,
};

use crate::r2::{R2Store, WorkerSleeper};

/// R2 の binding (wrangler.toml の `[[r2_buckets]]`。本番と staging で別の bucket)
const DTAKO_R2_BINDING: &str = "DTAKO_R2";

/// 口は素と `/api` の nest の両方に出す (auth-worker の proxy は `/api/…` を転送する)。
/// どの route にも当たらない path の fallback にも tenant の layer が掛かる (ヘッダー無しは 401)。
fn router(state: DtakoState) -> Router {
    let dtako = tenant_router()
        .layer(middleware::from_fn(require_tenant_header))
        .with_state(state);
    Router::new().merge(dtako.clone()).nest("/api", dtako)
}

/// 口のログの出し先 (Workers のログ)。口が渡す文は、段の名前・kind・件数まで (識別子を含まない)
fn log_sink() -> LogSink {
    Arc::new(|level, message| match level {
        LogLevel::Warn => console_warn!("dtako: {message}"),
        LogLevel::Error => console_error!("dtako: {message}"),
    })
}

fn error_response(status: StatusCode, code: &str) -> Response<Body> {
    let mut resp = Response::new(Body::from(format!(r#"{{"error":"{code}"}}"#)));
    *resp.status_mut() = status;
    resp
}

#[event(fetch)]
async fn fetch(req: HttpRequest, env: Env, _ctx: Context) -> Result<Response<Body>> {
    let started = Date::now().as_millis();
    // 全リクエストで routing の前に繋ぐ (ippoan/alc-vein-worker と同じ)
    let pg = match db::connect(&env).await {
        Ok(c) => c,
        Err(db::ConnectError::NotConfigured) => {
            console_error!("dtako: {}", db::ConnectError::NotConfigured);
            return Ok(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "database_not_configured",
            ));
        }
        Err(e) => {
            console_error!("dtako: {e}");
            return Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ));
        }
    };
    let connect_ms = Date::now().as_millis() - started;
    let bucket = match env.bucket(DTAKO_R2_BINDING) {
        Ok(b) => b,
        Err(_) => {
            console_error!("dtako: no {DTAKO_R2_BINDING} binding");
            return Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ));
        }
    };
    let state = DtakoState {
        pg: Arc::new(Mutex::new(pg)),
        store: Arc::new(R2Store::new(bucket)),
        sleeper: Arc::new(WorkerSleeper),
        log: log_sink(),
    };
    let mut resp = match router(state).call(req).await {
        Ok(r) => r,
        Err(e) => match e {},
    };
    // 測定用: 接続に掛かった時間だけ
    let timing = format!("connect;dur={connect_ms}");
    if let Ok(v) = HeaderValue::from_str(&timing) {
        resp.headers_mut().insert("server-timing", v);
    }
    // どの版が応えたかを応答ヘッダーで分かるようにする (ippoan/rust-alc-api#697)。binding が取れなければ何も付けない
    if let Ok(meta) = env.get_binding::<WorkerVersionMetadata>("CF_VERSION_METADATA") {
        if let Ok(v) = HeaderValue::from_str(&meta.id()) {
            resp.headers_mut().insert("x-worker-version", v);
        }
        let tag = meta.tag();
        if !tag.is_empty() {
            if let Ok(v) = HeaderValue::from_str(&tag) {
                resp.headers_mut().insert("x-worker-tag", v);
            }
        }
    }
    Ok(resp)
}
