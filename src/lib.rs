//! デジタコの運行 CSV のアップロード・分割の Worker (Refs ippoan/rust-alc-api#725)。crates/alc-dtako-upload の口
//! (`tenant_router`、axum) を workers-rs (`http` / `axum` feature) に載せる。型は ippoan/alc-vein-worker と同じ。
//! DB への経路 (staging = Workers VPC の先の PgBouncer、本番 = Hyperdrive) は [`db::connect`] の 1 か所で出し分ける。
//! monolith と同じく `/api` 付きでも受ける。
//!
//! **いまは骨組みだけで、業務の口は 1 本も無い** (どの path も、tenant ヘッダー無しは 401・有りは 404)。
//!
//! **この Worker は JWT を検証せず、auth-worker が付け直した tenant ヘッダーを信頼する**
//! (`alc_core_wasm::require_tenant_header`)。本番の到達経路は auth-worker からの Service Binding
//! だけで、`workers_dev` / `preview_urls` / `routes` を持たない (wrangler.toml と
//! scripts/check-exposure.sh が保証する。ippoan/rust-alc-api#556 と同じ穴を開けないため)。
//! staging だけはテストから叩くため `workers_dev = true` で、その workers.dev は Cloudflare Access で
//! 保護する (Access を通らないリクエストは Worker に届かない。README 参照)。

mod db;
mod tcp;

use alc_core_wasm::require_tenant_header;
use alc_dtako_upload::tenant_router;
use axum::body::Body;
use axum::http::{HeaderValue, Response, StatusCode};
use axum::{middleware, Router};
use tower_service::Service;
use worker::{
    console_error, event, Context, Date, Env, HttpRequest, Result, WorkerVersionMetadata,
};

/// 口は素と `/api` の nest の両方に出す (auth-worker の proxy は `/api/…` を転送する)。
/// どの route にも当たらない path の fallback にも tenant の layer が掛かる (ヘッダー無しは 401)。
fn router() -> Router {
    let dtako = tenant_router().layer(middleware::from_fn(require_tenant_header));
    Router::new().merge(dtako.clone()).nest("/api", dtako)
}

fn error_response(status: StatusCode, code: &str) -> Response<Body> {
    let mut resp = Response::new(Body::from(format!(r#"{{"error":"{code}"}}"#)));
    *resp.status_mut() = status;
    resp
}

#[event(fetch)]
async fn fetch(req: HttpRequest, env: Env, _ctx: Context) -> Result<Response<Body>> {
    let started = Date::now().as_millis();
    // 全リクエストで routing の前に繋ぐ (ippoan/alc-vein-worker と同じ)。
    // 繋いだ `PgClient` は、この PR では使わずに落とす。口を足す PR で state に渡す
    let _pg = match db::connect(&env).await {
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
    let mut resp = match router().call(req).await {
        Ok(r) => r,
        Err(e) => match e {},
    };
    // 測定用: 接続に掛かった時間だけ (DB・それ以外の内訳は、口を足す PR で足す)
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
