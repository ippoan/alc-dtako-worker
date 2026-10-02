//! デジタコの運行 CSV のアップロード・分割の route の crate (Refs ippoan/rust-alc-api#725)。
//!
//! **いまは骨組みだけで、口は 1 本も無い。** 口は後続の PR で足す。
//! tenant ヘッダーの layer (`alc_core_wasm::require_tenant_header`) は載せる側 (直下の worker) が掛ける。

use axum::Router;

/// テナントの口の Router。**口は後続の PR で足す** (いまは口の無い Router を返すだけ)。
pub fn tenant_router() -> Router {
    Router::new()
}
