//! デジタコの運行 CSV のアップロード・分割の route の crate (Refs ippoan/rust-alc-api#725)。
//!
//! **口はまだ 1 本も無い。** 口は後続の PR で足す。いま在るのは、口が使う DB の層だけ:
//! SQL の定数 ([`repo::sql`]) と、それを流す実装 ([`pg`])。
//! tenant ヘッダーの layer (`alc_core_wasm::require_tenant_header`) は載せる側 (直下の worker) が掛ける。

pub mod pg;
pub mod repo;

use axum::Router;

/// テナントの口の Router。**口は後続の PR で足す** (いまは口の無い Router を返すだけ)。
pub fn tenant_router() -> Router {
    Router::new()
}
