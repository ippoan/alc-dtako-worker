//! デジタコの運行 CSV のアップロード・分割の route の crate (Refs ippoan/rust-alc-api#725)。
//!
//! **口はまだ 1 本も無い。** 口は後続の PR で足す。いま在るのは、口が使う層だけ:
//! SQL の定数 ([`repo::sql`]) とそれを流す実装 ([`pg`])、保存先 (R2) の抽象と PUT のやり直し ([`store`])。
//! tenant ヘッダーの layer (`alc_core_wasm::require_tenant_header`) は載せる側 (直下の worker) が掛ける。

pub mod pg;
pub mod repo;
pub mod store;

use axum::Router;

/// テナントの口の Router。**口は後続の PR で足す** (いまは口の無い Router を返すだけ)。
pub fn tenant_router() -> Router {
    Router::new()
}
