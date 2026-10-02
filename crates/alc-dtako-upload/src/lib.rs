//! デジタコの運行 CSV のアップロード・分割の route の crate (Refs ippoan/rust-alc-api#725)。
//!
//! - [`routes`]: 口 (`POST /split-csv/{upload_id}`) と、その State
//! - [`split`]: 分割の流れ (axum に依らない) と、ログの差し込み口
//! - [`repo`] / [`pg`]: SQL の定数と、それを流す tokio-postgres 実装 (接続は持たない)
//! - [`store`]: 保存先 (R2) の抽象と PUT のやり直し (R2 を包む実装は直下の worker)
//!
//! tenant ヘッダーの layer (`alc_core_wasm::require_tenant_header`) は載せる側 (直下の worker) が掛ける。

pub mod pg;
pub mod repo;
pub mod routes;
pub mod split;
pub mod store;
