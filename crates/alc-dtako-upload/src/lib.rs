//! デジタコの運行 CSV のアップロード・分割の route の crate (Refs ippoan/rust-alc-api#725)。
//!
//! - [`routes`]: 口 (`POST /upload`・`POST /internal/rerun/{upload_id}`・`POST /split-csv/{upload_id}`・`POST /split-csv-all`・`POST /recalculate`・履歴の読み取りの GET 3 つ) と、その State
//! - [`ingest`]: アップロードの取り込みの流れ。zip を保存先に置き、運行と日別を DB に入れ、分割する (axum に依るのは `Bytes` の型だけ)
//! - [`split`]: 分割の流れ (axum に依らない) と、ログの差し込み口
//! - [`repo`] / [`pg`]: SQL の定数と、それを流す tokio-postgres 実装 (接続は持たない)
//! - [`recalc`]: 月の全員の再計算の流れ。保存先の分割の出力を読み、日別を計算し直して乗務員ごとに保存する (axum に依らない) と、その event
//! - [`timing`]: 段ごとの所要を測って `Server-Timing` に載せる形にする (時計は差し込む)
//! - [`store`]: 保存先 (R2) の抽象と PUT のやり直し (R2 を包む実装は直下の worker)
//!
//! tenant ヘッダーの layer (`alc_core_wasm::require_tenant_header`) は載せる側 (直下の worker) が掛ける。

mod archive;
pub mod ingest;
pub mod pg;
pub mod recalc;
pub mod repo;
pub mod routes;
pub mod split;
pub mod store;
pub mod timing;
