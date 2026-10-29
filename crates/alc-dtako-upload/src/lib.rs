//! デジタコの運行 CSV のアップロード・分割の route の crate (Refs ippoan/rust-alc-api#725)。
//!
//! - [`routes`]: 口 (`POST /upload`・`POST /internal/rerun/{upload_id}`・`POST /split-csv/{upload_id}`・`POST /split-csv-all`・`POST /recalculate`・`POST /recalculate-driver`・`POST /recalculate-drivers`・履歴の読み取りの GET 3 つ) と、その State
//! - [`ingest`]: アップロードの取り込みの流れ。zip を保存先に置き、運行と日別を DB に入れ、分割する (axum に依るのは `Bytes` の型だけ)
//! - [`split`]: 分割の流れ (axum に依らない) と、ログの差し込み口
//! - [`repo`] / [`pg`]: SQL の定数と、それを流す tokio-postgres 実装 (接続は持たない)
//! - [`recalc`]: 再計算の流れ (月の全員・乗務員ごと)。分割の出力やアップロードの zip を読み、日別を計算し直して乗務員ごとに保存する (axum に依らない) と、その event
//! - [`timing`]: 段ごとの所要を測って `Server-Timing` に載せる形にする (時計は差し込む)
//! - [`store`]: 保存先 (R2) の抽象と PUT のやり直し (R2 を包む実装は直下の worker)
//! - [`events`] (W5b): 勤怠向けの読み取り `GET /dtako/events`・`GET /dtako/events/etags` の組み立て
//! - [`screen`] (W5c-1): 画面向けの読み取り (乗務員・車両・日別・運行・CSV・変更記録・分類・スクレイプの履歴・車両の現在地のログ) と分類の更新
//! - [`ytime`] (W5c-2): Y時間 export (`GET /dtako/y-time-export`・`POST /dtako/y-time-rows`)。行の組み立ては rust-alc-api から写した純粋な部分
//! - [`restraint`] (W5c-3・W5c-4): 拘束時間の帳票 (`GET /restraint-report`・`POST /restraint-report/compare-csv`) と その PDF (`GET /restraint-report/pdf`・`GET /restraint-report/pdf-stream`)。組み立ては rust-alc-api から写した純粋な部分、描画は `alc_pdf`
//! - [`tickets`] (W5c-5): 車両のエラーの起票 (`/dtako/tickets*` の 5 口。起票・結果の反映・一覧・1 件・close_token で閉じる)
//! - [`logs_archive`] (W9): 車両の現在地のログの退避 (定時実行の [`logs_archive::run`]) と書き戻しの内部の口 `POST /internal/archive/restore`
//! - [`logs`] / [`dvr`]: 機械の取り込み (車両の現在地のログの一括 upsert・DVR の動画通知と実体) の入力の型・検査・応答の形 (W5a)
//!
//! tenant ヘッダーの layer (`alc_core_wasm::require_tenant_header`) は載せる側 (直下の worker) が掛ける。

mod archive;
pub mod dvr;
pub mod events;
pub mod ingest;
pub mod logs;
pub mod logs_archive;
pub mod pg;
pub mod recalc;
pub mod repo;
pub mod restraint;
pub mod routes;
pub mod screen;
pub mod split;
pub mod store;
pub mod tickets;
pub mod timing;
pub mod ytime;
