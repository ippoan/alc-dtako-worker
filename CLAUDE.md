# alc-dtako-worker

デジタコの運行 CSV のアップロード・分割の Cloudflare Worker `alc-dtako` と、その route の crate `alc-dtako-upload`。
rust-alc-api を Cloudflare Workers へ分ける 2 本目 (Refs ippoan/rust-alc-api#725)。型は 1 本目の ippoan/alc-vein-worker と同じ。
口は 10 本: `POST /upload` (zip の取り込み)・`POST /internal/rerun/{upload_id}` (やり直し)・`POST /split-csv/{upload_id}` (分割 1 件)・`POST /split-csv-all` (一括分割)・`POST /recalculate` (月の全員)・`POST /recalculate-driver` (乗務員 1 人)・`POST /recalculate-drivers` (乗務員の一括) の再計算 (この 4 つは応答が event-stream)・履歴の読み取り `GET /uploads`・`GET /internal/pending`・`GET /internal/download/{upload_id}`。構造は `.claude/skills/alc-dtako-worker-map`、詳細は `README.md`。
直下 = Worker 本体 (workspace の root)。`crates/alc-dtako-upload/` = route の crate (口 `routes`・取り込みの流れ `ingest`・再計算の流れ `recalc`・分割の流れ `split`・zip の展開 `archive`・段の所要 `timing`・SQL の定数 `repo::sql`・それを流す `pg`・保存先の層 `store`。接続も R2 の実装も持たない)。

## コマンド

```bash
bash scripts/check-exposure.sh && bash scripts/check-exposure-test.sh   # 公開範囲の検査と陰性対照
cargo fmt --check && cargo clippy --locked --target wasm32-unknown-unknown --release -- -D warnings
# テスト (DB = 組み込みの PostgreSQL、保存先 = 偽物) + coverage 100% の gate
bash scripts/fetch-migrations.sh && cargo llvm-cov --locked -p alc-dtako-upload --text > cov.txt && bash scripts/check_coverage_100.sh --use-cache cov.txt
worker-build --release                                                  # worker-build 0.8.7
npx wrangler@4.144.0 deploy --dry-run [--env staging]                   # 配信しない
```

## 規範

- **公開範囲と、その検査を弱めない。** この Worker は JWT を検証せず `X-Tenant-ID` を信頼する。本番の到達経路は auth-worker からの Service Binding だけ
  (`workers_dev` / `preview_urls` = false、route 無し)。`scripts/check-exposure.sh` と陰性対照 `check-exposure-test.sh` を緩めない・外さない。
- **`wrangler.toml` の表の順を変えない・前の方に表を足さない。** 陰性対照は `[build]` の初出の前・
  `[env.staging]` 直後の `[env.staging.observability]` という位置に行を挿す作りで、順が変わると検査が意味を失う
  (その 2 つの文字列を、その表より前のコメントにも書かない)。新しい表は `[placement]` より後に置く。worker 名・binding・`[version_metadata]` (2 か所) も変えない。
- **Hyperdrive の binding `DTAKO_HYPERDRIVE` はトップレベル (本番) にだけ置く。** 設定は実行用ロールのもの 1 つを複数の worker で
  共有する (worker ごとに作らない)。`env.*` の下に `hyperdrive` を置かない (本番の DB へ届くため)。
  平文の DB binding (`vpc_services` 等) を本番に置かない。Durable Object と Container は持たない。どれも `check-exposure.sh` が検査する。
- **staging の R2 は staging 用の bucket** (`env.*` から本番の bucket を指さない)。**secret・binding・入口 (route) を増やさない** (token は org の secret)。
- **public repo。** ホスト名・IP・account ID・Tunnel ID・project ref・テナント ID・メール・接続文字列・workers.dev の subdomain の実物を、
  コード・コメント・commit・PR に書かない (`wrangler.toml` に既に在る binding 用の ID は別。ほかの場所へ写さない)。
- **タグ `v*` = 本番。** main へのマージは staging に出るだけ。本番は Actions の Tag Release を手動で打つ。手で `v*` のタグを push しない。
- DB 操作は共通 crate `alc-worker-db` (ippoan/alc-worker-kit) の `PgClient::tenant_tx` だけを通す (戻り値は `TxOutput`)。
  SQL は `crates/alc-dtako-upload` の `repo::sql` の 1 か所、流すのは `pg.rs` の 1 か所。**名前付き prepared statement を呼ぶコード
  (`execute`・`query`・`query_one`・`query_opt`・`prepare`) を足さない** — 使うのは `TenantTx` の `query_typed` 系と `execute_typed` だけ
  (Hyperdrive 経由では名前付きの文で接続が切れる)。生の `tokio_postgres::Client` を `src/db.rs` の外へ出さない。
- **分割の出力 (R2 の key と中身のバイト列) を backend と変えない** — 分ける本体は `alc_csv_parser::split_csv_entry` を呼ぶ (写さない・整えない)。
  ログ (`split::LogSink`)・応答の本文・履歴の `error_message` に key・運行NO・upload_id・テナント ID・入力の値・エラーの生の文を出さない (固定の語だけ。一覧 2 口の**成功の本文**だけは、ヘッダーのテナント自身の履歴の列をそのまま返す)。`unsafe` を書かない (`Send` は `worker::send` の型で)。
- **DB の検査と coverage の gate を弱めない。** `tests/sql_db.rs` は migration が未取得なら失敗する作り (skip・`#[ignore]` にしない)。
  CI は本数を target ごとに固定して回す (`ci.yml` の `sql_db` 18・`store` 10・`split_flow` 20・`upload_flow` 16。足したら数も上げる)。`coverage_100.toml` の登録を外さない。
  route の crate の通常の依存に `tokio` を入れない (待ちは `store::Sleeper` 越し。R2 の読み書きは `store::ObjectStore` 越し)。
- **`pglite-oxide` は `=0.5.0` に固定**、wasmer 系 13 crate は `Cargo.lock` で alpha 版に pin (Rust 1.92.0 で通る版。toolchain は上げない)。
  lock を作り直したら pin し直す (README の「lock の pin」)。本番の wasm に pglite / wasmer を入れない。

## rev を上げる手順

- **`alc-core-wasm`・`alc-csv-parser`・`alc-compare`** (ippoan/rust-alc-api。**3 つは同じ rev**) と **`alc-worker-db`** (ippoan/alc-worker-kit): 直下の `Cargo.toml` の
  `[workspace.dependencies]` の `rev` を変え (**書くのはここだけ**)、`cargo update -p <名前>` で `Cargo.lock` も更新する。その後
  `cargo tree -i <名前> --target wasm32-unknown-unknown` で出どころが 1 つだけ (`worker`・`tokio-postgres` も版が 1 つ) を確かめ、テストを通す (分割の出力の固定の期待値 `split_output_matches_hand_written_bytes` が上げる前後とも通ること)。
- **alc-migrations** (テストが流す SQL): `scripts/ALC_MIGRATIONS_REV` の 1 行 (**rev を書くのはここだけ**。ippoan/rust-alc-api が固定している rev と揃える)。
