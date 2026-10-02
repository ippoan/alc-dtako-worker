# alc-dtako-worker

デジタコの運行 CSV のアップロード・分割の Cloudflare Worker `alc-dtako` と、その route の crate `alc-dtako-upload`。
rust-alc-api を Cloudflare Workers へ分ける 2 本目 (Refs ippoan/rust-alc-api#725)。型は 1 本目の ippoan/alc-vein-worker と同じ。
**いまは骨組みだけで、業務の口はまだ無い。** 構造は `.claude/skills/alc-dtako-worker-map`、詳細は `README.md`。

- 直下 = Worker 本体 (workers-rs + tokio-postgres、wasm32-unknown-unknown。workspace の root)
- `crates/alc-dtako-upload/` = route の crate (`tenant_router()`。口は後続の PR で足す)

## コマンド

```bash
bash scripts/check-exposure.sh && bash scripts/check-exposure-test.sh   # 公開範囲の検査と陰性対照
cargo fmt --check
cargo clippy --locked --target wasm32-unknown-unknown --release -- -D warnings
worker-build --release                                                  # worker-build 0.8.7
npx wrangler@4.144.0 deploy --dry-run [--env staging]                   # 配信しない
```

## 規範

- **公開範囲と、その検査を弱めない。** この Worker は JWT を検証せず `X-Tenant-ID` を信頼する。本番の到達経路は
  auth-worker からの Service Binding だけ (`workers_dev` / `preview_urls` = false、route 無し)。
  `scripts/check-exposure.sh` と陰性対照 `check-exposure-test.sh` を緩めない・外さない。
- **`wrangler.toml` の表の順を変えない・前の方に表を足さない。** 陰性対照は `[build]` の初出の前・
  `[env.staging]` 直後の `[env.staging.observability]` という位置に行を挿す作りで、順が変わると検査が意味を失う
  (その 2 つの文字列を、その表より前のコメントにも書かない)。新しい表は `[placement]` より後に置く。
  worker 名・binding・`[version_metadata]` (トップレベルと `env.staging` の両方) も変えない。
- **Hyperdrive の binding `DTAKO_HYPERDRIVE` はトップレベル (本番) にだけ置く。** 設定は実行用ロールのもの 1 つを複数の worker で
  共有する (worker ごとに作らない)。`env.*` の下に `hyperdrive` を置かない (本番の DB へ届くため)。
  平文の DB binding (`vpc_services` 等) を本番に置かない。Durable Object と Container は持たない。どれも `check-exposure.sh` が検査する。
- **staging の R2 は staging 用の bucket** (`ohishi-dtako-staging`)。`env.*` から本番の bucket (`ohishi-dtako`) を指さない
  (本番の object を上書きする。`check-exposure.sh` が検査)。
- **secret・binding・入口 (route) を増やさない。** Cloudflare の token は org の secret を使い、repo 単位の secret を作らない。
- **public repo。** ホスト名・IP・account ID・Tunnel ID・project ref・テナント ID・メール・接続文字列・workers.dev の subdomain の実物を、
  コード・コメント・commit・PR に書かない (`wrangler.toml` に既に在る binding 用の ID は別。ほかの場所へ写さない)。
- **タグ `v*` = 本番。** main へのマージは staging に出るだけ。本番は Actions の Tag Release を手動で打つ
  (マージで自動のタグは付けない)。手で `v*` のタグを push しない。
- DB 操作は共通 crate `alc-worker-db` (ippoan/alc-worker-kit) の `PgClient::tenant_tx` だけを通す (戻り値は `TxOutput`)。
  **名前付き prepared statement を呼ぶコード (`execute`・`query`・`query_one`・`query_opt`・`prepare`) を足さない** — 使うのは
  `TenantTx` の `query_typed`・`query_typed_one`・`query_typed_opt`・`execute_typed` だけ (Hyperdrive 経由では名前付きの文で接続が切れる)。
  生の `tokio_postgres::Client` を `src/db.rs` の外へ出さない。

## rev を上げる手順

- **`alc-core-wasm`** (ippoan/rust-alc-api) と **`alc-worker-db`** (ippoan/alc-worker-kit): 直下の `Cargo.toml` の `[workspace.dependencies]` の
  `rev` を変え (**書くのはここ 1 か所だけ**。`crates/alc-dtako-upload/Cargo.toml` や `[dependencies]` に git / path を書かない)、
  `cargo update -p <名前>` で `Cargo.lock` を一緒に更新する。その後 `cargo tree -i <名前> --target wasm32-unknown-unknown` で
  出どころが 1 つだけであることを確かめる (2 つになると、コンパイルは通るのに全リクエストが 500 になる)。
  `worker`・`tokio-postgres` も版が 1 つのままであること。
