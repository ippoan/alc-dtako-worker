---
name: alc-dtako-worker-map
generated-from: alc-dtako-worker:969df9cdd31fb0c20a231047f51d59e4a759e8d4
paths: [src/, crates/, scripts/, .github/workflows/]
description: alc-dtako-worker (デジタコの運行 CSV のアップロード・分割の Cloudflare Worker `alc-dtako` と route の crate `alc-dtako-upload`。workers-rs + tokio-postgres、ippoan/rust-alc-api から分ける 2 本目) の構造ナビゲーション。どこに何があるか / DB への経路 / 公開範囲の検査 / デプロイとタグ / 依存 (alc-core-wasm・alc-worker-db) の固定の仕方を 1 枚にまとめる。トリガー:「alc-dtako」「alc-dtako-worker」「alc-dtako-upload」「dtako worker」「デジタコ worker」「運行 CSV のアップロード」「DTAKO_DB_VPC」「DTAKO_HYPERDRIVE」「DTAKO_R2」「check-exposure」「tenant_tx」「alc-worker-db」等。
---

# alc-dtako-worker-map — alc-dtako-worker 構造ナビゲーション

デジタコの運行 CSV のアップロード・分割を提供する Cloudflare Worker `alc-dtako` (staging は `alc-dtako-staging`、ローカルは `alc-dtako-local`) と、
その route の crate。rust-alc-api を Cloudflare Workers へ分ける 2 本目 (Refs ippoan/rust-alc-api#725) で、型は 1 本目の ippoan/alc-vein-worker と同じ。
直下の `Cargo.toml` が workspace の root (`members = ["crates/alc-dtako-upload"]`、`Cargo.lock` は直下の 1 つ)。

**いまは骨組みだけで、業務の口はまだ無い** (口を足したら、この map も更新する)。

| 場所 | 中身 |
|---|---|
| `crates/alc-dtako-upload` | route の crate (package `alc-dtako-upload`)。`src/lib.rs` の `pub fn tenant_router() -> axum::Router` が**口の無い Router** を返すだけ。口・trait・SQL は後続の PR で足す。依存は `alc-core-wasm` (`workspace = true`) と `axum` |
| `src/lib.rs` | workers-rs (`http` / `axum` feature) への載せ方。`router()` = `tenant_router().layer(middleware::from_fn(require_tenant_header))` を素と `/api` の nest の両方に出す (未定義の path は、tenant ヘッダー無しが 401・有りが 404)。`#[event(fetch)]` は**全リクエストで routing の前に `db::connect`** (`NotConfigured` → 503 `database_not_configured` / ほか → 500 `internal_error`。ログは `dtako: …`)。繋いだ `PgClient` はまだ使わない (口を足す PR で state に渡す)。応答ヘッダー `server-timing` (`connect;dur=` だけ)・`x-worker-version`・`x-worker-tag` (binding `CF_VERSION_METADATA`。tag が空なら付けない) |
| `src/db.rs` | **DB への経路の 1 か所** (ippoan/alc-vein-worker の写し。DO の段は無い)。binding `DTAKO_DB_VPC` (staging。Workers VPC の VPC Service 型、vein の staging と同じ Service → PgBouncer へ平文・trust、ロール `alc_api_app`) → Hyperdrive の binding `DTAKO_HYPERDRIVE` (本番。`alc_worker_db::hyperdrive::connect`) → 文字列 `DATABASE_URL` (ローカル専用。STARTTLS、`sslmode=disable` + `ALLOW_INSECURE_DB=1` のときだけ平文) の順に見て最初にあったもの。**Hyperdrive の binding が在るのに使えないときは 500 で、次の段へ落ちない** (落ちるのは binding が無いときだけ)。`connect` は `alc_worker_db::PgClient` を返し、生の `tokio_postgres::Client` は外へ出さない |
| `src/tcp.rs` | JS の `connect(address)` を呼ぶ extern `TcpPort` (VPC の binding 用。vein の写し) |
| `wrangler.toml` | トップレベル (本番 `alc-dtako`): `workers_dev` / `preview_urls` = false・route 無し → `[build]` (worker-build 0.8.7) → `[version_metadata]` → `[placement]` → `[[hyperdrive]]` (`DTAKO_HYPERDRIVE`。設定は実行用ロールのもの 1 つを worker 間で共有) → `[[r2_buckets]]` (`DTAKO_R2` = `ohishi-dtako`)。`[env.staging]` (`alc-dtako-staging`、`workers_dev = true` = Cloudflare Access で保護): observability → version_metadata → placement → `[[env.staging.vpc_services]]` (`DTAKO_DB_VPC`) → `[[env.staging.r2_buckets]]` (`DTAKO_R2` = `ohishi-dtako-staging`)。`[env.local]` (`alc-dtako-local`、binding 無し、deploy しない)。**表の順を変えない・前の方に表を足さない** |
| `scripts/check-exposure.sh` | `wrangler.toml` の公開範囲の検査 (CI で毎回): トップレベルの `workers_dev` / `preview_urls` = false の明示 / どこにも route・routes が無い / `workers_dev`・`preview_urls` が true でよいのは `env.staging` だけ / vars に `ALLOW_INSECURE_DB` が無い / トップレベルに `vpc_services`・`vpc_networks` が無い / **どの階層にも `durable_objects`・`containers` が無い** / `env.*` の下に `hyperdrive` が無い / **`env.*` の `r2_buckets` が本番の bucket `ohishi-dtako` を指していない** |
| `scripts/check-exposure-test.sh` | 上の陰性対照 (陽性 1 本 + 陰性 13 本。`wrangler.toml` を 1 か所ずつ崩して exit 1 を確かめる)。`[build]` の初出の直前と `[env.staging.observability]` の直前に行を挿す作りなので、**その表より前に表を足す・その文字列をコメントに書く、と検査が意味を失う** |
| `.github/workflows/ci.yml` | job `Dtako Worker (wasm)` = check-exposure → 陰性対照 → toolchain 1.92.0 (wasm32・rustfmt・clippy) → fmt → clippy (`--locked`) → worker-build → gzip 後 10MB の検査。`auto-merge` (ippoan/ci-workflows の reusable)。テスト・coverage の gate はまだ無い |
| `.github/workflows/deploy.yml` | `check` → pull_request は `dry-run` (本番と `--env staging`) / main への push は `deploy-staging` (`--tag staging-<短い SHA>`、token 無しの `/api/uploads` が Access の 302/403 で止まることを検査) / タグ `v*` は `deploy-prod` (`--tag <タグ> --message <SHA>`、workers.dev の URL が出たら fail)。secret は org の `CLOUDFLARE_API_TOKEN` |
| `.github/workflows/tag-release.yml` | 本番用のタグ `v*` を手動 (workflow_dispatch、入力 `bump`) で打つ。**マージで自動のタグは付けない** (マージ = staging、手動のタグ = 本番) |

## 依存の固定 (上げ方は CLAUDE.md)

| 依存 | 出どころ | 版を書く場所 (1 か所) |
|---|---|---|
| `alc-core-wasm` | ippoan/rust-alc-api (public) の git 依存・rev 固定 | 直下の `Cargo.toml` の `[workspace.dependencies]` (Worker と `crates/alc-dtako-upload` は `workspace = true`)。**出どころが 2 つになると `TenantId` が別の型になり、コンパイルは通るのに全リクエストが 500** — `cargo tree -i alc-core-wasm --target wasm32-unknown-unknown` で 1 つだけを確かめる |
| `alc-worker-db` (`PgClient`・`TenantTx`・`TxOutput`) | ippoan/alc-worker-kit (public) の git 依存・rev 固定 (feature `chrono`) | 直下の `Cargo.toml` の `[workspace.dependencies]`。`cargo tree -i alc-worker-db --target wasm32-unknown-unknown` で 1 つだけ。`worker`・`tokio-postgres` も版が 1 つのままであること |

private の依存は無い。
