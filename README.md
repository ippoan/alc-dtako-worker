# alc-dtako-worker

デジタコの運行 CSV のアップロード・分割を提供する Cloudflare Worker `alc-dtako` (workers-rs + tokio-postgres)。
rust-alc-api を Cloudflare Workers へ段階移行する 2 本目 (Refs ippoan/rust-alc-api#725) で、型は 1 本目の
ippoan/alc-vein-worker に合わせている。

**いまは骨組みだけで、業務の口はまだ無い。** どの path も、tenant ヘッダー無しは 401・有りは 404 を返す
(その前に DB へ繋ぐので、繋げなければ 503 / 500)。口は後続の PR で `crates/alc-dtako-upload` に足す。

## 配置

| 場所 | 中身 |
|---|---|
| 直下 (`Cargo.toml`・`wrangler.toml`・`src/`) | Worker 本体 (package `alc-dtako-worker`、wasm32-unknown-unknown)。workspace の root で、`Cargo.lock` はここの 1 つだけ。`src/lib.rs` (workers-rs への載せ方)・`src/db.rs` (DB への経路)・`src/tcp.rs` (VPC の binding の extern) |
| `crates/alc-dtako-upload/` | route の crate (package `alc-dtako-upload`)。`tenant_router()` が口の無い Router を返すだけ |
| `scripts/` | 公開範囲の検査 (`check-exposure.sh` と陰性対照 `check-exposure-test.sh`) |
| `.github/workflows/` | `ci.yml` (検査) / `deploy.yml` (デプロイ) / `tag-release.yml` (本番用のタグ) |

## 依存の取り方

- **`alc-core-wasm`** は ippoan/rust-alc-api (public) に残る。直下の `Cargo.toml` の `[workspace.dependencies]` に
  **git 依存・rev 固定で 1 か所だけ**書き、Worker と `crates/alc-dtako-upload` は `workspace = true` で継承する。
  出どころが 2 つになると `TenantId` が別の型になり、**コンパイルは通るのに全リクエストが 500** になる
  (layer が入れる型と route が取り出す型が合わない)。確かめ方:

  ```bash
  cargo tree -i alc-core-wasm --target wasm32-unknown-unknown   # 出どころが 1 つだけ
  ```
- **`alc-worker-db`** (テナントの transaction の部品 `PgClient`・`TenantTx`・`TxOutput`) は ippoan/alc-worker-kit (public) に在る。
  同じく直下の `[workspace.dependencies]` に **git 依存・rev 固定で 1 か所だけ**書く (feature `chrono`。
  出どころが 2 つになると `PgClient` が別の型になる)。
- private の依存は無い (CI に private repo の取得用の step は要らない)。
- rev を上げる手順は `CLAUDE.md`。

## デプロイ

| きっかけ | 行き先 | workflow |
|---|---|---|
| pull_request | `wrangler deploy --dry-run` (本番と `--env staging`) だけ | `deploy.yml` |
| main への push (= PR のマージ) | staging (`alc-dtako-staging`、`--tag staging-<短い SHA>`) | `deploy.yml` |
| タグ `v*` の push | **本番** (`alc-dtako`、`--tag <タグ> --message <SHA>`) | `deploy.yml` |

本番のタグは Actions の **Tag Release** (`tag-release.yml`、workflow_dispatch) を手動で打つ。マージで自動のタグは付かない。
手で `v*` のタグを push しない。応答ヘッダー `x-worker-version` / `x-worker-tag` で、どの版が応えたか分かる
(`server-timing` は `connect;dur=` = DB への接続に掛かった時間だけ)。Cloudflare の token は org の secret を使う
(repo 単位の secret を作らない)。

## DB への経路 (`src/db.rs` の 1 か所で出し分ける)

上から順に見て、最初にあったものを使う。全リクエストで routing の前に繋ぐ。

| 順 | env | 読むもの | 経路 |
|---|---|---|---|
| 1 | staging (`--env staging`) | Workers VPC の binding `DTAKO_DB_VPC` (VPC Service 型、TCP。vein の staging と同じ Service) | Worker → 既存の Cloudflare Tunnel → staging の DB の PgBouncer (transaction mode) |
| 2 | 本番 (トップレベル) | Hyperdrive の binding `DTAKO_HYPERDRIVE` (`wrangler.toml` の `[[hyperdrive]]`、トップレベルにだけ置く) | Worker → Hyperdrive → DB。接続・TLS・接続の使い回しは Hyperdrive が受け持つ (接続の部品は `alc_worker_db::hyperdrive::connect`) |
| 3 | ローカル (`--env local`) | 文字列 `DATABASE_URL` (worker 自身の secret / `wrangler dev --var`) | 接続文字列の host:port へ STARTTLS。`sslmode=disable` + var `ALLOW_INSECURE_DB=1` のときだけ手元の PgBouncer へ平文 |

- どれも無ければ 503 (`database_not_configured`)。
- **binding `DTAKO_HYPERDRIVE` が在るのに使えないときは 500** (`internal_error`) で、3 の `DATABASE_URL` へは落ちない。
  落ちるのは binding が無いときだけ。ログに出るのは binding 名・段の label・`kind` だけ (宛先・接続文字列は出ない)。
- **Hyperdrive の設定は、実行用ロールのもの 1 つを複数の worker で共有する (worker ごとに作らない)。** `wrangler.toml` に書くのは
  設定の ID だけ (接続先・資格情報は設定の側に在り、repo に書かない)。`env.*` の下には置かない (本番の DB へ届くため)。
- `DTAKO_DB_VPC` は平文 (trust 認証) なので本番 (トップレベル) に置かない。`ALLOW_INSECURE_DB` はローカル専用で、
  読むのは 3 の段だけ (`wrangler.toml` の vars に書かない)。
- ローカルの `wrangler dev` は **`--env local`** (binding を持たない env。deploy しない) で立てる — `--env` なしだと、
  トップレベルの `DTAKO_HYPERDRIVE` の段に入ってローカルの接続文字列の段へ進まない。
- staging の VPC Service は TCP 型なので **wrangler は 4.78.0 以上** (CI の版は `WRANGLER_VERSION`)。

DB 操作を足すときは、共通 crate `alc-worker-db` の `PgClient::tenant_tx` だけを通す (`BEGIN` → テナントの設定 → 本文 → `COMMIT`)。
中で流せるのは `TenantTx` の型付きの名前なしの文 (`query_typed`・`query_typed_one`・`query_typed_opt`・`execute_typed`) だけで、
名前付き prepared statement は呼ばない (Hyperdrive 経由で接続が切れる)。R2 など JS の値の await は transaction の中に挟めない。

## R2

| env | binding | bucket |
|---|---|---|
| 本番 (トップレベル) | `DTAKO_R2` | `ohishi-dtako` |
| staging | `DTAKO_R2` | `ohishi-dtako-staging` |

**staging の R2 は staging 用の bucket。** `env.*` から本番の bucket を指さない (本番の object を上書きする。
`scripts/check-exposure.sh` が検査する)。骨組みの時点では binding を置いただけで、コードからは使っていない。

## 到達面

- **本番の到達経路は auth-worker からの Service Binding だけ。** JWT を検証せず `X-Tenant-ID` を
  信頼するので、トップレベルは `workers_dev` / `preview_urls` を false にし、`route` / `routes` を持たない。
- **staging はテストから叩くため `workers_dev = true`**
  (URL は `wrangler deploy --env staging` の出力を見る)。**この workers.dev は Cloudflare Access で保護する前提**
  (アプリ・ポリシー・service token は運用側が Access に設定する。repo には持たない)。
  Access を通らないリクエストは Worker に届かず、Access がログインへの 302 か 403 を返す
  (`deploy.yml` が staging の配信の後に、token 無しの `/api/uploads` で確かめる)。
- `workers_dev = true` を許すのは `env.staging` だけ。`scripts/check-exposure.sh` が CI で毎回これを検査し、
  `scripts/check-exposure-test.sh` が陰性対照 (wrangler.toml を崩すと exit 1) を回す。
- **`wrangler.toml` の表の順を変えない・前の方に表を足さない** (陰性対照が、特定の表の直前に行を挿して崩す作りのため。`CLAUDE.md`)。
- secret・binding・入口 (route) を増やさない。Durable Object と Container は持たない (検査が落とす)。

## ビルドと検査 (CI の `ci.yml` と同じもの)

```bash
bash scripts/check-exposure.sh && bash scripts/check-exposure-test.sh
cargo fmt --check
cargo clippy --locked --target wasm32-unknown-unknown --release -- -D warnings
cargo install worker-build@0.8.7 --locked
worker-build --release
npx wrangler@4.144.0 deploy --dry-run            # 配信しない
npx wrangler@4.144.0 deploy --dry-run --env staging
```

toolchain は CI の `dtolnay/rust-toolchain@1.92.0` (`rust-toolchain.toml` は置いていない)。テストと coverage の gate はまだ無い
(口を足す PR で入る)。
