# alc-dtako-worker

デジタコの運行 CSV のアップロード・分割を提供する Cloudflare Worker `alc-dtako` (workers-rs + tokio-postgres)。
rust-alc-api を Cloudflare Workers へ段階移行する 2 本目 (Refs ippoan/rust-alc-api#725) で、型は 1 本目の
ippoan/alc-vein-worker に合わせている。

口は **`POST /split-csv/{upload_id}`** (アップロード 1 件の分割。`/api` 付きでも受ける) の 1 本。それ以外の path は、
tenant ヘッダー無しが 401・有りが 404 (どのリクエストも、その前に DB へ繋ぐので、繋げなければ 503 / 500)。
アップロード本体の口は後続の PR で足す。本番ではまだ誰もこの口を呼ばない (auth-worker の振り分け表に足すのは、アップロード本体が揃った後)。

## 配置

| 場所 | 中身 |
|---|---|
| 直下 (`Cargo.toml`・`wrangler.toml`・`src/`) | Worker 本体 (package `alc-dtako-worker`、wasm32-unknown-unknown)。workspace の root で、`Cargo.lock` はここの 1 つだけ。`src/lib.rs` (workers-rs への載せ方)・`src/db.rs` (DB への経路)・`src/r2.rs` (R2 の binding と待ちを、保存先の層に載せる実装)・`src/tcp.rs` (VPC の binding の extern) |
| `crates/alc-dtako-upload/` | route の crate (package `alc-dtako-upload`)。口 (`src/routes.rs`)・分割の流れ (`src/split.rs`)・SQL の定数 (`src/repo.rs` の `sql`)・それを流す tokio-postgres 実装 (`src/pg.rs`)・保存先の抽象と PUT のやり直し (`src/store.rs`)。接続も R2 の実装も持たない (張るのは Worker とテスト。テストの DB は process の中で起こす組み込みの PostgreSQL — 下の「DB の検査」) |
| `scripts/` | 公開範囲の検査 (`check-exposure.sh` と陰性対照 `check-exposure-test.sh`)、`fetch-migrations.sh` (版は `ALC_MIGRATIONS_REV`)、coverage の gate (`check_coverage_100.sh`、登録簿は直下の `coverage_100.toml`) |
| `.github/workflows/` | `ci.yml` (検査) / `deploy.yml` (デプロイ) / `tag-release.yml` (本番用のタグ) |

## 依存の取り方

- **`alc-core-wasm`** は ippoan/rust-alc-api (public) に残る。直下の `Cargo.toml` の `[workspace.dependencies]` に
  **git 依存・rev 固定で 1 か所だけ**書き、Worker と `crates/alc-dtako-upload` は `workspace = true` で継承する。
  出どころが 2 つになると `TenantId` が別の型になり、**コンパイルは通るのに全リクエストが 500** になる
  (layer が入れる型と route が取り出す型が合わない)。確かめ方:

  ```bash
  cargo tree -i alc-core-wasm --target wasm32-unknown-unknown   # 出どころが 1 つだけ
  ```
- **`alc-csv-parser`** (分割の純粋な部分 `split_csv_entry`・`cap_sorted`。backend と同じ関数を呼ぶ) も ippoan/rust-alc-api の crate。
  `alc-core-wasm` と**同じ rev** で、同じく `[workspace.dependencies]` の 1 か所に書く (`default-features = false` = zip の展開を引かない。
  zip の既定 features は C の依存を連れてきて wasm32 に載らないので、展開は route の crate が `zip` を deflate だけで直接引く)。
- **`alc-worker-db`** (テナントの transaction の部品 `PgClient`・`TenantTx`・`TxOutput`) は ippoan/alc-worker-kit (public) に在る。
  同じく直下の `[workspace.dependencies]` に **git 依存・rev 固定で 1 か所だけ**書く (feature `chrono`。
  出どころが 2 つになると `PgClient` が別の型になる)。
- テストが流す SQL (`init_local_db.sql`・`local_app_grants.sql`・`migrations/`) は、写しを置かず、正本の ippoan/alc-migrations (public) から
  **版を固定して**取る。版は `scripts/ALC_MIGRATIONS_REV` の 1 行 (**rev を書くのはこのファイルだけ**)。`bash scripts/fetch-migrations.sh` が
  repo 直下の `.alc-migrations/` (`.gitignore` 済み) に取り出す (同じ rev が在れば何もしない)。**テスト・coverage の計測の前に必ず通す。**
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

## DB の層 (`crates/alc-dtako-upload`)

DB 操作は、共通 crate `alc-worker-db` の `PgClient::tenant_tx` だけを通す (`BEGIN` → テナントの設定 → 本文 → `COMMIT`)。
中で流せるのは `TenantTx` の型付きの名前なしの文 (`query_typed`・`query_typed_one`・`query_typed_opt`・`execute_typed`) だけで、
名前付き prepared statement は呼ばない (Hyperdrive 経由で接続が切れる)。R2 など JS の値の await は transaction の中に挟めない。
SQL は `src/repo.rs` の `sql` の 1 か所、流すのは `src/pg.rs` の 1 か所 (関数 1 回 = 1 トランザクション。引数の型の並びはここだけ)。
テナントを設定した接続 (RLS) で流し、加えて `WHERE tenant_id` でも絞る。

| `pg` の関数 | SQL の定数 | 返すもの |
|---|---|---|
| `upload_zip_key(pg, tenant_id, upload_id)` | `SELECT_UPLOAD_ZIP_KEY` | アップロード 1 件の ZIP の R2 の key (`Option<String>`。行が無い・key が NULL はどちらも `None`) |
| `mark_has_kudgivt(pg, tenant_id, unko_nos)` | `MARK_HAS_KUDGIVT` | 渡した運行NO の運行に分割済みの印を付け、付けた行の運行NO を返す (`RETURNING` のまま。同じ運行NO が乗務員ごとに複数行あればその数だけ返る — 呼び手が集合にする)。空の入力は transaction を開かない。backend は 100 件ずつの `IN (…)` だったが、`= ANY($2)` の 1 文にしている (結果は同じ) |
| `uploads_needing_split(pg, tenant_id)` | `LIST_UPLOADS_NEEDING_SPLIT` | 分割がまだの運行がテナントに 1 件でも在るとき、completed で key の在るアップロードの `(id, filename)` を新しい順に |

分割の口が使うのは `upload_zip_key` と `mark_has_kudgivt` (`uploads_needing_split` は、後続の口が使う)。

## 分割の口 `POST /split-csv/{upload_id}`

アップロード済みの zip を R2 から読み、CSV を運行NO ごとに分けて R2 に置き、KUDGIVT を置けた運行に印 (`has_kudgivt`) を付ける。
backend (ippoan/rust-alc-api) の `POST /api/split-csv/{upload_id}` と同じ仕事で、**置く key (`{テナント}/unko/{運行NO}/{CSV名}`) と
中身のバイト列は backend と同じ** (読む側が object の ETag を指紋に使う)。1 エントリを分ける本体は、backend と共有の
`alc_csv_parser::split_csv_entry` を呼ぶ (写しを持たない。`alc-csv-parser` は ippoan/rust-alc-api の crate)。

- 流れ (`src/split.rs` の `split_upload`。axum に依らない): zip の key を引く → zip を読む → **全エントリが展開できることを先に確かめる**
  (ここまでは何も書かない) → エントリを 1 つずつ「展開 → 分ける → 置く (失敗したものだけやり直す)」→ KUDGIVT を置けた運行に印。
  メモリに載るのは zip 全体と、処理中の 1 エントリだけ。R2 の await は DB の transaction の中に挟まない。
- この口は、テナントを設定した接続で、テナントでも絞って引く (`pg::upload_zip_key`・`pg::mark_has_kudgivt`)。
- 応答 200 は backend と同じ 7 フィールド: `status` (`"ok"`)・`upload_id`・`split_failed` (置けなかった CSV の数。KUDGIVT 以外も数える)・
  `split_unko_nos` / `split_unko_nos_total` (KUDGIVT を置けた運行NO。一覧はソートして 500 件で切り、総数は `_total`)・
  `split_failed_unko_nos` / `split_failed_unko_nos_total` (KUDGIVT を置けなかった運行NO。印は付けない)。
- エラー: アップロードの行が無い・zip の key が入っていない → **404** `{"error":"not_found"}` / それ以外 (DB・保存先・zip) → **500**
  `{"error":"internal_error"}`。本文に原因は出さない。`upload_id` が UUID でなければ axum の既定の 400。
- ログは State に持たせた差し込み口 (`split::LogSink`) から出す (worker は `console_warn!` / `console_error!`、テストは溜める偽物)。
  出すのは「固定の語 + 段の名前 + kind + 件数」まで。**key・運行NO・upload_id・テナント ID・エラーの生の文を出さない。**

### 旧 (backend) との違い

| | この worker | backend |
|---|---|---|
| 見つからない | **404** | 500 |
| エラーの本文 | JSON の固定の文 (`{"error":"…"}`) | 平文 |
| PUT の同時数 | **6** (Workers の同時接続の上限に合わせた) | 20 |
| PUT のやり直し | **エントリ (zip の中の 1 ファイル) ごと**に最大 3 回・待ち 300ms / 800ms。待ちの合計は最大でエントリ数ぶん | 全 item をまとめて回単位で最大 3 回・同じ待ち |
| zip の圧縮方式 | **deflate と無圧縮だけ** (ほかの方式のエントリが在ると 500) | zip crate の既定 (bzip2・zstd・deflate64・lzma 等も) |
| 書き始める前 | 全エントリの展開を先に確かめる (壊れたエントリが在れば何も書かない)。エントリの展開は 2 回 | 全エントリを展開してメモリに持ってから書く (同じく、壊れていれば何も書かない) |
| やり直しのログ | 最後に残った失敗の件数だけ | やり直すたびに件数 |

**`src/r2.rs` と `src/lib.rs` (wasm 専用) は CI のテストの外。** マージの後に staging で実物 (R2 と DB) を通して確かめる。

## 保存先の層 (`crates/alc-dtako-upload/src/store.rs`)

口のコードから R2 を切り離すための小さな層。R2 の binding (`worker::Bucket`) は wasm32 でしか動かないので、口の流れを native の
テストで通すときは偽の保存先を差す。**R2 を包む実装は直下の worker の `src/r2.rs`** (`worker::Bucket` と `worker::Delay` を `worker::send` の
`SendWrapper`・`SendFuture` で包む。wasm 専用で、CI のテストと coverage の gate の外)。

- `trait ObjectStore`: `get(key)` → `Option<Vec<u8>>` (object が無ければ `None`) / `put(key, bytes, content_type)`。
  `trait Sleeper`: `sleep_ms(ms)` (worker では `worker::Delay`)。どちらも `Send + Sync` で、返す future も `Send` (axum の state と handler が要求する。Workers の R2 の値と future は `Send` でないので、実装する側が包む)。
- `StoreError` が持つのは段の名前 (コードに書いた固定の語) だけ。key・bucket 名・ランタイムの生のエラー文を載せない。
- `put_all_with_retry(store, sleeper, items)`: **失敗した PUT だけ**を最大 3 回 (`PUT_RETRY_ATTEMPTS`) までやり直す (成功済みは再送しない)。
  回の中は同時 6 本 (`PUT_CONCURRENCY`。backend は 20。Workers の同時接続の上限に合わせた)。回の後に失敗が残り、次の回が在るときだけ
  300ms・800ms (`PUT_RETRY_DELAYS_MS`) を待つ。回数と待ちは backend と同じ。返すのは item の `tag` だけ (`succeeded` / `failed`、順不同)。
- 待ちに `tokio::time` を使わない (wasm32 で動かない)。`tokio` は route の crate の dev-dependency だけ。

## R2

| env | binding | bucket |
|---|---|---|
| 本番 (トップレベル) | `DTAKO_R2` | `ohishi-dtako` |
| staging | `DTAKO_R2` | `ohishi-dtako-staging` |

**staging の R2 は staging 用の bucket。** `env.*` から本番の bucket を指さない (本番の object を上書きする。
`scripts/check-exposure.sh` が検査する)。読み書きは `src/r2.rs` (分割の口が zip を読み、分けた CSV を置く)。

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
bash scripts/fetch-migrations.sh                 # 下の「DB の検査」
cargo llvm-cov --locked -p alc-dtako-upload --text > cov.txt && bash scripts/check_coverage_100.sh --use-cache cov.txt
```

toolchain は CI の `dtolnay/rust-toolchain@1.92.0` (`rust-toolchain.toml` は置いていない)。

### DB の検査 (SQL の定数) と coverage の gate

`crates/alc-dtako-upload/tests/sql_db.rs` は、**worker が使うものと同じ実装** (`alc_dtako_upload::pg`) に native の tokio-postgres の接続を渡し、
**テストの process の中で起こす組み込みの PostgreSQL** (dev-dependency の `pglite-oxide`) に流す (ippoan/alc-vein-worker と同じ型)。
docker も外の DB も env も要らない。`#[ignore]` ではない。

```bash
bash scripts/fetch-migrations.sh                                        # 先に要る (テストが .alc-migrations の SQL を流す)
cargo test -p alc-dtako-upload --test sql_db
# coverage の gate つき (CI はこの形の 1 回だけ。cargo-llvm-cov が要る)
cargo llvm-cov --locked -p alc-dtako-upload --text > cov.txt && bash scripts/check_coverage_100.sh --use-cache cov.txt
```

`tests/split_flow.rs` (13 本。口から、組み込みの PostgreSQL と偽の保存先まで) は分割の口を確かめる: 置かれた key と中身が、同じ zip に
`split_csv_entry` を当てた結果と集合として一致 / 印 / 応答の 7 フィールド / 別テナントのアップロード・key が NULL・存在しない id は 404 で
何も書かない / zip が無い・壊れている (途中のエントリ) は 500 で何も書かない / PUT が 2 回失敗して 3 回目に成功 / 3 回とも失敗した運行には印を付けない /
印が当たらない運行NO はログに件数 / DB の失敗は 500 でログに段と kind / 一覧の 500 件上限 / 本文とログに key・運行NO・テナント ID が出ない。

`tests/store.rs` (9 本。偽の保存先と偽の待ち。DB も R2 も要らない) は保存先の層を確かめる: 1 回で全部成功なら待たない /
1 回失敗は 2 回目で成功し、成功済みは再送しない / 2 回失敗は 3 回目で成功 (待ちは 300・800) / 3 回とも失敗は `failed` (3 回目の後は待たない) /
同時に走る PUT は 6 本まで / 空の入力は何も呼ばない / `get` の 3 通り / `StoreError` の文に key が出ない。

CI は target ごとに本数を固定で見る (`sql_db` は `7 passed`、`store` は `9 passed`、`split_flow` は `13 passed`、どれも `0 failed; 0 ignored`)。減らすと落ちる。
足したら `ci.yml` の数も上げる。`sql_db` が確かめること (7 本):

- ZIP の key: 自テナントの id で引ける / 別テナントの id・key が NULL の行・存在しない id は `None`
- 分割済みの印: 渡した運行NO の行だけに付き `RETURNING` が返る / 同じ運行NO が 2 行なら 2 つ返る / 別テナントの同じ運行NO は変わらない /
  空の入力は何もしない / 101 件以上を 1 回で渡せる
- 分割待ちの一覧: 未分割が在れば completed かつ key ありの履歴が新しい順 / 未分割が無ければ空 / completed でない・key が NULL・別テナントは出ない
- テナントを設定しない素の接続では `dtako_upload_history`・`dtako_operations` の行が読めない (エラーか 0 行)
- DB のエラーがそのまま `Err` で返り、失敗した transaction が残らない / 権限の無いロールは 42501 / 切れた接続は `Err`

`crates/alc-dtako-upload/src/` の `pg.rs`・`store.rs`・`split.rs`・`routes.rs` は行カバレッジ 100% を保つ (登録簿は直下の `coverage_100.toml`。`repo.rs` は定数だけで実行行が無いので登録しない)。

作りと、本物の DB との違い:

- **migration が未取得だと失敗する** (skip して緑にしない)。接続ロールが superuser / BYPASSRLS / 表の所有者でも失敗する
  (準備の `Embedded::start` が全テストで確かめる。ロールは RLS が効く `alc_api_app`)
- **同時に張れる接続は 1 本。** テストごとに DB を 1 つ起こし、別の接続が要るときは閉じ切ってから次を張る (`tests/embedded/mod.rs`)
- エンジンは **PostgreSQL 17.5** (wasm の 32-bit build)。**PL/pgSQL の `EXCEPTION` ブロックがエラーを受けない**エンジン差が在る
  (`dtako_upload_history`・`dtako_operations` に trigger は無いので、この検査には効かない)
- **直結で流す** (PgBouncer を挟まない)。接続は superuser の session に `SET ROLE` を重ねた形になる
- migration を流す順 (`init_local_db.sql` → 空の `alc_api._sqlx_migrations` → `migrations/*.sql` を 1 ファイル 1 transaction →
  `local_app_grants.sql` → `ALTER ROLE alc_api_app LOGIN`) は ippoan/alc-vein-worker の `container/start.sh`・テストと同じ
- 初回は `~/.cache/pglite-oxide` に runtime の cache (約 99MB) が書かれる。unix socket は TMPDIR (長いときは `crates/alc-dtako-upload/.dtako-pg-*/`、
  `.gitignore` 済み) に置き、テストの終わりに消す

#### lock の pin

`pglite-oxide` は `=0.5.0` に固定している。その依存の wasmer 系 13 crate は、素の解決だと Rust 1.93 を要求する版になり、
この repo の toolchain (1.92.0) で build が落ちる。`Cargo.lock` で alpha 版に pin している (ippoan/alc-vein-worker と同じ版)。
**toolchain は上げない。** lock を作り直したときは pin し直す:

```bash
for c in wasmer wasmer-compiler wasmer-derive wasmer-types wasmer-vm; do cargo update -p "$c" --precise 7.2.0-alpha.2; done
for c in wasmer-wasix wasmer-wasix-types wasmer-config wasmer-journal wasmer-package virtual-fs virtual-mio virtual-net; do
  cargo update -p "$c" --precise 0.702.0-alpha.2
done
grep -c -- '-alpha' Cargo.lock                                                                # 13
cargo tree --target wasm32-unknown-unknown -e normal | grep -c -i -E 'pglite|wasmer'          # 0 (本番の wasm に入らない)
```
