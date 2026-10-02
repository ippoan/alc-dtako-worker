# alc-dtako-worker

デジタコの運行 CSV のアップロード・分割を提供する Cloudflare Worker `alc-dtako` (workers-rs + tokio-postgres)。
rust-alc-api を Cloudflare Workers へ段階移行する 2 本目 (Refs ippoan/rust-alc-api#725) で、型は 1 本目の
ippoan/alc-vein-worker に合わせている。

口は **`POST /upload`** (zip の取り込み)・**`POST /internal/rerun/{upload_id}`** (やり直し)・**`POST /split-csv/{upload_id}`** (アップロード 1 件の分割)・**`POST /split-csv-all`** (一括分割) の 4 本 (どれも `/api` 付きでも受ける)。それ以外の path は、
tenant ヘッダー無しが 401・有りが 404 (どのリクエストも、その前に DB へ繋ぐので、繋げなければ 503 / 500)。
本番ではまだ誰もこの口を呼ばない (auth-worker の振り分け表に足すのは後)。

## 配置

| 場所 | 中身 |
|---|---|
| 直下 (`Cargo.toml`・`wrangler.toml`・`src/`) | Worker 本体 (package `alc-dtako-worker`、wasm32-unknown-unknown)。workspace の root で、`Cargo.lock` はここの 1 つだけ。`src/lib.rs` (workers-rs への載せ方)・`src/db.rs` (DB への経路)・`src/r2.rs` (R2 の binding と待ちを、保存先の層に載せる実装)・`src/tcp.rs` (VPC の binding の extern) |
| `crates/alc-dtako-upload/` | route の crate (package `alc-dtako-upload`)。口 (`src/routes.rs`)・取り込みの流れ (`src/ingest.rs`)・分割の流れ (`src/split.rs`)・zip の展開 (`src/archive.rs`。私有)・段ごとの所要 (`src/timing.rs`)・SQL の定数 (`src/repo.rs` の `sql`)・それを流す tokio-postgres 実装 (`src/pg.rs`)・保存先の抽象と PUT のやり直し (`src/store.rs`)。接続も R2 の実装も持たない (張るのは Worker とテスト。テストの DB は process の中で起こす組み込みの PostgreSQL — 下の「DB の検査」) |
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
- **`alc-compare`** (日別の労働時間とセグメントの計算 `upload_daily::compute_daily_hours`。backend と同じ関数を呼ぶ) も ippoan/rust-alc-api の crate。
  上の 2 つと**同じ rev** (3 行とも同じ値) で、同じく `[workspace.dependencies]` の 1 か所に書く。`alc-csv-parser` を中から引くので、
  `cargo tree -i alc-csv-parser --target wasm32-unknown-unknown` で出どころが 1 つだけ (直接と `alc-compare` 経由が同じもの) を確かめる。
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
(`server-timing` は `connect;dur=` = DB への接続に掛かった時間。取り込みの口は、その後ろに段ごとの所要が続く)。Cloudflare の token は org の secret を使う
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

分割 1 件の口が使うのは `upload_zip_key` と `mark_has_kudgivt`、一括分割の口は候補を `uploads_needing_split` で引く。

### アップロードの取り込みの段 (呼ぶのは `POST /upload` の流れ = `ingest.rs`)

zip の取り込みのうち DB に書く部分。**SQL は backend (ippoan/rust-alc-api) の文と同じ** (同じ入力が同じ行になることを、文が同じであることで担保する。
まとめ直さない)。`pg.rs` は 2 つの層に分かれる: `&mut PgClient` を取って transaction を 1 回開く**段の関数**と、その中から呼ぶ
`&TenantTx` を取る**文の関数** (transaction を開かない。`TenantTx` はテナントを設定した transaction の中でしか手に入らない)。

| 段の関数 | すること |
|---|---|
| `create_upload(pg, tenant_id, filename)` | 履歴を作り id を返す (id は DB の既定値)。**テナントが存在しない**ときは `CreateUploadError::TenantNotFound` (ほかの DB の失敗と区別する) |
| `set_upload_zip_key(pg, tenant_id, upload_id, key)` | 履歴に zip の key を記録する。**単独の transaction** (後の段が落ちても key は残る)。返すのは更新した行数 |
| `prepare_upload(pg, tenant_id, rows, kudgivt_rows)` | 1 transaction の中で、**KUDGURI の行の順に** 営業所・車輌・乗務員を解決し、運行が既に在るかを見て、続けて分類を読み、未登録のイベントCD を既定の分類で足す。「既に在る」= DB に在る、または同じ zip の中で先の行に同じ (運行NO, crew_role) が出た |
| `apply_upload(pg, tenant_id, upload_id, rows, inputs, daily)` | 取り込みの本体。**1 transaction の中で** 運行の入れ替え (`replace_operations_in`。行の順に入れ替え、前の行と違えば変更記録) → 日別の保存 (`save_daily_hours`) → 履歴に完了の印 (`operations_count` = 流した**行数**。運行NO の種類の数ではない)。途中で落ちたら、運行も日別も履歴も元のまま。`rows` と `inputs` の数が違えば DB を触る前に `ApplyUploadError::LengthMismatch` |
| `mark_upload_failed(pg, tenant_id, upload_id, label)` | 履歴に失敗の印を付ける。`label` は呼び手が渡す固定の語 (生のエラー文を入れない) |

- **乗務員の解決 (`upsert_driver`)**: 乗務員CD は `code` 列に入っていることも `driver_cd` 列に入っていることも在るので、順に当てる —
  `code` の行 (`driver_cd` が NULL なら埋める。同じ driver_cd の別の生存行が在れば埋めない) → `driver_cd` の行 → 新規 → 一意の制約に当たったら引き直す。
  これは運行の `driver_id` 用で、日別の集計が使う乗務員の引き当てとは別 (1 つにまとめない)。
- **運行の入れ替え (`replace_operation`)**: 旧 snapshot → 消す → 入れる → 旧が在ったときだけ新 snapshot と比べ、違えば変更記録
  (`reason = "reupload"`)。snapshot への分数の足し方・比べ方・記録に載せる乗務員CD は、backend と共有の `alc_csv_parser::operation_changes` を呼ぶ。
  変更記録は追記だけ (UPDATE・DELETE を書かない)。
- 日時 (出発・帰着・出庫・入庫) は、KUDGURI の壁時計を**そのまま UTC の時刻として**入れる。営業所・車輌・乗務員の cd が空なら DB を引かず NULL。
- 分類は `(event_cd, 分類の文字列)` で返す。`PreparedUpload::classification_map()` が `EventClass` の map にする。
- **日別の保存 (`save_daily_hours`)**: `daily` は `alc_compare::upload_daily::compute_daily_hours` (backend と同じ関数) の出力そのまま。
  ここでは計算しない (保存する値のうち 2 つは `DailyHours::saved_total_drive_minutes()`・`saved_late_night_minutes()` を呼ぶ。ほかは field の写し)。順:
  1. 日エントリの乗務員CD のうち空でないものの id を引く (`get_employee_id_by_driver_cd` = `code` の行 → 無ければ `driver_cd` の行。**読むだけ**で、
     埋めない・作らない。運行の `upsert_driver` とは別の id を返しうる)。id が引けない CD と空の CD の日エントリは、消す対象にも保存の対象にもしない
  2. 引けた乗務員ごとに、全日エントリの運行NO で、セグメントと日別 (`unko_nos` が重なる行) を消す (上げ直しで帰属日が変わっても古い行が残らない)
  3. 日エントリを保存: (乗務員, 日, 開始時刻) の日別を消す → 日別を入れる → (乗務員, 日) のセグメントを消す → セグメントを入れる
- 日別の日は `DATE`、開始時刻は `TIME`、セグメントの開始・終了は壁時計を**そのまま UTC の時刻として**入れる。

#### 旧 (backend) との違い

- **保存の順と、セグメントの消し方が決定的**: 日エントリを (乗務員CD, 日, 開始時刻) の順に保存し、「(乗務員, 日) のセグメントを消す」は
  **(乗務員, 日) ごとに最初の 1 回だけ**流す。旧は順が決まっておらず、日エントリごとに毎回消すので、同じ乗務員・同じ日に日エントリが 2 つ以上在ると、
  先に入れたセグメントが後の日エントリの保存で消え、結果が実行ごとに変わりうる。worker は両方のセグメントが残る。
- 「運行NO でセグメントを消す」は、乗務員ごとに `unko_no = ANY($3)` の 1 文 (旧は 乗務員 × 運行NO の数だけ流す。消える行は同じ)。
- 完了の印の UPDATE に `AND tenant_id` を足している (RLS に加えて文でも絞る。ほかの文と同じ形)。
- 運行の入れ替え・日別の保存・完了の印が 1 つの transaction (旧は文ごとに別)。そのため、1 回のアップロードで入る運行と変更記録の時刻
  (`created_at`・`recorded_at` などの DB の既定値) は全部同じ値になる (旧は行ごとに別の時刻)。

## アップロードの口 `POST /upload`

デジタコの zip を受け取り、保存先に置き、運行と日別を DB に入れ、運行NO ごとの CSV に分割する。backend (ippoan/rust-alc-api) の
`POST /api/upload` と同じ仕事・同じ順で、**応答の形も同じ**。流れは `crates/alc-dtako-upload/src/ingest.rs`。

- 入力: `multipart/form-data` の **`file` field** (filename が無ければ `upload.zip`)。body は 20MB まで。
- 段の順:
  1. 履歴を作る (`processing`)
  2. zip を保存先に置く (key = `{テナント}/uploads/{履歴の id}/{filename}`。filename は加工しない)
  3. key を履歴に記録する
  4. zip を展開して KUDGURI と KUDGIVT を読む (名前に `KUDGURI`・`KUDGIVT` を含む最初のエントリ。Shift_JIS)。
     KUDGURI が 0 行なら、KUDGIVT が無くても運行 0 件として進む
  5. 営業所・車輌・乗務員を解決し、運行が既に在るかを見て、分類を読む (上の `prepare_upload`)
  6. 既に在る運行だけ、前回の分割が置いた旧 KUDGIVT (`{テナント}/unko/{運行NO}/KUDGIVT.csv`) を保存先から読んで前回の分数を出す
     (運行NO ごとに 1 回。読めない・無い・parse できないときは「取れなかった」として続け、変更記録の before に印が残る)。
     今回の分数と日別は、backend と共有の関数 (`alc_csv_parser::operation_changes`・`alc_compare::upload_daily::compute_daily_hours`) で出す
  7. 運行の入れ替え + 日別の保存 + 完了の印 (上の `apply_upload`。1 transaction)。`operations_count` は KUDGURI の行数
  8. 分割 (下の分割の口と同じ `split_upload`。zip は保存先から読み直す)。丸ごと失敗したら待って、全体を最大 3 回 (待ち 300ms・800ms)。
     尽きても応答は 200 のままで、`split_failed` が 1 になる (後から分割の口で復旧できる)
- 応答 (200): `upload_id`・`operations_count`・`status` (`"completed"`)・`split_failed`・`split_unko_nos`・`split_unko_nos_total`・
  `split_failed_unko_nos`・`split_failed_unko_nos_total` (運行NO の一覧は 500 件で切り、総数は `_total`)。
- 失敗:
  - **入力の誤りは 400 `{"error": "<語>"}`**。語は固定: `invalid_multipart` (multipart として読めない・body が上限を超える)・`no_file`・
    `tenant_not_found`・`invalid_zip`・`zip_too_large`・`kudguri_not_found`・`kudguri_invalid`・`kudgivt_not_found`・`kudgivt_invalid`
  - **保存先・DB の失敗は 500 `{"error": "internal_error"}`** (原因はログに、段の名前と kind だけ)
  - 履歴を作った後の失敗は、履歴に失敗の印を付けてから返す (`error_message` は上の語。500 のときは段の名前 `storage`・`db`)。
    準備 (5) が通って 7 が失敗したとき、営業所・車輌・乗務員・分類の行は残る
- **段ごとの所要**: 応答 (成功でも失敗でも) の `Server-Timing` に、終えた段の所要 (ミリ秒) を終えた順に載せる
  (`history`・`put_zip`・`parse`・`prepare`・`old_kudgivt`・`apply`・`split`。やり直しの口は頭が `zip_key`・`get_zip`)。載せるのは
  **固定の語の名前と数字だけ**で、件数・id・key は載せない。直下の worker が、その前に接続の所要 `connect` を足す。
  時計は差し込み (`timing.rs` の `Clock`。worker はランタイムの時計、テストは固定の歩幅の偽物)。**Workers の時計は I/O をまたがないと進まない**ので、
  I/O を含まない段 (`parse` と、旧 KUDGIVT を読まないときの `old_kudgivt`) は 0 に見え、その時間は次の I/O を含む段に乗る。
- zip の展開 (`archive.rs`。分割の口も同じものを使う): 圧縮は deflate と無圧縮だけ。**非圧縮サイズの合計が 64MB を超える zip は展開しない**
  (アップロードでは `zip_too_large`、分割では失敗)。読むときも、エントリに書かれた非圧縮サイズまでしか読まない (書かれた値より中身が大きい zip は不正)。

### 旧 (backend) との違い

- **400 の本文は固定の語**。旧と本文の形が違う (旧は平文の理由、こちらは `{"error": "<固定の語>"}`)。履歴の `error_message` も同じ語。入力の値・テナント ID・エラーの生の文を、本文・履歴・ログに出さない。
- **保存先・DB の失敗は 500** (旧は取り込みの中の失敗を、種類を問わず 400 で返す)。
- **展開後の大きさに上限 (64MB)** が在り、圧縮は deflate と無圧縮だけ。旧より受け付ける zip が狭い (上限を超える zip と、deflate・無圧縮以外の zip は入力の誤りになる)。
- 取り込みの本体が 1 つの transaction (上の「アップロードの取り込みの段 > 旧との違い」。旧は行ごとに別の transaction)。途中で落ちたら、運行も日別も入らない。
  日別のセグメントの消し方が決定的 (同じ節)。
- 旧 KUDGIVT を読むのは運行NO ごとに 1 回 (旧は行ごと)。
- **呼び手との接続が切れると、失敗の印も付かずに途中で止まることがある** (Workers はリクエストが終わると処理を打ち切る)。履歴が `processing` のまま残る・
  分割が未完になる、がありうる。運行と日別は 1 つの transaction なので半端には入らない。取り込みが終わっていない履歴は、やり直しの口 (下) で、
  分割の未完は、分割の口 (下) で復旧する。

## やり直しの口 `POST /internal/rerun/{upload_id}`

既に保存先に在る zip を、もう一度取り込む (失敗した履歴の復旧に使う)。backend (ippoan/rust-alc-api) の `POST /api/internal/rerun/{upload_id}` と
同じ仕事。流れは `ingest.rs` の `rerun_upload` で、**アップロードの口の段 4 から後ろと同じもの**を通す (同じ流れを 2 つ持たない)。

- 履歴の zip の key を引く → zip を保存先から読む → 展開して読む → 準備 → 前回の分数 → 運行の入れ替え + 日別の保存 + 完了の印 → 分割 (最大 3 回)。
- 履歴を作らない・zip を保存先に置き直さない・key を更新しない。履歴の status は、始めるときには変えない (成功で `completed` と行数)。
- 応答 (200) はアップロードの口と同じ 8 フィールド (`upload_id` は path の id)。`Server-Timing` も同じ形 (頭の 2 段が `zip_key`・`get_zip`)。
- 失敗:
  - **404 `{"error":"not_found"}`**: 履歴が無い・zip の key が入っていない (分割の口と同じ形)
  - 400 と固定の語: zip の中身の誤り (アップロードの口と同じ語)。UUID でない id は axum の既定の 400
  - 500 `internal_error`: zip が保存先に無い・読めない (履歴の語は `storage`)、DB の失敗 (`db`)
  - key を引けた後の失敗は、アップロードの口と同じく履歴に失敗の印 (同じ語) を付けてから返す

### 旧 (backend) との違い

- **やり直せるのは、ヘッダーのテナントの履歴だけ** (ほかは 404)。
- zip を保存先に置き直さない (中身は同じなので結果は変わらない)。
- 失敗の本文と status は、アップロードの口と同じ (入力の誤りは 400 と固定の語、保存先・DB の失敗は 500)。

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

## 一括分割の口 `POST /split-csv-all`

テナントに「分割がまだの運行」が 1 件でも在るとき、分割の元にできる (completed で zip の key が在る) アップロードを**新しい順に最大 50 件**、
分割し直す。backend の `POST /api/split-csv-all` と同じ仕事で、1 件ぶんの本体は上の分割 1 件と同じ `split::split_upload`。

- 応答は 200・`Content-Type: text/event-stream` (`Cache-Control: no-cache`・`X-Accel-Buffering: no`)。本文は `data: <JSON>` と空行の繰り返しで、
  **1 件処理するごとに 1 個**流す (まとめて出さない):
  - `{"event":"progress","current":n,"total":m,"filename":"…"}` — `total` は今回処理する数 (候補の数と 50 の小さい方)
  - `{"event":"done","candidates":…,"total":…,"success":…,"failed":…,"skipped":…}` — `candidates` は候補の総数、`total = success + failed`、
    `skipped = candidates − total` (上限で今回処理しなかった残り。もう一度呼ぶと続きではなく、その時点の候補を新しい順に引き直す)。
    置けなかった CSV が在っても、その履歴は `success` に数える (backend と同じ)
  - `{"event":"error","message":"internal_error"}` — 候補の取得 (DB) に失敗したとき。これ 1 個で終わり、`done` は出ない
- 候補が 0 件なら `done` (全部 0) が 1 個だけ。
- 履歴 1 件の分割が失敗しても止めずに次へ進み、`failed` に数える。ログに出すのは段の名前と kind だけ (upload の id・filename・エラーの生の文を出さない)。
  `filename` が出るのは本文の `progress` だけ。

### 旧 (backend) との違い

| | この worker | backend |
|---|---|---|
| 処理の進め方 | **1 件ずつ**順に | 5 件ずつ並列 |
| 途中の event | 1 件ごとに `progress` | 無い (終わりの `done` だけ) |
| `error` の `message` | 固定の語 (`internal_error`) | 平文の理由 |
| 途中で切れたとき | 呼び手が切れる・Worker の上限 (CPU 時間・subrequest) に当たると、**`done` も `error` も無く stream が閉じる**。それまでに終えた履歴は分割済み。もう一度呼ぶと同じ候補を先頭からやり直す (分割は冪等) | — |
| DB へ繋げないとき | event ではなく **500 / 503 の JSON** (routing の前に繋ぐため) | — |

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

`tests/upload_flow.rs` (8 本。口から、組み込みの PostgreSQL と偽の保存先まで) はアップロードの口 (6 本) とやり直しの口 (2 本) を確かめる。zip はテストの中で作る
(`upload_zip`。KUDGURI・KUDGIVT は Shift_JIS・CRLF): 端から端 (応答の 8 field・履歴・運行・日別・セグメント・保存先の zip と分割の出力・分割済みの印) /
上げ直し (同じ zip は記録なし・値が変われば before と after の分数・旧 KUDGIVT が無い / 読めないときの印) / リクエストの形の誤り (400 の語・
tenant ヘッダー無しは 401・body は 2MB を超えても通り 20MB を超えると読めない) / zip の中身の誤り (語ごとに履歴の `error_message`・展開後の上限・
KUDGURI が 0 行は 0 件で completed) / 保存先と DB の失敗 (500・履歴の段の名前・取り込みの本体の途中の失敗で運行も日別も入らない・切れた接続) /
分割のやり直し (2 回失敗 → 3 回目で通る・3 回とも失敗 → `split_failed = 1` で 200・後から分割の口で復旧)。やり直しの口: 復旧 (途中で失敗した履歴を
取り込み直す → completed と行数・運行と日別と分割の出力・もう一度やり直しても変更記録が増えない・zip は置き直さない) / 引けない・読めない
(存在しない id・別テナントの履歴・key が NULL は 404 / zip が無い・読めないは 500 / 壊れた zip は 400 / UUID でない id・tenant ヘッダー無し・切れた接続)。
本文とログに識別子が出ないことも見る。

`tests/split_flow.rs` (20 本。口から、組み込みの PostgreSQL と偽の保存先まで) は 2 つの口を確かめる。一括分割 (6 本): 候補 0 件は `done` だけ /
新しい順に 1 件ずつ `progress` → `done` / 1 件の失敗を数えて続ける / 上限 50 件と `skipped` / 候補の取得の失敗は固定の `error` / tenant ヘッダー無しは 401。
本文は、呼び手と同じ読み方 (空行で割り、`data:` の行を JSON に) で読む。**分割の出力の固定の期待値 (1 本)**: Shift_JIS・CRLF の KUDGIVT を通し、
置かれる key と中身を、テストに手で書いたバイト列と比べる (共有の関数を呼んで期待値を作らない。rust-alc-api の rev を上げるときは、上げる前後ともこれが通ることを確かめる)。
分割 1 件 (13 本): 置かれた key と中身が、同じ zip に
`split_csv_entry` を当てた結果と集合として一致 / 印 / 応答の 7 フィールド / 別テナントのアップロード・key が NULL・存在しない id は 404 で
何も書かない / zip が無い・壊れている (途中のエントリ) は 500 で何も書かない / PUT が 2 回失敗して 3 回目に成功 / 3 回とも失敗した運行には印を付けない /
印が当たらない運行NO はログに件数 / DB の失敗は 500 でログに段と kind / 一覧の 500 件上限 / 本文とログに key・運行NO・テナント ID が出ない。

`tests/store.rs` (9 本。偽の保存先と偽の待ち。DB も R2 も要らない) は保存先の層を確かめる: 1 回で全部成功なら待たない /
1 回失敗は 2 回目で成功し、成功済みは再送しない / 2 回失敗は 3 回目で成功 (待ちは 300・800) / 3 回とも失敗は `failed` (3 回目の後は待たない) /
同時に走る PUT は 6 本まで / 空の入力は何も呼ばない / `get` の 3 通り / `StoreError` の文に key が出ない。

CI は target ごとに本数を固定で見る (`sql_db` は `15 passed`、`store` は `9 passed`、`split_flow` は `20 passed`、`upload_flow` は `8 passed`、どれも `0 failed; 0 ignored`)。減らすと落ちる。
足したら `ci.yml` の数も上げる (テスト 1 本ごとに DB を起動して全 migration を流すので、本数を増やさず 1 本に筋書きを束ねる)。
`sql_db` が確かめること (15 本)。取り込みの DB の層 (8 本):

- 乗務員の解決: `code` の行を使って `driver_cd` を埋める / `driver_cd` の行へ落ちる / 新規 / 別の生存行が同じ driver_cd を持つときは埋めない /
  論理削除済みは対象外 / INSERT が一意の制約に当たったら引き直す
- 上げ直しと変更記録: 初回は記録なし / 同じ値は記録なし / 値が変わったら 1 件 (before・after の JSON をリテラルで比べる) / 2 人乗務は crew_role ごと /
  前回の分数が取れないときの印 / 同じ zip の中の重複行 (後の行が残り、流した数は行の数。記録の before・after もリテラルで比べる)
- 運行の 23 列: 全 field に別々の値を入れ、列ごとに読み戻して比べる (引数の順の取り違えを捕まえる)。日時は壁時計がそのまま UTC として入る。
  省ける field が空の行は NULL。同じ cd の営業所・車輌は名前だけ更新する
- テナントの分離・履歴・分類: 同じ cd・同じ運行NO でもテナントごとに別の行 / 別テナントの履歴の id には 0 行 / 存在しないテナントは区別された失敗 /
  未登録のイベントCD を既定の分類で足す (2 回目は増えない)
- 切れた接続では各段が DB の失敗を返す。行と入力の数が違うときは、切れた接続でも DB の失敗ではなく数の不一致が返る (DB に触れていない)
- 日別の保存: 計算の出力 (2 日にまたがる 1 運行) と、全 field に別々の値を入れた日エントリを保存し、日別の 17 列・セグメントの 11 列と乗務員を
  リテラルで読み戻す (引数の順の取り違え・保存する 2 つの値が method の値であること) / 前から在る行のうち、同じ (乗務員, 日, 開始時刻)・
  その日のセグメント・同じ運行NO を持つ別の日の行は消え、当たらない行は残る / 上げ直しで帰属日が変わると古い日別とセグメントが消える /
  その zip に出てこない乗務員の行はそのまま / 履歴が completed と行数になる
- 決定的な消し方と skip: 同じ乗務員・同じ日の日エントリ 2 つ (1 運行の中の休息で分かれる) の両方のセグメントが残る (2 回流しても同じ) /
  乗務員CD が空・id が引けない日エントリは保存されず、乗務員も作られない / 運行は `upsert_driver` の id、日別は `get_employee_id_by_driver_cd` の id に付く
- 日別の分離と失敗: 別テナントの日別・セグメント・履歴は変わらない / 別テナントの履歴の id を渡しても completed にならない /
  セグメントの INSERT が落ちると、運行の入れ替え・変更記録・日別・履歴が元のまま / 行と入力の数が違うと何も変わらない

分割の口が使う 3 関数 (7 本):

- ZIP の key: 自テナントの id で引ける / 別テナントの id・key が NULL の行・存在しない id は `None`
- 分割済みの印: 渡した運行NO の行だけに付き `RETURNING` が返る / 同じ運行NO が 2 行なら 2 つ返る / 別テナントの同じ運行NO は変わらない /
  空の入力は何もしない / 101 件以上を 1 回で渡せる
- 分割待ちの一覧: 未分割が在れば completed かつ key ありの履歴が新しい順 / 未分割が無ければ空 / completed でない・key が NULL・別テナントは出ない
- テナントを設定しない素の接続では、この crate が触る 9 つの表の行が読めない (エラーか 0 行)
- DB のエラーがそのまま `Err` で返り、失敗した transaction が残らない / 権限の無いロールは 42501 / 切れた接続は `Err`

`crates/alc-dtako-upload/src/` の `pg.rs`・`store.rs`・`split.rs`・`routes.rs`・`ingest.rs`・`archive.rs`・`timing.rs` は行カバレッジ 100% を保つ (登録簿は直下の `coverage_100.toml`。`repo.rs` は定数だけで実行行が無いので登録しない)。

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
