//! 車両の現在地のログの退避 (定時実行の `logs_archive::run`) と書き戻しの口 (`POST /internal/archive/restore`) を、
//! DB と偽の保存先まで通して確かめる (native。W9、Refs ippoan/rust-alc-api#680)。
//!
//! DB はテストの process の中で起こす組み込みの PostgreSQL (`embedded/mod.rs`)、保存先・ログは偽物 (`fakes/mod.rs`)。
//! DB はテナントごとの transaction (RLS)。migration 084 の archive_* 関数と archive_mode のポリシーは使わない
//! (どちらも migration 194 で消えた。最初のテストが、無いことを確かめてから流して固定する)。失敗は、superuser で権限・列名・trigger を変えて作る。

/// 084 の archive_mode のポリシーと archive_* 関数が 1 つでも残っていれば失敗する (194 で消えた。テストで消す必要はない)
const ASSERT_NO_ARCHIVE_084: &str = "DO $$ BEGIN \
     IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
                WHERE n.nspname = 'alc_api' AND p.proname LIKE 'archive\\_%') \
        OR EXISTS (SELECT 1 FROM pg_policies \
                   WHERE schemaname = 'alc_api' AND tablename = 'dtakologs' AND policyname LIKE 'archive\\_%') \
     THEN RAISE EXCEPTION 'ci: archive of 084 remains'; END IF; END $$";

/// `op` (INSERT / DELETE) の文を、dtakologs で必ず失敗させる trigger
fn fail_on(op: &str) -> String {
    format!(
        "CREATE FUNCTION alc_api.ci_fail_{op}() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN RAISE EXCEPTION 'ci: {op} fails'; END $$; \
         CREATE TRIGGER ci_fail_{op} BEFORE {op} ON alc_api.dtakologs \
         FOR EACH ROW EXECUTE FUNCTION alc_api.ci_fail_{op}()"
    )
}

// 共用の土台・偽物のうち、このファイルが使わないものが在る
#[allow(dead_code)]
mod embedded;
#[allow(dead_code)]
mod fakes;

use std::io::{Read, Write};
use std::sync::Arc;

use alc_core_wasm::TenantId;
use alc_dtako_upload::logs_archive::{self, RunSummary, MANIFEST_KEY, MAX_DATES_PER_RUN};
use alc_dtako_upload::pg;
use alc_dtako_upload::routes::{tenant_router, DtakoState};
use alc_dtako_upload::split::LogLevel;
use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::{Extension, Router};
use chrono::{DateTime, TimeZone, Utc};
use embedded::{exec, rows_json, tenant, Embedded, Held, APP_ROLE};
use fakes::{FakeClock, FakeSleeper, FakeStore, Logs};
use futures_util::lock::Mutex;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

/// 実行の時刻。境目は 2026-03-13 (この日より前を退避して消す)。
fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 3, 20, 18, 0, 0).unwrap()
}

struct Ctx {
    db: Embedded,
    pg: Held<Arc<Mutex<PgClient>>>,
    store: Arc<FakeStore>,
    logs: Logs,
}

impl Ctx {
    async fn start() -> Self {
        let db = Embedded::start().await;
        let pg = db.shared(APP_ROLE).await;
        Self {
            db,
            pg,
            store: Arc::new(FakeStore::default()),
            logs: Logs::default(),
        }
    }

    fn app(&self, tenant_id: Uuid) -> Router {
        let state = DtakoState {
            pg: self.pg.inner.clone(),
            store: self.store.clone(),
            sleeper: Arc::new(FakeSleeper::default()),
            clock: Arc::new(FakeClock::default()),
            log: self.logs.sink(),
        };
        tenant_router()
            .with_state(state)
            .layer(Extension(TenantId(tenant_id)))
    }

    /// 接続を閉じ切って superuser で `sql` を流し、口の接続を張り直す (同時に張れる接続は 1 本)。
    async fn run_as_superuser(self, sql: &str) -> Self {
        let Self {
            db,
            pg,
            store,
            logs,
        } = self;
        pg.close().await;
        let su = db.superuser().await;
        su.inner.batch_execute(sql).await.unwrap();
        su.close().await;
        let pg = db.shared(APP_ROLE).await;
        Self {
            db,
            pg,
            store,
            logs,
        }
    }

    async fn finish(self) {
        self.pg.close().await;
        self.db.shutdown();
    }

    async fn tenant(&self, name: &str) -> Uuid {
        let mut c = self.pg.inner.lock().await;
        tenant(&mut c, name).await
    }

    /// 車両の現在地のログ 1 行。
    async fn log_row(&self, t: Uuid, at: &str, vehicle_cd: i32) {
        let mut c = self.pg.inner.lock().await;
        let sql = format!(
            "INSERT INTO dtakologs (tenant_id, data_date_time, vehicle_cd, vehicle_name, speed) \
             VALUES ($1, '{at}', {vehicle_cd}, 'TEST-CAR {vehicle_cd}', 12.5)"
        );
        assert_eq!(exec(&mut c, t, &sql).await, 1);
    }

    /// テナントの行の (日時, 車両CD)。日時の順。
    async fn rows(&self, t: Uuid) -> Vec<(String, i64)> {
        let mut c = self.pg.inner.lock().await;
        let q = "SELECT data_date_time, vehicle_cd FROM dtakologs WHERE tenant_id = $1 \
                 ORDER BY data_date_time, vehicle_cd";
        rows_json(&mut c, t, q)
            .await
            .iter()
            .map(|v| {
                let at = v["data_date_time"].as_str().unwrap().to_owned();
                (at, v["vehicle_cd"].as_i64().unwrap())
            })
            .collect()
    }

    async fn run(&self) -> Option<RunSummary> {
        let mut c = self.pg.inner.lock().await;
        let sink = self.logs.sink();
        logs_archive::run(&mut c, self.store.as_ref(), &sink, now()).await
    }

    async fn call(&self, t: Uuid, path: &str, body: Value) -> (StatusCode, Value) {
        let req = Request::builder()
            .method(Method::POST)
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let res = self.app(t).oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await;
        let value = serde_json::from_slice(&bytes.unwrap()).unwrap_or(Value::Null);
        (status, value)
    }

    async fn restore(&self, t: Uuid, date: &str) -> (StatusCode, Value) {
        self.call(t, "/internal/archive/restore", json!({ "date": date }))
            .await
    }

    fn manifest(&self) -> Value {
        let (bytes, content_type) = self.store.object(MANIFEST_KEY).unwrap();
        assert_eq!(content_type, "application/json");
        serde_json::from_slice(&bytes).unwrap()
    }

    /// 保存先の 1 日ぶんを、行ごとの JSON にして返す (1 行目は見出し)。
    fn archived(&self, t: Uuid, date: &str) -> Vec<Value> {
        let key = logs_archive::r2_key(&t.to_string(), date);
        let (gz, content_type) = self.store.object(&key).unwrap();
        assert_eq!(content_type, "application/gzip");
        let mut text = String::new();
        flate2::read::GzDecoder::new(gz.as_slice())
            .read_to_string(&mut text)
            .unwrap();
        text.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
}

fn gz(text: &str) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(text.as_bytes()).unwrap();
    enc.finish().unwrap()
}

/// 境目より前の日だけを (テナント, 日付) ごとに退避し、目録に載せてから消す。key・見出しの行・gzip・目録は
/// rust-alc-api と同じ形で、画面の範囲の読み取りが退避から読める。2 回目は何もしない (目録も書かない)。
#[tokio::test(flavor = "multi_thread")]
async fn run_archives_old_days_then_deletes_them() {
    let ctx = Ctx::start()
        .await
        .run_as_superuser(ASSERT_NO_ARCHIVE_084)
        .await;
    let a = ctx.tenant("archive-a").await;
    let b = ctx.tenant("archive-b").await;
    ctx.log_row(a, "2026-03-01T09:00:00+09:00", 2).await;
    ctx.log_row(a, "2026-03-01T08:00:00+09:00", 1).await;
    ctx.log_row(a, "2026-03-12T10:00:00+09:00", 1).await;
    ctx.log_row(b, "2026-03-01T10:00:00+09:00", 7).await;
    // 境目の日と、それより後は残す
    ctx.log_row(a, "2026-03-13T10:00:00+09:00", 1).await;
    ctx.log_row(a, "2026-03-19T10:00:00+09:00", 1).await;

    let want = RunSummary {
        archived: 3,
        archived_rows: 4,
        deleted: 3,
        deleted_rows: 4,
        failed: 0,
        deferred: 0,
    };
    assert_eq!(ctx.run().await, Some(want.clone()));
    assert!(want.line().contains("archived 3 days (4 rows)"));
    assert_eq!(
        ctx.rows(a).await,
        [
            ("2026-03-13T10:00:00+09:00".to_owned(), 1),
            ("2026-03-19T10:00:00+09:00".to_owned(), 1),
        ]
    );
    assert_eq!(ctx.rows(b).await, []);

    // 1 日ぶん: 見出しの行の後に、時刻の順の行
    let day = ctx.archived(a, "2026-03-01");
    assert_eq!(day.len(), 3);
    assert_eq!(day[0]["_archive_header"], json!(true));
    assert_eq!(day[0]["schema_version"], json!(1));
    assert_eq!(day[0]["table"], json!("alc_api.dtakologs"));
    assert_eq!(day[0]["tenant_id"], json!(a.to_string()));
    assert_eq!(day[0]["date"], json!("2026-03-01"));
    assert_eq!(day[0]["archived_at"], json!(now().to_rfc3339()));
    assert_eq!(day[1]["vehicle_cd"], json!(1));
    assert_eq!(day[2]["vehicle_cd"], json!(2));
    assert_eq!(day[1]["tenant_id"], json!(a.to_string()));
    assert_eq!(day[1]["vehicle_name"], json!("TEST-CAR 1"));
    assert_eq!(ctx.archived(b, "2026-03-01").len(), 2);

    // 目録 (rust-alc-api の Manifest と同じ形)
    let m = ctx.manifest();
    assert_eq!(m["updated_at"], json!(now().to_rfc3339()));
    let entry = &m["archived_dates"][a.to_string()]["2026-03-01"];
    assert_eq!(entry["row_count"], json!(2));
    assert_eq!(entry["archived_at"], json!(now().to_rfc3339()));
    let key = format!("archive/alc_api/dtakologs/{a}/2026/03/01.jsonl.gz");
    assert_eq!(entry["r2_key"], json!(key));
    assert_eq!(
        m["archived_dates"][a.to_string()]["2026-03-12"]["row_count"],
        json!(1)
    );
    assert_eq!(
        m["archived_dates"][b.to_string()]["2026-03-01"]["row_count"],
        json!(1)
    );

    // 画面の範囲の読み取りは、消した日を退避から読む
    let app = ctx.app(a);
    let uri = "/dtako-logs/by-date-range?start_date_time=2026-03-01T00:00:00&end_date_time=2026-03-01T23:59:59";
    let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await;
    let v: Value = serde_json::from_slice(&bytes.unwrap()).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 2);

    // 2 回目: 退避する日も消す日も無く、目録も書かない
    assert_eq!(ctx.run().await, Some(RunSummary::default()));
    assert_eq!(ctx.store.put_calls(MANIFEST_KEY), 1);
    assert!(ctx.logs.all().is_empty());
    ctx.finish().await;
}

/// 目録の行数が DB と合わない日 (rust が今日を含めて退避していた日・退避の後に行が増えた日) は退避し直してから消す。
/// 保存先に書けなかった日は目録に載らず、消さない (次の実行でやり直す)。
#[tokio::test(flavor = "multi_thread")]
async fn run_redoes_days_whose_count_changed_and_keeps_unsaved_days() {
    let ctx = Ctx::start().await;
    let a = ctx.tenant("archive-redo-a").await;
    let b = ctx.tenant("archive-redo-b").await;
    ctx.log_row(a, "2026-03-01T08:00:00+09:00", 1).await;
    ctx.log_row(a, "2026-03-01T09:00:00+09:00", 2).await;
    ctx.log_row(b, "2026-03-02T09:00:00+09:00", 3).await;
    // rust が 1 行の時点で退避していた日 (目録に別のテナントの日も在る)
    let stale = json!({
        "updated_at": "2026-03-01T03:00:00+00:00",
        "archived_dates": {
            a.to_string(): { "2026-03-01": {
                "row_count": 1, "archived_at": "2026-03-01T03:00:00+00:00",
                "r2_key": logs_archive::r2_key(&a.to_string(), "2026-03-01") } },
            "00000000-0000-0000-0000-000000000001": { "2025-12-31": {
                "row_count": 9, "archived_at": "2026-01-01T03:00:00+00:00", "r2_key": "kept" } }
        }
    });
    let bytes = serde_json::to_vec(&stale).unwrap();
    ctx.store.seed(MANIFEST_KEY, bytes, "application/json");
    ctx.store.fail_puts_containing(&b.to_string(), 1);

    let got = ctx.run().await.unwrap();
    assert_eq!(
        got,
        RunSummary {
            archived: 1,
            archived_rows: 2,
            deleted: 1,
            deleted_rows: 2,
            failed: 1,
            deferred: 0,
        }
    );
    assert_eq!(ctx.archived(a, "2026-03-01").len(), 3);
    let m = ctx.manifest();
    assert_eq!(
        m["archived_dates"][a.to_string()]["2026-03-01"]["row_count"],
        json!(2)
    );
    assert!(m["archived_dates"].get(b.to_string()).is_none());
    let kept = &m["archived_dates"]["00000000-0000-0000-0000-000000000001"]["2025-12-31"];
    assert_eq!(kept["r2_key"], json!("kept"));
    assert_eq!(ctx.rows(a).await, []);
    assert_eq!(ctx.rows(b).await.len(), 1);
    let warn = (
        LogLevel::Warn,
        "dtakologs archive: object store error (put)".to_owned(),
    );
    assert_eq!(ctx.logs.all(), [warn]);

    // 次の実行で残りを退避して消す
    let got = ctx.run().await.unwrap();
    assert_eq!((got.archived, got.deleted, got.failed), (1, 1, 0));
    assert_eq!(ctx.rows(b).await, []);
    ctx.finish().await;
}

/// 1 回の実行で退避するのは上限の日数まで。残りは次の実行 (退避していない日は消さない)。
#[tokio::test(flavor = "multi_thread")]
async fn run_caps_days_per_run() {
    let ctx = Ctx::start().await;
    let a = ctx.tenant("archive-cap").await;
    let days = MAX_DATES_PER_RUN + 2;
    for d in 0..days {
        let at = format!("2026-02-{:02}T09:00:00+09:00", d + 1);
        ctx.log_row(a, &at, 1).await;
    }
    let got = ctx.run().await.unwrap();
    assert_eq!(
        (got.archived, got.deleted),
        (MAX_DATES_PER_RUN, MAX_DATES_PER_RUN)
    );
    assert_eq!(got.deferred, 2);
    assert_eq!(ctx.rows(a).await.len(), 2);
    let got = ctx.run().await.unwrap();
    assert_eq!((got.archived, got.deleted, got.deferred), (2, 2, 0));
    assert_eq!(ctx.rows(a).await, []);
    ctx.finish().await;
}

/// 目録が読めない・壊れている・書けない、DB の一覧が引けないときは何も消さない (`None`)。
#[tokio::test(flavor = "multi_thread")]
async fn run_stops_without_deleting_when_manifest_or_list_fails() {
    let ctx = Ctx::start().await;
    let a = ctx.tenant("archive-stop").await;
    ctx.log_row(a, "2026-03-01T09:00:00+09:00", 1).await;

    // 壊れた目録は上書きしない
    ctx.store
        .seed(MANIFEST_KEY, b"not json".to_vec(), "application/json");
    assert_eq!(ctx.run().await, None);
    assert_eq!(ctx.store.total_put_calls(), 0);
    ctx.store.remove(MANIFEST_KEY);

    // 目録が読めない
    ctx.store.fail_gets_containing("_manifest", 1);
    assert_eq!(ctx.run().await, None);

    // 目録が書けない: 退避はしたが消さない
    ctx.store.fail_puts(MANIFEST_KEY, 1);
    assert_eq!(ctx.run().await, None);
    assert_eq!(ctx.rows(a).await.len(), 1);
    assert_eq!(
        ctx.logs.all(),
        [
            (
                LogLevel::Error,
                "dtakologs archive: manifest unreadable".to_owned()
            ),
            (
                LogLevel::Error,
                "dtakologs archive: object store error (get)".to_owned()
            ),
            (
                LogLevel::Error,
                "dtakologs archive: manifest object store error (put)".to_owned()
            ),
        ]
    );

    // DB の一覧が引けない
    let ctx = ctx
        .run_as_superuser("REVOKE ALL ON alc_api.tenants FROM alc_api_app, PUBLIC")
        .await;
    assert_eq!(ctx.run().await, None);
    let last = ctx.logs.all().pop().unwrap();
    assert_eq!(last.0, LogLevel::Error);
    assert!(last.1.starts_with("dtakologs archive: db ("), "{}", last.1);
    ctx.finish().await;
}

/// 日ごとの読み取り・削除の失敗はその日だけを飛ばす (読めなければ目録に載せず、消せなければ次の実行で消す)。
#[tokio::test(flavor = "multi_thread")]
async fn run_skips_days_whose_fetch_or_delete_fails() {
    let ctx = Ctx::start().await;
    let a = ctx.tenant("archive-skip").await;
    ctx.log_row(a, "2026-03-01T09:00:00+09:00", 1).await;
    ctx.log_row(a, "2026-03-02T09:00:00+09:00", 1).await;

    let ctx = ctx
        .run_as_superuser("ALTER TABLE alc_api.dtakologs RENAME COLUMN vehicle_cd TO vehicle_cd_x")
        .await;
    let got = ctx.run().await.unwrap();
    assert_eq!((got.archived, got.deleted, got.failed), (0, 0, 2));
    assert!(ctx.store.object(MANIFEST_KEY).is_none());

    let ctx = ctx
        .run_as_superuser(&format!(
            "ALTER TABLE alc_api.dtakologs RENAME COLUMN vehicle_cd_x TO vehicle_cd; {}",
            fail_on("DELETE")
        ))
        .await;
    let got = ctx.run().await.unwrap();
    assert_eq!((got.archived, got.deleted, got.failed), (2, 0, 2));
    assert_eq!(ctx.rows(a).await.len(), 2);
    let warns: Vec<_> = ctx.logs.all().into_iter().map(|(_, m)| m).collect();
    assert!(
        warns[0].starts_with("dtakologs archive: fetch db ("),
        "{warns:?}"
    );
    assert!(
        warns[3].starts_with("dtakologs archive: delete db ("),
        "{warns:?}"
    );
    ctx.finish().await;
}

/// 1 日ぶんの行は、件数ごとに分けて読んでも全部が時刻の順で返る。
#[tokio::test(flavor = "multi_thread")]
async fn archive_rows_reads_all_pages() {
    let ctx = Ctx::start().await;
    let a = ctx.tenant("archive-pages").await;
    for v in 1..=3 {
        let at = format!("2026-03-01T0{v}:00:00+09:00");
        ctx.log_row(a, &at, v).await;
    }
    let mut c = ctx.pg.inner.lock().await;
    for batch in [1, 2, 3, 10] {
        let rows = pg::archive_rows(&mut c, a, "2026-03-01".to_owned(), batch)
            .await
            .unwrap();
        let cds: Vec<_> = rows.0.iter().map(|r| r["vehicle_cd"].clone()).collect();
        assert_eq!(cds, [json!(1), json!(2), json!(3)], "batch {batch}");
    }
    drop(c);
    ctx.finish().await;
}

/// 書き戻し: 退避した 1 日ぶんを、tenant ヘッダーのテナントに書き戻す (別のテナントからは無い = 404)。
#[tokio::test(flavor = "multi_thread")]
async fn restore_writes_back_the_tenants_day() {
    let ctx = Ctx::start().await;
    let a = ctx.tenant("restore-a").await;
    let b = ctx.tenant("restore-b").await;
    ctx.log_row(a, "2026-03-01T08:00:00+09:00", 1).await;
    ctx.log_row(a, "2026-03-01T09:00:00+09:00", 2).await;
    assert_eq!(ctx.run().await.unwrap().deleted_rows, 2);
    assert_eq!(ctx.rows(a).await, []);

    assert_eq!(
        ctx.restore(b, "2026-03-01").await,
        (StatusCode::NOT_FOUND, json!({ "error": "not_found" }))
    );
    assert_eq!(
        ctx.restore(a, "2026-03-01").await,
        (StatusCode::OK, json!({ "restored": 2 }))
    );
    assert_eq!(
        ctx.rows(a).await,
        [
            ("2026-03-01T08:00:00+09:00".to_owned(), 1),
            ("2026-03-01T09:00:00+09:00".to_owned(), 2),
        ]
    );
    assert_eq!(ctx.rows(b).await, []);
    // 2 回目は同じ行に upsert する (増えない)
    assert_eq!(ctx.restore(a, "2026-03-01").await.0, StatusCode::OK);
    assert_eq!(ctx.rows(a).await.len(), 2);
    ctx.finish().await;
}

/// 書き戻しの 1 文に載せる行数を超える日も、全部の行を書き戻す。
#[tokio::test(flavor = "multi_thread")]
async fn restore_splits_large_days_into_batches() {
    let ctx = Ctx::start().await;
    let a = ctx.tenant("restore-batches").await;
    let n = logs_archive::RESTORE_BATCH + 3;
    let mut text = format!(
        "{}\n",
        json!({ "_archive_header": true, "tenant_id": a.to_string(), "date": "2026-03-01" })
    );
    for i in 0..n {
        let row = json!({
            "tenant_id": a.to_string(),
            "data_date_time": format!("2026-03-01T09:{:02}:{:02}+09:00", i / 60, i % 60),
            "vehicle_cd": 1,
        });
        text.push_str(&format!("{row}\n"));
    }
    let key = logs_archive::r2_key(&a.to_string(), "2026-03-01");
    ctx.store.seed(&key, gz(&text), "application/gzip");
    assert_eq!(
        ctx.restore(a, "2026-03-01").await,
        (StatusCode::OK, json!({ "restored": n }))
    );
    assert_eq!(ctx.rows(a).await.len(), n);
    ctx.finish().await;
}

/// 書き戻しの拒否: body のテナント・知らない欄 (400)、日付の形 (400)、gzip / JSON でない (422)、
/// 見出しか行のテナントが違う (422。1 行も書かない)、保存先・DB の失敗 (500。原因はログに段の名前と kind だけ)。
#[tokio::test(flavor = "multi_thread")]
async fn restore_rejects_bad_requests_and_foreign_rows() {
    let ctx = Ctx::start().await;
    let a = ctx.tenant("restore-reject-a").await;
    let b = ctx.tenant("restore-reject-b").await;
    let path = "/internal/archive/restore";
    let bad_body = (StatusCode::BAD_REQUEST, json!({ "error": "invalid_body" }));
    let body = json!({ "date": "2026-03-01", "tenant_id": b.to_string() });
    assert_eq!(ctx.call(a, path, body).await, bad_body);
    assert_eq!(ctx.call(a, path, json!({})).await, bad_body);
    let bad_date = (StatusCode::BAD_REQUEST, json!({ "error": "invalid_date" }));
    for date in ["2026-3-1", "2026-02-30", "../../x", "2026-03-01T00"] {
        assert_eq!(ctx.restore(a, date).await, bad_date, "{date}");
    }

    let key = logs_archive::r2_key(&a.to_string(), "2026-03-01");
    let invalid = (
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({ "error": "invalid_archive" }),
    );
    ctx.store
        .seed(&key, b"not gzip".to_vec(), "application/gzip");
    assert_eq!(ctx.restore(a, "2026-03-01").await, invalid);
    ctx.store.seed(&key, gz("{not json}\n"), "application/gzip");
    assert_eq!(ctx.restore(a, "2026-03-01").await, invalid);

    let mismatch = (
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({ "error": "tenant_mismatch" }),
    );
    let header = |t: Uuid| json!({ "_archive_header": true, "tenant_id": t.to_string() });
    let row = |t: Uuid, at: &str| json!({ "tenant_id": t.to_string(), "data_date_time": at, "vehicle_cd": 1 });
    let at = "2026-03-01T09:00:00+09:00";
    // 行の 1 つが別のテナント (先の行も書かない)
    let text = format!(
        "{}\n{}\n{}\n",
        header(a),
        row(a, at),
        row(b, "2026-03-01T10:00:00+09:00")
    );
    ctx.store.seed(&key, gz(&text), "application/gzip");
    assert_eq!(ctx.restore(a, "2026-03-01").await, mismatch);
    // 見出しが別のテナント
    let text = format!("{}\n{}\n", header(b), row(a, at));
    ctx.store.seed(&key, gz(&text), "application/gzip");
    assert_eq!(ctx.restore(a, "2026-03-01").await, mismatch);
    // tenant_id の無い行
    let text = format!(
        "{}\n{}\n",
        header(a),
        json!({ "data_date_time": at, "vehicle_cd": 1 })
    );
    ctx.store.seed(&key, gz(&text), "application/gzip");
    assert_eq!(ctx.restore(a, "2026-03-01").await, mismatch);
    assert_eq!(ctx.rows(a).await, []);
    assert_eq!(ctx.rows(b).await, []);
    assert!(ctx.logs.all().is_empty());

    // 保存先の失敗
    let internal = (
        StatusCode::INTERNAL_SERVER_ERROR,
        json!({ "error": "internal_error" }),
    );
    ctx.store.fail_gets_containing("dtakologs", 1);
    assert_eq!(ctx.restore(a, "2026-03-01").await, internal);
    let get_failed = (
        LogLevel::Error,
        "archive restore failed: object store error (get)".to_owned(),
    );
    assert_eq!(ctx.logs.all(), [get_failed]);

    // DB の失敗 (1 行も書かない)
    let text = format!("{}\n{}\n", header(a), row(a, at));
    ctx.store.seed(&key, gz(&text), "application/gzip");
    let ctx = ctx.run_as_superuser(&fail_on("INSERT")).await;
    assert_eq!(ctx.restore(a, "2026-03-01").await, internal);
    let last = ctx.logs.all().pop().unwrap();
    assert!(
        last.1.starts_with("archive restore failed: db ("),
        "{}",
        last.1
    );
    assert_eq!(ctx.rows(a).await, []);
    ctx.finish().await;
}
