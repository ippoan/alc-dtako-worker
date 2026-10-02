//! 分割の口 `POST /split-csv/{upload_id}` を、口から DB・保存先まで通して確かめる (native)。
//!
//! DB はテストの process の中で起こす組み込みの PostgreSQL (`embedded/mod.rs`。`sql_db.rs` と同じ)、
//! 保存先・待ち・ログは偽物 (`fakes/mod.rs`)。口は `tower::ServiceExt::oneshot` で叩き、`TenantId` の extension は
//! テスト側の layer で入れる (本番では直下の worker が `require_tenant_header` の layer で入れる)。
//!
//! zip はテストの中でコードで作る (実データを使わない。運行NO・名前は作り物)。テナント ID はテストごとの乱数。

// 共用の土台・偽物のうち、このファイルが使わないものが在る
#[allow(dead_code)]
mod embedded;
#[allow(dead_code)]
mod fakes;

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;

use alc_core_wasm::TenantId;
use alc_dtako_upload::routes::{tenant_router, DtakoState, SPLIT_UNKO_NOS_DISPLAY_LIMIT};
use alc_dtako_upload::split::{LogLevel, SplitError};
use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Extension;
use embedded::{kudgivt_flags, operation, tenant, upload, Embedded, Held, APP_ROLE};
use fakes::{FakeSleeper, FakeStore, Logs};
use futures_util::lock::Mutex;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;
use zip::write::SimpleFileOptions;
use zip::CompressionMethod::{Deflated, Stored};

const ZIP_KEY: &str = "zips/test-upload.zip";
const ZIP_TYPE: &str = "application/zip";

/// テスト 1 本ぶんの土台: DB (接続 1 本)・偽の保存先・偽の待ち・溜めるログ。
struct Ctx {
    db: Embedded,
    pg: Held<Arc<Mutex<PgClient>>>,
    store: Arc<FakeStore>,
    sleeper: Arc<FakeSleeper>,
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
            sleeper: Arc::new(FakeSleeper::default()),
            logs: Logs::default(),
        }
    }

    fn state(&self) -> DtakoState {
        state_of(
            self.pg.inner.clone(),
            &self.store,
            &self.sleeper,
            &self.logs,
        )
    }

    /// `tenant_id` として口を叩く → (status, 本文)。
    async fn post(&self, tenant_id: Uuid, path: &str) -> (StatusCode, String) {
        post(self.state(), tenant_id, path).await
    }

    async fn post_split(&self, tenant_id: Uuid, upload_id: Uuid) -> (StatusCode, String) {
        self.post(tenant_id, &format!("/split-csv/{upload_id}"))
            .await
    }

    /// テナントと、zip の key を持つ completed のアップロード 1 件を作り、zip を保存先に置く。
    async fn tenant_with_zip(&self, name: &str, zip: Vec<u8>) -> (Uuid, Uuid) {
        let mut c = self.pg.inner.lock().await;
        let t = tenant(&mut c, name).await;
        let up = upload(&mut c, t, "test.zip", "completed", Some(ZIP_KEY), 0.0).await;
        self.store.seed(ZIP_KEY, zip, ZIP_TYPE);
        (t, up)
    }

    async fn operation(&self, tenant_id: Uuid, unko_no: &str, crew_role: i32) {
        let mut c = self.pg.inner.lock().await;
        operation(&mut c, tenant_id, unko_no, crew_role, false).await;
    }

    async fn flags(&self, tenant_id: Uuid) -> Vec<(String, i32, bool)> {
        let mut c = self.pg.inner.lock().await;
        kudgivt_flags(&mut c, tenant_id).await
    }

    /// 保存先に在る object のうち、zip 以外 (= 分割が置いたもの)。
    fn written(&self) -> BTreeMap<String, (Vec<u8>, String)> {
        let mut objects = self.store.objects();
        objects.remove(ZIP_KEY);
        objects
    }

    async fn finish(self) {
        self.pg.close().await;
        self.db.shutdown();
    }
}

fn state_of(
    pg: Arc<Mutex<PgClient>>,
    store: &Arc<FakeStore>,
    sleeper: &Arc<FakeSleeper>,
    logs: &Logs,
) -> DtakoState {
    DtakoState {
        pg,
        store: store.clone(),
        sleeper: sleeper.clone(),
        log: logs.sink(),
    }
}

async fn post(state: DtakoState, tenant_id: Uuid, path: &str) -> (StatusCode, String) {
    let app = tenant_router()
        .with_state(state)
        .layer(Extension(TenantId(tenant_id)));
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

fn json_of(body: &str) -> Value {
    serde_json::from_str(body).unwrap()
}

type Entry<'a> = (&'a str, &'a [u8], zip::CompressionMethod);

fn zip_of(entries: &[Entry]) -> Vec<u8> {
    let mut buf = std::io::Cursor::new(Vec::new());
    {
        let mut zip = zip::ZipWriter::new(&mut buf);
        for (name, bytes, method) in entries {
            let opts = SimpleFileOptions::default().compression_method(*method);
            zip.start_file(*name, opts).unwrap();
            zip.write_all(bytes).unwrap();
        }
        zip.finish().unwrap();
    }
    buf.into_inner()
}

/// 同じエントリに、backend と共有の `split_csv_entry` を当てた結果 (key → (中身, content-type))。
fn expected_objects(tenant_id: Uuid, entries: &[Entry]) -> BTreeMap<String, (Vec<u8>, String)> {
    let mut out = BTreeMap::new();
    for (name, bytes, _) in entries {
        for f in alc_csv_parser::split_csv_entry(&tenant_id.to_string(), name, bytes) {
            out.insert(f.key, (f.content, "text/csv".to_owned()));
        }
    }
    out
}

/// 本文・ログに、key・運行NO・テナント ID・upload の id が出ていないこと。
fn assert_no_identifiers(ctx: &Ctx, bodies: &[&str], needles: &[String]) {
    let logs: Vec<String> = ctx.logs.all().into_iter().map(|(_, m)| m).collect();
    for text in bodies
        .iter()
        .copied()
        .chain(logs.iter().map(String::as_str))
    {
        for needle in needles.iter().map(String::as_str).chain([ZIP_KEY, "unko/"]) {
            assert!(!text.contains(needle), "{text:?} に {needle:?} が出ている");
        }
    }
}

const NOT_FOUND: &str = r#"{"error":"not_found"}"#;
const INTERNAL_ERROR: &str = r#"{"error":"internal_error"}"#;

/// 置かれる key と中身は `split_csv_entry` の結果と集合として一致し、KUDGIVT を置けた運行に印が付く。
#[tokio::test(flavor = "multi_thread")]
async fn split_writes_the_shared_split_output_and_marks_kudgivt() {
    let ctx = Ctx::start().await;
    let entries: &[Entry] = &[
        (
            "KUDGIVT.csv",
            b"unko,event\r\n5001,TEST-A\r\n5002,TEST-B\r\n5001,TEST-C\r\n",
            Deflated,
        ),
        (
            "KUDGURI.csv",
            b"unko,name\n5001,TEST-DRIVER-1\n5002,TEST-DRIVER-2\n",
            Stored,
        ),
        ("readme.txt", b"not a csv", Deflated),
        ("sub/sokudo.CSV", b"unko,speed\n5002,40\n", Deflated),
    ];
    let (t, up) = ctx
        .tenant_with_zip("Dtako Split Flow", zip_of(entries))
        .await;
    ctx.operation(t, "5001", 0).await;
    ctx.operation(t, "5001", 1).await;
    ctx.operation(t, "5002", 0).await;
    ctx.operation(t, "5003", 0).await;

    let (status, body) = ctx.post_split(t, up).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        json_of(&body),
        json!({
            "status": "ok",
            "upload_id": up.to_string(),
            "split_failed": 0,
            "split_unko_nos": ["5001", "5002"],
            "split_unko_nos_total": 2,
            "split_failed_unko_nos": [],
            "split_failed_unko_nos_total": 0,
        })
    );
    assert_eq!(json_of(&body).as_object().unwrap().len(), 7);

    // 置かれたもの = 同じ zip に split_csv_entry を当てた結果 (.csv 以外は飛ばす)。content-type は text/csv
    let written = ctx.written();
    assert_eq!(written, expected_objects(t, entries));
    assert_eq!(written.len(), 5);
    // 1 つは期待値をバイト列で直に書く (CRLF は LF になり、同じ運行NO の行がまとまる)
    assert_eq!(
        written.get(&format!("{t}/unko/5001/KUDGIVT.csv")),
        Some(&(
            b"unko,event\n5001,TEST-A\n5001,TEST-C\n".to_vec(),
            "text/csv".to_owned()
        ))
    );
    assert!(written.contains_key(&format!("{t}/unko/5002/SOKUDO.csv")));
    // zip はそのまま
    assert_eq!(
        ctx.store.object(ZIP_KEY).map(|(_, ty)| ty).as_deref(),
        Some(ZIP_TYPE)
    );

    assert_eq!(
        ctx.flags(t).await,
        [
            ("5001".to_owned(), 0, true),
            ("5001".to_owned(), 1, true),
            ("5002".to_owned(), 0, true),
            ("5003".to_owned(), 0, false),
        ]
    );
    assert_eq!(ctx.sleeper.slept(), Vec::<u64>::new());
    assert_eq!(ctx.logs.all(), []);
    ctx.finish().await;
}

/// 別テナントのアップロード・zip の key が NULL・存在しない id は 404 で、何も書かず、印も変わらない。
#[tokio::test(flavor = "multi_thread")]
async fn not_found_is_404_and_writes_nothing() {
    let ctx = Ctx::start().await;
    let zip = zip_of(&[("KUDGIVT.csv", b"unko,event\n6001,TEST-A\n", Deflated)]);
    let (a, a_up) = ctx.tenant_with_zip("Dtako 404 Tenant A", zip).await;
    ctx.operation(a, "6001", 0).await;
    let (b, no_key) = {
        let mut c = ctx.pg.inner.lock().await;
        let b = tenant(&mut c, "Dtako 404 Tenant B").await;
        operation(&mut c, b, "6001", 0, false).await;
        (
            b,
            upload(&mut c, a, "nokey.zip", "processing", None, 0.0).await,
        )
    };

    // 別テナントから、テナント A のアップロードの id で
    let (status, other_tenant) = ctx.post_split(b, a_up).await;
    assert_eq!(
        (status, other_tenant.as_str()),
        (StatusCode::NOT_FOUND, NOT_FOUND)
    );
    // zip の key が NULL
    let (status, null_key) = ctx.post_split(a, no_key).await;
    assert_eq!(
        (status, null_key.as_str()),
        (StatusCode::NOT_FOUND, NOT_FOUND)
    );
    // 存在しない id
    let missing = Uuid::new_v4();
    let (status, body) = ctx.post_split(a, missing).await;
    assert_eq!((status, body.as_str()), (StatusCode::NOT_FOUND, NOT_FOUND));

    assert_eq!(ctx.written(), BTreeMap::new());
    assert_eq!(ctx.store.total_put_calls(), 0);
    assert_eq!(ctx.flags(a).await, [("6001".to_owned(), 0, false)]);
    assert_eq!(ctx.flags(b).await, [("6001".to_owned(), 0, false)]);
    // 404 はログに出さない
    assert_eq!(ctx.logs.all(), []);
    let ids = [
        a.to_string(),
        b.to_string(),
        a_up.to_string(),
        "6001".to_owned(),
    ];
    assert_no_identifiers(&ctx, &[&other_tenant, &null_key, &body], &ids);
    ctx.finish().await;
}

/// `upload_id` が UUID でなければ axum の既定の 400 (分割の流れに入らない)。
#[tokio::test(flavor = "multi_thread")]
async fn non_uuid_upload_id_is_400() {
    let ctx = Ctx::start().await;
    let (status, _) = ctx.post(Uuid::new_v4(), "/split-csv/not-a-uuid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(ctx.logs.all(), []);
    ctx.finish().await;
}

/// zip が保存先に無い・保存先が読めない → 500。本文は固定の文、ログは段の名前だけ。
#[tokio::test(flavor = "multi_thread")]
async fn missing_or_unreadable_zip_is_500() {
    let ctx = Ctx::start().await;
    let (t, up) = {
        let mut c = ctx.pg.inner.lock().await;
        let t = tenant(&mut c, "Dtako Storage Tenant").await;
        let up = upload(&mut c, t, "test.zip", "completed", Some(ZIP_KEY), 0.0).await;
        (t, up)
    };
    // key は DB に在るが、保存先に object が無い
    let (status, missing) = ctx.post_split(t, up).await;
    assert_eq!(
        (status, missing.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR)
    );
    // 保存先の GET が失敗する
    ctx.store.seed(ZIP_KEY, zip_of(&[]), ZIP_TYPE);
    ctx.store.break_get();
    let (status, broken) = ctx.post_split(t, up).await;
    assert_eq!(
        (status, broken.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR)
    );

    let storage = (LogLevel::Error, "split-csv failed: storage".to_owned());
    assert_eq!(ctx.logs.all(), [storage.clone(), storage]);
    assert_eq!(ctx.store.total_put_calls(), 0);
    assert_no_identifiers(&ctx, &[&missing, &broken], &[t.to_string(), up.to_string()]);
    ctx.finish().await;
}

/// 壊れた zip (途中のエントリが展開できない・zip でない) → 500 で、保存先に 1 つも書かない。
#[tokio::test(flavor = "multi_thread")]
async fn broken_zip_is_500_and_writes_nothing() {
    let ctx = Ctx::start().await;
    // 1 つ目は正しい CSV、2 つ目 (無圧縮) の中身を 1 バイト書き換えて CRC を合わなくする、3 つ目も正しい CSV
    let mut zip = zip_of(&[
        ("KUDGIVT.csv", b"unko,event\n8001,TEST-A\n", Deflated),
        ("notes.txt", b"TEST-BROKEN-ENTRY", Stored),
        ("KUDGURI.csv", b"unko,name\n8001,TEST-DRIVER\n", Deflated),
    ]);
    let at = zip
        .windows(17)
        .position(|w| w == b"TEST-BROKEN-ENTRY")
        .unwrap();
    zip[at] = b'X';
    let (t, up) = ctx.tenant_with_zip("Dtako Broken Zip Tenant", zip).await;
    ctx.operation(t, "8001", 0).await;

    let (status, broken_entry) = ctx.post_split(t, up).await;
    assert_eq!(
        (status, broken_entry.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR)
    );
    assert_eq!(ctx.written(), BTreeMap::new());
    assert_eq!(ctx.store.total_put_calls(), 0);

    // zip として開けない
    ctx.store.seed(ZIP_KEY, b"not a zip".to_vec(), ZIP_TYPE);
    let (status, not_zip) = ctx.post_split(t, up).await;
    assert_eq!(
        (status, not_zip.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR)
    );
    assert_eq!(ctx.store.total_put_calls(), 0);

    let zip_error = (LogLevel::Error, "split-csv failed: zip".to_owned());
    assert_eq!(ctx.logs.all(), [zip_error.clone(), zip_error]);
    assert_eq!(ctx.flags(t).await, [("8001".to_owned(), 0, false)]);
    let ids = [t.to_string(), up.to_string(), "8001".to_owned()];
    assert_no_identifiers(&ctx, &[&broken_entry, &not_zip], &ids);
    ctx.finish().await;
}

/// CSV の無い zip (空・`.csv` 以外だけ) → 200 で、何も置かず、DB にも触らない。
#[tokio::test(flavor = "multi_thread")]
async fn zip_without_csv_is_ok_and_writes_nothing() {
    let ctx = Ctx::start().await;
    let zip = zip_of(&[("readme.txt", b"no csv here", Deflated)]);
    let (t, up) = ctx.tenant_with_zip("Dtako No Csv Tenant", zip).await;
    let (status, body) = ctx.post_split(t, up).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let v = json_of(&body);
    assert_eq!(
        (&v["split_failed"], &v["split_unko_nos_total"]),
        (&json!(0), &json!(0))
    );
    assert_eq!(
        (&v["split_unko_nos"], &v["split_failed_unko_nos"]),
        (&json!([]), &json!([]))
    );
    assert_eq!(ctx.written(), BTreeMap::new());
    assert_eq!(ctx.logs.all(), []);
    ctx.finish().await;
}

/// PUT が 2 回失敗して 3 回目に通る → 失敗 0 件。待ちは 300・800。
#[tokio::test(flavor = "multi_thread")]
async fn put_failing_twice_succeeds_on_the_third_attempt() {
    let ctx = Ctx::start().await;
    let entries: &[Entry] = &[(
        "KUDGIVT.csv",
        b"unko,event\n9001,TEST-A\n9002,TEST-B\n",
        Deflated,
    )];
    let (t, up) = ctx
        .tenant_with_zip("Dtako Retry Tenant", zip_of(entries))
        .await;
    ctx.operation(t, "9001", 0).await;
    ctx.operation(t, "9002", 0).await;
    let flaky = format!("{t}/unko/9001/KUDGIVT.csv");
    ctx.store.fail_puts(&flaky, 2);

    let (status, body) = ctx.post_split(t, up).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let v = json_of(&body);
    assert_eq!(v["split_failed"], json!(0));
    assert_eq!(v["split_unko_nos"], json!(["9001", "9002"]));
    assert_eq!(v["split_failed_unko_nos"], json!([]));
    assert_eq!(ctx.sleeper.slept(), [300, 800]);
    // 成功済みは再送しない
    let steady = format!("{t}/unko/9002/KUDGIVT.csv");
    assert_eq!(
        (ctx.store.put_calls(&flaky), ctx.store.put_calls(&steady)),
        (3, 1)
    );
    assert_eq!(ctx.written(), expected_objects(t, entries));
    let flags = ctx.flags(t).await;
    assert_eq!(
        flags,
        [("9001".to_owned(), 0, true), ("9002".to_owned(), 0, true)]
    );
    assert_eq!(ctx.logs.all(), []);
    ctx.finish().await;
}

/// PUT が 3 回とも失敗 → 失敗に数え、KUDGIVT を置けなかった運行には印を付けない (ほかには付ける)。
#[tokio::test(flavor = "multi_thread")]
async fn put_failing_three_times_is_counted_and_not_marked() {
    let ctx = Ctx::start().await;
    let entries: &[Entry] = &[
        (
            "KUDGIVT.csv",
            b"unko,event\n9101,TEST-A\n9102,TEST-B\n",
            Deflated,
        ),
        (
            "KUDGURI.csv",
            b"unko,name\n9101,TEST-DRIVER-1\n9102,TEST-DRIVER-2\n",
            Deflated,
        ),
    ];
    let (t, up) = ctx
        .tenant_with_zip("Dtako Failed Put Tenant", zip_of(entries))
        .await;
    ctx.operation(t, "9101", 0).await;
    ctx.operation(t, "9102", 0).await;
    // 9101 の KUDGIVT と、9102 の KUDGURI (KUDGIVT ではない) が置けない
    let failed_kudgivt = format!("{t}/unko/9101/KUDGIVT.csv");
    let failed_kudguri = format!("{t}/unko/9102/KUDGURI.csv");
    ctx.store.fail_puts(&failed_kudgivt, 9);
    ctx.store.fail_puts(&failed_kudguri, 9);

    let (status, body) = ctx.post_split(t, up).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        json_of(&body),
        json!({
            "status": "ok",
            "upload_id": up.to_string(),
            "split_failed": 2,
            "split_unko_nos": ["9102"],
            "split_unko_nos_total": 1,
            "split_failed_unko_nos": ["9101"],
            "split_failed_unko_nos_total": 1,
        })
    );
    // やり直しはエントリごと (エントリ 2 つがそれぞれ 300・800 を待つ)。3 回で止める
    assert_eq!(ctx.sleeper.slept(), [300, 800, 300, 800]);
    assert_eq!(ctx.store.put_calls(&failed_kudgivt), 3);
    assert_eq!(ctx.store.put_calls(&failed_kudguri), 3);
    let mut expected = expected_objects(t, entries);
    expected.remove(&failed_kudgivt);
    expected.remove(&failed_kudguri);
    assert_eq!(ctx.written(), expected);

    let flags = ctx.flags(t).await;
    assert_eq!(
        flags,
        [("9101".to_owned(), 0, false), ("9102".to_owned(), 0, true)]
    );
    // ログは件数だけ
    let warn = (LogLevel::Warn, "split: PUT failed for 2 files".to_owned());
    assert_eq!(ctx.logs.all(), [warn]);
    assert_no_identifiers(
        &ctx,
        &[],
        &[t.to_string(), up.to_string(), "9101".to_owned()],
    );
    ctx.finish().await;
}

/// 印が当たらない運行NO (DB に行が無い) が在っても 200 のまま。ログに件数が出る。
#[tokio::test(flavor = "multi_thread")]
async fn unmatched_unko_no_is_logged_as_a_count() {
    let ctx = Ctx::start().await;
    let entries: &[Entry] = &[(
        "KUDGIVT.csv",
        b"unko,event\n9201,TEST-A\n9202,TEST-B\n",
        Deflated,
    )];
    let (t, up) = ctx
        .tenant_with_zip("Dtako Unmatched Tenant", zip_of(entries))
        .await;
    ctx.operation(t, "9201", 0).await;

    let (status, body) = ctx.post_split(t, up).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let v = json_of(&body);
    assert_eq!(v["split_unko_nos"], json!(["9201", "9202"]));
    assert_eq!(v["split_failed"], json!(0));
    assert_eq!(ctx.flags(t).await, [("9201".to_owned(), 0, true)]);
    let warn = "split: has_kudgivt not applied: 1 unko_no(s)".to_owned();
    assert_eq!(ctx.logs.all(), [(LogLevel::Warn, warn)]);
    assert_no_identifiers(
        &ctx,
        &[],
        &[t.to_string(), up.to_string(), "9202".to_owned()],
    );
    ctx.finish().await;
}

/// 応答の運行NO の一覧は 500 件で切り、総数は `_total` に載る。
#[tokio::test(flavor = "multi_thread")]
async fn unko_no_list_is_capped_at_the_display_limit() {
    let ctx = Ctx::start().await;
    let mut csv = String::from("unko,event\n");
    for i in 0..=SPLIT_UNKO_NOS_DISPLAY_LIMIT {
        csv.push_str(&format!("T{i:04},TEST\n"));
    }
    let zip = zip_of(&[("KUDGIVT.csv", csv.as_bytes(), Deflated)]);
    let (t, up) = ctx.tenant_with_zip("Dtako Cap Tenant", zip).await;

    let (status, body) = ctx.post_split(t, up).await;
    assert_eq!(status, StatusCode::OK);
    let v = json_of(&body);
    let listed = v["split_unko_nos"].as_array().unwrap();
    assert_eq!(listed.len(), 500);
    assert_eq!(
        (&listed[0], &listed[499]),
        (&json!("T0000"), &json!("T0499"))
    );
    assert_eq!(v["split_unko_nos_total"], json!(501));
    assert_eq!(v["split_failed"], json!(0));
    assert_eq!(ctx.written().len(), 501);
    assert!(ctx.store.max_running() <= 6);
    // DB に運行の行は無いので、501 件とも当たらない
    let warn = "split: has_kudgivt not applied: 501 unko_no(s)".to_owned();
    assert_eq!(ctx.logs.all(), [(LogLevel::Warn, warn)]);
    ctx.finish().await;
}

/// DB の失敗 (切れた接続) → 500。ログは段の名前と kind。
#[tokio::test(flavor = "multi_thread")]
async fn db_failure_is_500_with_stage_and_kind_in_the_log() {
    let ctx = Ctx::start().await;
    let zip = zip_of(&[("KUDGIVT.csv", b"unko,event\n9301,TEST-A\n", Deflated)]);
    let (t, up) = ctx.tenant_with_zip("Dtako Db Failure Tenant", zip).await;
    let Ctx {
        db,
        pg,
        store,
        sleeper,
        logs,
    } = ctx;
    let pg = pg.sever().await;

    let state = state_of(pg.clone(), &store, &sleeper, &logs);
    let (status, body) = post(state, t, &format!("/split-csv/{up}")).await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR)
    );
    let error = "split-csv failed: db (closed)".to_owned();
    assert_eq!(logs.all(), [(LogLevel::Error, error)]);
    assert_eq!(store.total_put_calls(), 0);
    for needle in [t.to_string(), up.to_string()] {
        assert!(!body.contains(&needle) && !logs.all()[0].1.contains(&needle));
    }
    drop(pg);
    db.shutdown();
}

/// 印を付ける段の DB の失敗 (表の権限が無いロール) も 500。保存先は書き終えている。
#[tokio::test(flavor = "multi_thread")]
async fn db_failure_while_marking_is_500_after_the_puts() {
    let db = Embedded::start().await;
    let store = Arc::new(FakeStore::default());
    let (sleeper, logs) = (Arc::new(FakeSleeper::default()), Logs::default());
    // 準備は RLS が効くロールで
    let entries: &[Entry] = &[("KUDGIVT.csv", b"unko,event\n9401,TEST-A\n", Deflated)];
    let prep = db.shared(APP_ROLE).await;
    let (t, up) = {
        let mut c = prep.inner.lock().await;
        let t = tenant(&mut c, "Dtako Mark Failure Tenant").await;
        operation(&mut c, t, "9401", 0, false).await;
        (
            t,
            upload(&mut c, t, "test.zip", "completed", Some(ZIP_KEY), 0.0).await,
        )
    };
    prep.close().await;
    store.seed(ZIP_KEY, zip_of(entries), ZIP_TYPE);

    // 口は、履歴の表は読めるが運行の表を更新できないロールで繋ぐ
    let su = db.superuser().await;
    su.inner
        .batch_execute(
            "CREATE ROLE dtako_test_reader LOGIN; \
             GRANT USAGE ON SCHEMA alc_api TO dtako_test_reader; \
             GRANT SELECT ON alc_api.dtako_upload_history TO dtako_test_reader",
        )
        .await
        .unwrap();
    su.close().await;
    let pg = db.shared("dtako_test_reader").await;
    let state = state_of(pg.inner.clone(), &store, &sleeper, &logs);
    let (status, body) = post(state, t, &format!("/split-csv/{up}")).await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR)
    );
    // 42501 = insufficient_privilege (識別子を含まない SQLSTATE だけ)
    let error = "split-csv failed: db (42501)".to_owned();
    assert_eq!(logs.all(), [(LogLevel::Error, error)]);
    let mut written = store.objects();
    written.remove(ZIP_KEY);
    assert_eq!(written, expected_objects(t, entries));
    pg.close().await;
    db.shutdown();
}

/// `SplitError` の文は段の名前と kind だけ。
#[test]
fn split_error_display_is_stage_and_kind_only() {
    assert_eq!(SplitError::NotFound.to_string(), "not found");
    assert_eq!(SplitError::Db("57P01".to_owned()).to_string(), "db (57P01)");
    assert_eq!(SplitError::Storage.to_string(), "storage");
    assert_eq!(SplitError::Zip.to_string(), "zip");
    let source: &dyn std::error::Error = &SplitError::Zip;
    assert!(source.source().is_none());
}
