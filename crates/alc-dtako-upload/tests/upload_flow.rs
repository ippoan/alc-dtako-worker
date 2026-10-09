//! アップロードの口 `POST /upload` を、口から DB・保存先まで通して確かめる (native)。
//!
//! DB はテストの process の中で起こす組み込みの PostgreSQL (`embedded/mod.rs`)、保存先・待ち・ログは偽物 (`fakes/mod.rs`)。
//! 口は `tower::ServiceExt::oneshot` で叩く (multipart の body はここで手で組む。呼び手と同じ `file` field・filename 付き)。
//!
//! zip はテストの中でコードで作る ([`upload_zip`]。KUDGURI・KUDGIVT は Shift_JIS・CRLF)。運行NO・乗務員CD・名前・日時は作り物。
//! テナント ID はテストごとの乱数。

// 共用の土台・偽物のうち、このファイルが使わないものが在る
#[allow(dead_code)]
mod embedded;
#[allow(dead_code)]
mod fakes;

use std::io::Write;
use std::sync::Arc;

use alc_core_wasm::TenantId;
use alc_dtako_upload::ingest::IngestLimits;
use alc_dtako_upload::routes::{
    safe_download_filename, tenant_router, tenant_router_with, DtakoState,
};
use alc_dtako_upload::split::LogLevel;
use alc_dtako_upload::store::GET_CONCURRENCY;
use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::{middleware, Extension, Router};
use embedded::{exec, rows_json, tenant, upload, Embedded, Held, APP_ROLE};
use fakes::{
    declare_uncompressed_size, flag_data_descriptor, FakeClock, FakeSleeper, FakeStore, Logs,
};
use futures_util::lock::Mutex;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;
use zip::write::SimpleFileOptions;

const BOUNDARY: &str = "----test-boundary-7d1a";
const INTERNAL_ERROR: &str = r#"{"error":"internal_error"}"#;

/// KUDGURI の列 (必須 = 運行NO・読取日・事業所CD・事業所名・車輌CD・車輌名・対象乗務員CD (または 乗務員CD1)・
/// 対象乗務員名 (または 乗務員名１)・対象乗務員区分。ほかは省ける)。
const KUDGURI_HEADER: &str = "運行NO,読取日,運行日,事業所CD,事業所名,車輌CD,車輌名,対象乗務員CD,対象乗務員名,対象乗務員区分,出社日時,退社日時,総走行距離";
/// KUDGIVT の列 (必須 = 運行NO・読取日・対象乗務員CD (または 乗務員CD1)・乗務員名１・対象乗務員区分・開始日時・イベントCD・イベント名。
/// 区間時間 (分) は省けるが、分数と日別の計算が使う)。
const KUDGIVT_HEADER: &str =
    "運行NO,読取日,対象乗務員CD,乗務員名１,対象乗務員区分,開始日時,イベントCD,イベント名,区間時間";

/// KUDGURI の 1 行。`day` = 2026/03 の日、`dep`・`ret` = 時 (`hh:15:00`)。営業所と車輌は固定の作り物。
fn kudguri_line(
    unko_no: &str,
    crew_role: i32,
    driver_cd: &str,
    day: u32,
    dep: u32,
    ret: u32,
) -> String {
    format!(
        "{unko_no},2026/03/{:02},2026/03/{day:02},OF1,TEST-OFFICE,VH1,TEST-VEHICLE,{driver_cd},TEST-DRIVER {driver_cd},{crew_role},\
         2026/03/{day:02} {dep:02}:15:00,2026/03/{day:02} {ret:02}:15:00,120.5",
        day + 1
    )
}

/// KUDGIVT の 1 行 (201 運転 / 202 荷役 / 301 休憩 / 302 休息)。`start` = `hh:mm`。
fn kudgivt_line(
    unko_no: &str,
    crew_role: i32,
    driver_cd: &str,
    day: u32,
    start: &str,
    event_cd: &str,
    minutes: i32,
) -> String {
    format!(
        "{unko_no},2026/03/{:02},{driver_cd},TEST-DRIVER {driver_cd},{crew_role},2026/03/{day:02} {start}:00,{event_cd},TEST-EVENT,{minutes}",
        day + 1
    )
}

/// ヘッダーと行を CRLF でつなぎ、Shift_JIS にする (実物の CSV と同じ形)。
fn sjis_csv(header: &str, lines: &[String]) -> Vec<u8> {
    let mut text = format!("{header}\r\n");
    for line in lines {
        text.push_str(line);
        text.push_str("\r\n");
    }
    encoding_rs::SHIFT_JIS.encode(&text).0.into_owned()
}

fn zip_of(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut buf = std::io::Cursor::new(Vec::new());
    {
        let mut zip = zip::ZipWriter::new(&mut buf);
        for (name, bytes) in entries {
            zip.start_file(*name, SimpleFileOptions::default()).unwrap();
            zip.write_all(bytes).unwrap();
        }
        zip.finish().unwrap();
    }
    buf.into_inner()
}

/// **作り物の最小の zip**: `KUDGURI.csv` と `KUDGIVT.csv` の 2 エントリ (deflate)。
fn upload_zip(kudguri: &[String], kudgivt: &[String]) -> Vec<u8> {
    zip_of(&[
        ("KUDGURI.csv", sjis_csv(KUDGURI_HEADER, kudguri)),
        ("KUDGIVT.csv", sjis_csv(KUDGIVT_HEADER, kudgivt)),
    ])
}

/// multipart/form-data の body (field 1 つ)。`filename` が `None` なら filename を付けない。
fn multipart_body(field: &str, filename: Option<&str>, bytes: &[u8]) -> Vec<u8> {
    let filename = filename
        .map(|f| format!("; filename=\"{f}\""))
        .unwrap_or_default();
    let head = format!(
        "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{field}\"{filename}\r\nContent-Type: application/zip\r\n\r\n"
    );
    let mut body = head.into_bytes();
    body.extend_from_slice(bytes);
    body.extend_from_slice(format!("\r\n--{BOUNDARY}--\r\n").as_bytes());
    body
}

fn multipart_type() -> String {
    format!("multipart/form-data; boundary={BOUNDARY}")
}

/// 口を叩く → (status, 応答の `Server-Timing`, 本文)。
async fn call(app: Router, req: Request<Body>) -> (StatusCode, Option<String>, String) {
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let timing = res.headers().get("server-timing").cloned();
    let timing = timing.map(|v| v.to_str().unwrap().to_owned());
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await;
    let body = String::from_utf8(bytes.unwrap().to_vec()).unwrap();
    (status, timing, body)
}

fn upload_request(content_type: &str, body: Vec<u8>) -> Request<Body> {
    let req = Request::builder().method("POST").uri("/upload");
    let req = req.header("content-type", content_type);
    req.body(Body::from(body)).unwrap()
}

fn rerun_request(id: &str) -> Request<Body> {
    let req = Request::builder().method("POST");
    let req = req.uri(format!("/internal/rerun/{id}"));
    req.body(Body::empty()).unwrap()
}

async fn send(app: Router, content_type: &str, body: Vec<u8>) -> (StatusCode, String) {
    let (status, _, body) = call(app, upload_request(content_type, body)).await;
    (status, body)
}

async fn post_rerun(app: Router, id: &str) -> (StatusCode, String) {
    let (status, _, body) = call(app, rerun_request(id)).await;
    (status, body)
}

fn json_of(body: &str) -> Value {
    serde_json::from_str(body).unwrap()
}

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
        Self::on(db).await
    }

    async fn on(db: Embedded) -> Self {
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
        DtakoState {
            pg: self.pg.inner.clone(),
            store: self.store.clone(),
            sleeper: self.sleeper.clone(),
            clock: Arc::new(FakeClock::default()),
            log: self.logs.sink(),
        }
    }

    fn app(&self, tenant_id: Uuid) -> Router {
        tenant_router()
            .with_state(self.state())
            .layer(Extension(TenantId(tenant_id)))
    }

    /// 接続を閉じ切って superuser で `sql` を流し、口の接続を張り直す (同時に張れる接続は 1 本)。保存先・待ち・ログはそのまま。
    async fn run_as_superuser(self, sql: &str) -> Self {
        let Self {
            db,
            pg,
            store,
            sleeper,
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
            sleeper,
            logs,
        }
    }

    /// zip を `file` field で送る → (status, `Server-Timing`)。
    async fn upload_timing(&self, tenant_id: Uuid, zip: &[u8]) -> (StatusCode, Option<String>) {
        let body = multipart_body("file", Some("timed.zip"), zip);
        let req = upload_request(&multipart_type(), body);
        let (status, timing, _) = call(self.app(tenant_id), req).await;
        (status, timing)
    }

    /// やり直しの口を叩く → (status, `Server-Timing`)。
    async fn rerun_timing(&self, tenant_id: Uuid, id: &str) -> (StatusCode, Option<String>) {
        let (status, timing, _) = call(self.app(tenant_id), rerun_request(id)).await;
        (status, timing)
    }

    /// `tenant_id` として、やり直しの口を叩く → (status, 本文)。`id` は path にそのまま入れる。
    async fn rerun(&self, tenant_id: Uuid, id: &str) -> (StatusCode, String) {
        post_rerun(self.app(tenant_id), id).await
    }

    async fn tenant(&self, name: &str) -> Uuid {
        let mut c = self.pg.inner.lock().await;
        tenant(&mut c, name).await
    }

    /// `tenant_id` として、zip を `file` field (filename 付き) で送る → (status, 本文)。
    async fn upload(&self, tenant_id: Uuid, filename: &str, zip: &[u8]) -> (StatusCode, String) {
        let body = multipart_body("file", Some(filename), zip);
        send(self.app(tenant_id), &multipart_type(), body).await
    }

    /// 200 を確かめて、本文を JSON で返す。
    async fn upload_ok(&self, tenant_id: Uuid, filename: &str, zip: &[u8]) -> Value {
        let (status, body) = self.upload(tenant_id, filename, zip).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        json_of(&body)
    }

    async fn rows(&self, tenant_id: Uuid, query: &str) -> Vec<Value> {
        let mut c = self.pg.inner.lock().await;
        rows_json(&mut c, tenant_id, query).await
    }

    /// 履歴の `(filename, status, error_message, operations_count, r2_zip_key が在るか)` (作った順)。
    async fn history(&self, tenant_id: Uuid) -> Vec<Value> {
        let query = "SELECT filename, status, error_message, operations_count, r2_zip_key IS NOT NULL AS has_key \
                     FROM dtako_upload_history WHERE tenant_id = $1 ORDER BY created_at, filename";
        self.rows(tenant_id, query).await
    }

    /// 運行の `(unko_no, crew_role, 乗務員CD, 出発, has_kudgivt)` (運行NO・crew_role の順)。
    async fn operations(&self, tenant_id: Uuid) -> Vec<Value> {
        let query = "SELECT o.unko_no, o.crew_role, e.driver_cd, o.has_kudgivt, \
                     to_char(o.departure_at AT TIME ZONE 'UTC', 'MM-DD HH24:MI') AS departure \
                     FROM dtako_operations o LEFT JOIN employees e ON e.id = o.driver_id \
                     WHERE o.tenant_id = $1 ORDER BY o.unko_no, o.crew_role";
        self.rows(tenant_id, query).await
    }

    /// 変更記録の `(unko_no, crew_role, before, after)` (記録の順)。
    async fn changes(&self, tenant_id: Uuid) -> Vec<Value> {
        let query = "SELECT unko_no, crew_role, before, after FROM dtako_operation_changes \
                     WHERE tenant_id = $1 ORDER BY recorded_at, unko_no, crew_role";
        self.rows(tenant_id, query).await
    }

    async fn count(&self, tenant_id: Uuid, table: &str) -> i64 {
        let query = format!("SELECT COUNT(*) AS n FROM {table} WHERE tenant_id = $1");
        self.rows(tenant_id, &query).await[0]["n"].as_i64().unwrap()
    }

    /// 保存先の key の一覧 (テナント ID を `T` に置き換える)。
    fn keys(&self, tenant_id: Uuid) -> Vec<String> {
        let t = tenant_id.to_string();
        let keys = self.store.objects().into_keys();
        keys.map(|k| k.replace(&t, "T")).collect()
    }

    fn log_lines(&self) -> Vec<(LogLevel, String)> {
        self.logs.all()
    }

    /// 本文とログに、テナント ID・key の形・`needles` (運行NO・乗務員CD・filename・履歴の id など) が出ていないこと。
    fn assert_no_identifiers(&self, tenant_id: Uuid, bodies: &[&str], needles: &[&str]) {
        let logs: Vec<String> = self.logs.all().into_iter().map(|(_, m)| m).collect();
        let t = tenant_id.to_string();
        let fixed = [t.as_str(), "unko/", "uploads/", "TEST-"];
        for text in bodies
            .iter()
            .copied()
            .chain(logs.iter().map(String::as_str))
        {
            for needle in needles.iter().copied().chain(fixed) {
                assert!(!text.contains(needle), "{text:?} に {needle:?} が出ている");
            }
        }
    }

    async fn finish(self) {
        self.pg.close().await;
        self.db.shutdown();
    }
}

/// 2 運行 (1 つは 2 人乗務) の zip。U-1001 = 03/02 の 08:15〜17:15 (主 D-ONE・助手 D-TWO)、U-1002 = 03/04 の 09:15〜15:15 (D-ONE)。
/// `break_minutes` = U-1001 の主の休憩の長さ (上げ直しで変える値)。
fn sample_zip(break_minutes: i32) -> Vec<u8> {
    let kudguri = [
        kudguri_line("U-1001", 1, "D-ONE", 2, 8, 17),
        kudguri_line("U-1001", 2, "D-TWO", 2, 8, 17),
        kudguri_line("U-1002", 1, "D-ONE", 4, 9, 15),
    ];
    let kudgivt = [
        kudgivt_line("U-1001", 1, "D-ONE", 2, "08:15", "201", 240),
        kudgivt_line("U-1001", 1, "D-ONE", 2, "12:15", "301", break_minutes),
        kudgivt_line("U-1001", 1, "D-ONE", 2, "13:15", "202", 60),
        kudgivt_line("U-1001", 1, "D-ONE", 2, "14:15", "201", 180),
        kudgivt_line("U-1001", 2, "D-TWO", 2, "08:15", "201", 300),
        kudgivt_line("U-1001", 2, "D-TWO", 2, "13:15", "301", 45),
        kudgivt_line("U-1001", 2, "D-TWO", 2, "14:00", "201", 195),
        kudgivt_line("U-1002", 1, "D-ONE", 4, "09:15", "201", 360),
    ];
    upload_zip(&kudguri, &kudgivt)
}

/// 端から端 (初回): 応答・履歴・運行・日別・セグメント・保存先 (zip と分割の出力)・分割済みの印。
#[tokio::test(flavor = "multi_thread")]
async fn upload_imports_operations_daily_hours_and_splits() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Upload Tenant").await;
    let zip = sample_zip(60);

    let body = ctx.upload_ok(t, "sample.zip", &zip).await;
    let upload_id = body["upload_id"].as_str().unwrap().to_owned();
    assert_eq!(upload_id.parse::<Uuid>().unwrap().to_string(), upload_id);
    let want = json!({
        "upload_id": upload_id,
        "operations_count": 3,
        "status": "completed",
        "split_failed": 0,
        "split_unko_nos": ["U-1001", "U-1002"],
        "split_unko_nos_total": 2,
        "split_failed_unko_nos": [],
        "split_failed_unko_nos_total": 0,
    });
    assert_eq!(body, want);
    assert_eq!(body.as_object().unwrap().len(), 8);

    // 履歴: completed・行数 (運行NO の種類の数 2 ではなく、KUDGURI の行数 3)・zip の key
    let history = "SELECT filename, status, error_message, operations_count, r2_zip_key FROM dtako_upload_history WHERE tenant_id = $1";
    let zip_key = format!("{t}/uploads/{upload_id}/sample.zip");
    let completed = json!({
        "filename": "sample.zip", "status": "completed", "error_message": null,
        "operations_count": 3, "r2_zip_key": zip_key,
    });
    assert_eq!(ctx.rows(t, history).await, [completed]);

    // 運行: 2 人乗務は crew_role ごとに 1 行。分割で KUDGIVT を置けた運行に印が付く
    let op = |unko_no: &str, crew_role: i32, driver_cd: &str, departure: &str| json!({ "unko_no": unko_no, "crew_role": crew_role, "driver_cd": driver_cd, "has_kudgivt": true, "departure": departure });
    let want_operations = [
        op("U-1001", 1, "D-ONE", "03-02 08:15"),
        op("U-1001", 2, "D-TWO", "03-02 08:15"),
        op("U-1002", 1, "D-ONE", "03-04 09:15"),
    ];
    assert_eq!(ctx.operations(t).await, want_operations);
    let masters = "SELECT (SELECT office_name FROM dtako_offices WHERE tenant_id = $1) AS office, \
                   (SELECT vehicle_name FROM dtako_vehicles WHERE tenant_id = $1) AS vehicle, \
                   (SELECT COUNT(*) FROM employees WHERE tenant_id = $1) AS employees, \
                   (SELECT COUNT(*) FROM dtako_event_classifications WHERE tenant_id = $1) AS classifications";
    let want_masters = json!({ "office": "TEST-OFFICE", "vehicle": "TEST-VEHICLE", "employees": 2, "classifications": 3 });
    assert_eq!(ctx.rows(t, masters).await, [want_masters]);

    // 日別とセグメント (値は backend と共有の `compute_daily_hours` の出力。フェリーは無し)
    let days = "SELECT e.driver_cd, h.work_date, h.start_time, h.total_work_minutes, h.total_drive_minutes, h.total_rest_minutes, \
                h.drive_minutes, h.cargo_minutes, h.total_distance, h.operation_count, h.unko_nos \
                FROM dtako_daily_work_hours h JOIN employees e ON e.id = h.driver_id \
                WHERE h.tenant_id = $1 ORDER BY e.driver_cd, h.work_date, h.start_time";
    let two_crew_day = |driver_cd: &str| {
        json!({
            "driver_cd": driver_cd, "work_date": "2026-03-02", "start_time": "08:15:00", "total_work_minutes": 1080,
            "total_drive_minutes": 975, "total_rest_minutes": 0, "drive_minutes": 915, "cargo_minutes": 60,
            "total_distance": 120.5, "operation_count": 1, "unko_nos": ["U-1001"],
        })
    };
    let single_day = json!({
        "driver_cd": "D-ONE", "work_date": "2026-03-04", "start_time": "09:15:00", "total_work_minutes": 360,
        "total_drive_minutes": 360, "total_rest_minutes": 0, "drive_minutes": 360, "cargo_minutes": 0,
        "total_distance": 120.5, "operation_count": 1, "unko_nos": ["U-1002"],
    });
    let want_days = [two_crew_day("D-ONE"), single_day, two_crew_day("D-TWO")];
    assert_eq!(ctx.rows(t, days).await, want_days);
    let segments = "SELECT e.driver_cd, s.work_date, s.unko_no, s.segment_index, s.work_minutes, s.labor_minutes, s.drive_minutes, s.cargo_minutes, \
                    to_char(s.start_at AT TIME ZONE 'UTC', 'HH24:MI') AS start_at, to_char(s.end_at AT TIME ZONE 'UTC', 'HH24:MI') AS end_at \
                    FROM dtako_daily_work_segments s JOIN employees e ON e.id = s.driver_id \
                    WHERE s.tenant_id = $1 ORDER BY e.driver_cd, s.work_date, s.start_at";
    let two_crew_segment = |driver_cd: &str| {
        json!({
            "driver_cd": driver_cd, "work_date": "2026-03-02", "unko_no": "U-1001", "segment_index": 0, "work_minutes": 540,
            "labor_minutes": 975, "drive_minutes": 915, "cargo_minutes": 60, "start_at": "08:15", "end_at": "17:15",
        })
    };
    let single_segment = json!({
        "driver_cd": "D-ONE", "work_date": "2026-03-04", "unko_no": "U-1002", "segment_index": 0, "work_minutes": 360,
        "labor_minutes": 360, "drive_minutes": 360, "cargo_minutes": 0, "start_at": "09:15", "end_at": "15:15",
    });
    let want_segments = [
        two_crew_segment("D-ONE"),
        single_segment,
        two_crew_segment("D-TWO"),
    ];
    assert_eq!(ctx.rows(t, segments).await, want_segments);
    assert_eq!(ctx.changes(t).await, Vec::<Value>::new());

    // 保存先: zip はそのまま (application/zip)、分割の出力は運行NO ごとの CSV (text/csv。UTF-8・LF)
    let want_keys = [
        "T/unko/U-1001/KUDGIVT.csv".to_owned(),
        "T/unko/U-1001/KUDGURI.csv".to_owned(),
        "T/unko/U-1002/KUDGIVT.csv".to_owned(),
        "T/unko/U-1002/KUDGURI.csv".to_owned(),
        format!("T/uploads/{upload_id}/sample.zip"),
    ];
    assert_eq!(ctx.keys(t), want_keys);
    assert_eq!(
        ctx.store.object(&zip_key),
        Some((zip, "application/zip".to_owned()))
    );
    let split_kudgivt = format!(
        "{KUDGIVT_HEADER}\n{}\n",
        kudgivt_line("U-1002", 1, "D-ONE", 4, "09:15", "201", 360)
    );
    let want_split = Some((split_kudgivt.into_bytes(), "text/csv".to_owned()));
    assert_eq!(
        ctx.store.object(&format!("{t}/unko/U-1002/KUDGIVT.csv")),
        want_split
    );

    assert_eq!(ctx.sleeper.slept(), Vec::<u64>::new());
    assert_eq!(ctx.log_lines(), []);

    // 本文のキーの順は固定で、`upload_id` が先頭に来る (呼び手に、本文の先頭の決まった長さだけから `upload_id` を読むものが在る)。
    // 運行が多くて運行NO の一覧が長い応答でも変わらない
    let unko_nos: Vec<String> = (1..=30).map(|n| format!("U-LONG-{n:04}")).collect();
    let line = |unko_no: &String| kudguri_line(unko_no, 1, "D-ONE", 9, 8, 17);
    let kudguri: Vec<String> = unko_nos.iter().map(line).collect();
    let line = |unko_no: &String| kudgivt_line(unko_no, 1, "D-ONE", 9, "08:15", "201", 540);
    let kudgivt: Vec<String> = unko_nos.iter().map(line).collect();
    let (status, text) = ctx
        .upload(t, "many.zip", &upload_zip(&kudguri, &kudgivt))
        .await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let many_id = json_of(&text)["upload_id"].as_str().unwrap().to_owned();
    let listed: Vec<String> = unko_nos.iter().map(|u| format!("\"{u}\"")).collect();
    let want_text = format!(
        r#"{{"upload_id":"{many_id}","operations_count":30,"status":"completed","split_failed":0,"split_unko_nos":[{}],"split_unko_nos_total":30,"split_failed_unko_nos":[],"split_failed_unko_nos_total":0}}"#,
        listed.join(",")
    );
    assert_eq!(text, want_text);
    assert!(text.len() > 300 && text.starts_with(r#"{"upload_id":""#));

    // 応答の `Server-Timing` に、段ごとの所要が終えた順に載る (名前と数字だけ。偽の時計は読むたびに 7 進む)
    let stages = "history;dur=7, put_zip;dur=7, parse;dur=7, prepare;dur=7, old_kudgivt;dur=7, apply;dur=7, split;dur=7, daily;dur=7";
    let timed = ctx.upload_timing(t, &sample_zip(60)).await;
    assert_eq!(timed, (StatusCode::OK, Some(stages.to_owned())));
    ctx.finish().await;
}

/// 上げ直し: 前回の分数は、前回の分割が置いた旧 KUDGIVT から読む。同じ zip は記録なし、値が変われば before・after の分数が残る。
/// 旧 KUDGIVT が保存先に無い・読めないときは、before に印を残して取り込みは続ける。
#[tokio::test(flavor = "multi_thread")]
async fn reupload_reads_previous_minutes_from_the_split_kudgivt() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Reupload Flow Tenant").await;
    ctx.upload_ok(t, "first.zip", &sample_zip(60)).await;

    // 同じ zip をもう一度: 3 行とも「既に在る」→ 旧 KUDGIVT から前回の分数が読め、今回と同じなので記録なし
    let body = ctx.upload_ok(t, "same.zip", &sample_zip(60)).await;
    assert_eq!(body["operations_count"], 3);
    assert_eq!(ctx.changes(t).await, Vec::<Value>::new());
    assert_eq!(ctx.log_lines(), []);

    // 主の休憩が 60 → 75 に変わった zip: 記録は U-1001 の crew_role 1 の 1 件。before は旧 KUDGIVT の分数、after は今回の分数
    ctx.upload_ok(t, "changed.zip", &sample_zip(75)).await;
    let snapshot = |break_minutes: i32| {
        json!({
            "driver_cd": "D-ONE", "departure_at": "2026-03-02T08:15:00Z", "return_at": "2026-03-02T17:15:00Z",
            "drive_minutes": 420, "cargo_minutes": 60, "break_minutes": break_minutes, "rest_minutes": 0,
        })
    };
    let first_change = json!({ "unko_no": "U-1001", "crew_role": 1, "before": snapshot(60), "after": snapshot(75) });
    assert_eq!(ctx.changes(t).await, std::slice::from_ref(&first_change));
    assert_eq!(ctx.log_lines(), []);

    // 旧 KUDGIVT が保存先に無い運行 (U-1002) と、読めない運行 (U-1001 の GET が 1 回失敗): どちらも取り込みは通り、
    // 前回の分数は「取れなかった」になる (before に印。分数は比べない → 休憩を 75 → 90 に変えても、それだけでは記録しない)
    ctx.store.remove(&format!("{t}/unko/U-1002/KUDGIVT.csv"));
    ctx.store.fail_gets_containing("/unko/U-1001/", 1);
    let body = ctx.upload_ok(t, "unavailable.zip", &sample_zip(90)).await;
    assert_eq!(body["split_failed"], 0);
    assert_eq!(ctx.changes(t).await, std::slice::from_ref(&first_change));
    let warn = "upload: previous KUDGIVT unavailable for 3 row(s)".to_owned();
    assert_eq!(ctx.log_lines(), [(LogLevel::Warn, warn)]);

    // 出発が変わった行は、前回の分数が取れなくても記録される (before に `before_kudgivt` の印)
    ctx.store.remove(&format!("{t}/unko/U-1002/KUDGIVT.csv"));
    let kudguri = [kudguri_line("U-1002", 1, "D-ONE", 4, 10, 15)];
    let kudgivt = [kudgivt_line("U-1002", 1, "D-ONE", 4, "10:15", "201", 300)];
    ctx.upload_ok(t, "moved.zip", &upload_zip(&kudguri, &kudgivt))
        .await;
    let second_change = json!({
        "unko_no": "U-1002", "crew_role": 1,
        "before": {
            "driver_cd": "D-ONE", "departure_at": "2026-03-04T09:15:00Z", "return_at": "2026-03-04T15:15:00Z",
            "before_kudgivt": "unavailable",
        },
        "after": {
            "driver_cd": "D-ONE", "departure_at": "2026-03-04T10:15:00Z", "return_at": "2026-03-04T15:15:00Z",
            "drive_minutes": 300, "cargo_minutes": 0, "break_minutes": 0, "rest_minutes": 0,
        },
    });
    assert_eq!(ctx.changes(t).await, [first_change, second_change]);

    // 旧 KUDGIVT は在るが、その crew_role の行が無い → 前回の分数は 0 (取れなかった、ではない)
    let orphan = format!(
        "{KUDGIVT_HEADER}\n{}\n",
        kudgivt_line("U-1002", 2, "D-TWO", 4, "10:15", "201", 300)
    );
    ctx.store.seed(
        &format!("{t}/unko/U-1002/KUDGIVT.csv"),
        orphan.into_bytes(),
        "text/csv",
    );
    ctx.upload_ok(t, "zero.zip", &upload_zip(&kudguri, &kudgivt))
        .await;
    let changes = ctx.changes(t).await;
    assert_eq!(changes.len(), 3);
    assert_eq!(changes[2]["before"]["drive_minutes"], 0);
    assert_eq!(changes[2]["after"]["drive_minutes"], 300);

    let history = ctx.history(t).await;
    let statuses: Vec<&str> = history
        .iter()
        .map(|h| h["status"].as_str().unwrap())
        .collect();
    assert_eq!(statuses, ["completed"; 6]);
    ctx.assert_no_identifiers(t, &[], &["U-1001", "U-1002", "D-ONE", ".zip"]);
    ctx.finish().await;
}

/// 前回の KUDGIVT は、運行NO ごとに 1 回、まとめて同時に読む (同時は上限まで)。結果は運行NO に結び付き、
/// 1 本の読み込みの失敗は、その運行だけ「前回の分数なし」になる。
#[tokio::test(flavor = "multi_thread")]
async fn previous_kudgivt_is_read_concurrently_and_bound_to_its_operation() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Concurrent Read Tenant").await;
    // 8 運行。運行ごとに運転の分数が違う (base + 運行の番号) ので、結果の結び付けを取り違えると値が合わない
    let zip_of_minutes = |base: i32| {
        let unko_no = |n: i32| format!("U-90{n:02}");
        let kudguri: Vec<String> = (1..=8)
            .map(|n| kudguri_line(&unko_no(n), 1, "D-ONE", 2, 8, 17))
            .collect();
        let line = |n: i32| kudgivt_line(&unko_no(n), 1, "D-ONE", 2, "08:15", "201", base + n);
        let kudgivt: Vec<String> = (1..=8).map(line).collect();
        upload_zip(&kudguri, &kudgivt)
    };

    // 初回: 既に在る運行が無いので、前回の KUDGIVT は読まない (KUDGIVT の GET は、分割の後の日別の計算し直しの運行ごとの 1 回だけ)
    ctx.upload_ok(t, "first.zip", &zip_of_minutes(100)).await;
    let kudgivt_gets = ctx.store.get_calls_containing("/KUDGIVT.csv");
    assert_eq!(kudgivt_gets.len(), 8);
    assert!(kudgivt_gets.iter().all(|(_, n)| *n == 1));

    // 同じ zip の上げ直し: 8 運行ぶんを同時に読む (上限まで)。全部読めて値が同じなので、変更記録は付かない
    let body = ctx.upload_ok(t, "same.zip", &zip_of_minutes(100)).await;
    assert_eq!(body["operations_count"], 8);
    assert_eq!(ctx.store.max_get_running(), GET_CONCURRENCY);
    assert_eq!(ctx.changes(t).await, Vec::<Value>::new());
    assert_eq!(ctx.log_lines(), []);

    // 全運行の運転の分数を変えた上げ直しで、1 本 (U-9005) の読み込みだけ失敗させる:
    // ほかの 7 運行は、その運行の前回の分数 (100 + 番号) と今回の分数 (200 + 番号) で記録が付く。
    // U-9005 は前回の分数が取れないので、分数だけの違いでは記録しない
    ctx.store.fail_gets_containing("/unko/U-9005/", 1);
    ctx.upload_ok(t, "changed.zip", &zip_of_minutes(200)).await;
    let snapshot = |drive_minutes: i32| {
        json!({
            "driver_cd": "D-ONE", "departure_at": "2026-03-02T08:15:00Z", "return_at": "2026-03-02T17:15:00Z",
            "drive_minutes": drive_minutes, "cargo_minutes": 0, "break_minutes": 0, "rest_minutes": 0,
        })
    };
    let change = |n: i32| json!({ "unko_no": format!("U-90{n:02}"), "crew_role": 1, "before": snapshot(100 + n), "after": snapshot(200 + n) });
    let want_changes: Vec<Value> = [1, 2, 3, 4, 6, 7, 8].into_iter().map(change).collect();
    assert_eq!(ctx.changes(t).await, want_changes);
    let warn = "upload: previous KUDGIVT unavailable for 1 row(s)".to_owned();
    assert_eq!(ctx.log_lines(), [(LogLevel::Warn, warn)]);
    assert_eq!(ctx.store.max_get_running(), GET_CONCURRENCY);

    ctx.assert_no_identifiers(t, &[], &["U-90", "D-ONE", ".zip"]);
    ctx.finish().await;
}

/// リクエストの形の誤り: `file` field が無い・multipart でない・multipart として読めない・テナントが存在しない → 400 と固定の語
/// (履歴は作られない)。tenant ヘッダー無しは 401。body は 2MB を超えても通り、20MB を超えると読めない。
#[tokio::test(flavor = "multi_thread")]
async fn malformed_requests_are_400_with_fixed_labels() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Upload Request Tenant").await;
    let zip = sample_zip(60);
    let label = |label: &str| (StatusCode::BAD_REQUEST, format!(r#"{{"error":"{label}"}}"#));

    // `file` でない field だけ
    let other_field = multipart_body("other", Some("sample.zip"), &zip);
    let no_file = send(ctx.app(t), &multipart_type(), other_field).await;
    assert_eq!(no_file, label("no_file"));
    // multipart でない
    let not_multipart = send(ctx.app(t), "application/json", b"{}".to_vec()).await;
    assert_eq!(not_multipart, label("invalid_multipart"));
    // multipart の途中で切れている (field の中身の途中・field の頭の途中)
    let mut cut_in_field = multipart_body("file", Some("sample.zip"), &zip);
    cut_in_field.truncate(cut_in_field.len() - 40);
    let cut_in_field = send(ctx.app(t), &multipart_type(), cut_in_field).await;
    assert_eq!(cut_in_field, label("invalid_multipart"));
    let cut_in_head =
        format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"fi").into_bytes();
    let cut_in_head = send(ctx.app(t), &multipart_type(), cut_in_head).await;
    assert_eq!(cut_in_head, label("invalid_multipart"));
    // 20MB を超える body は読めない
    let too_big = multipart_body("file", Some("big.zip"), &vec![0u8; 21 * 1024 * 1024]);
    let too_big = send(ctx.app(t), &multipart_type(), too_big).await;
    assert_eq!(too_big, label("invalid_multipart"));
    // 段を 1 つも終えていない応答に `Server-Timing` は付かない
    let req = upload_request(&multipart_type(), multipart_body("other", None, &zip));
    let (_, timing, _) = call(ctx.app(t), req).await;
    assert_eq!(timing, None);
    // ここまで履歴は作られていない
    assert_eq!(ctx.history(t).await, Vec::<Value>::new());

    // 存在しないテナント: 履歴を作れない (ほかの失敗と区別して 400)
    let missing = Uuid::new_v4();
    let tenant_not_found = ctx.upload(missing, "sample.zip", &zip).await;
    assert_eq!(tenant_not_found, label("tenant_not_found"));
    assert_eq!(ctx.store.total_put_calls(), 0);

    // tenant ヘッダーの layer (直下の worker が掛けるもの) を通すと、ヘッダー無しは 401 で口に届かない
    let guarded = tenant_router()
        .layer(middleware::from_fn(alc_core_wasm::require_tenant_header))
        .with_state(ctx.state());
    let body = multipart_body("file", Some("sample.zip"), &zip);
    let (status, _) = send(guarded, &multipart_type(), body).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(ctx.store.total_put_calls(), 0);

    // 2MB を超える body は通る (口まで届き、zip でないので入力の誤り)。filename が無ければ `upload.zip`
    let body = multipart_body("file", None, &vec![b'x'; 3 * 1024 * 1024]);
    let not_zip = send(ctx.app(t), &multipart_type(), body).await;
    assert_eq!(not_zip, label("invalid_zip"));
    let stored = "SELECT status, error_message, r2_zip_key LIKE '%/upload.zip' AS default_name FROM dtako_upload_history WHERE tenant_id = $1";
    let failed =
        json!({ "status": "failed", "error_message": "invalid_zip", "default_name": true });
    assert_eq!(ctx.rows(t, stored).await, [failed]);

    assert_eq!(ctx.log_lines(), []);
    let bodies = [
        &no_file.1,
        &not_multipart.1,
        &cut_in_field.1,
        &cut_in_head.1,
        &tenant_not_found.1,
        &not_zip.1,
    ];
    let bodies: Vec<&str> = bodies.iter().map(|b| b.as_str()).collect();
    ctx.assert_no_identifiers(t, &bodies, &[&missing.to_string(), "sample.zip", "U-1001"]);
    ctx.finish().await;
}

/// zip の中身の誤り → 400 と固定の語。履歴は failed で、同じ語が残る (zip は保存先に置かれ、key も記録される)。
/// KUDGURI が 0 行なら、KUDGIVT が無くても 0 件で completed。
#[tokio::test(flavor = "multi_thread")]
async fn invalid_zip_contents_are_400_and_mark_the_history_failed() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Upload Content Tenant").await;
    let kudguri = sjis_csv(
        KUDGURI_HEADER,
        &[kudguri_line("U-2001", 1, "D-ONE", 2, 8, 17)],
    );
    let kudgivt = sjis_csv(
        KUDGIVT_HEADER,
        &[kudgivt_line("U-2001", 1, "D-ONE", 2, "08:15", "201", 540)],
    );
    let label = |label: &str| (StatusCode::BAD_REQUEST, format!(r#"{{"error":"{label}"}}"#));
    let mut bodies: Vec<String> = Vec::new();
    let mut check = |name: &str, got: (StatusCode, String), want: &str| {
        assert_eq!(got, label(want), "{name}");
        bodies.push(got.1);
    };

    // zip として開けない
    check(
        "not-zip",
        ctx.upload(t, "1-not-zip.zip", b"not a zip").await,
        "invalid_zip",
    );
    // 書かれた非圧縮サイズより中身が大きい (書かれた大きさまでしか読まない)
    let mut understated = zip_of(&[
        ("KUDGURI.csv", kudguri.clone()),
        ("KUDGIVT.csv", kudgivt.clone()),
    ]);
    declare_uncompressed_size(&mut understated, 10);
    check(
        "understated",
        ctx.upload(t, "2-understated.zip", &understated).await,
        "invalid_zip",
    );
    // 書かれた非圧縮サイズの合計が上限 (既定 64MB) を超える (展開しない)
    let mut overstated = zip_of(&[
        ("KUDGURI.csv", kudguri.clone()),
        ("KUDGIVT.csv", kudgivt.clone()),
    ]);
    declare_uncompressed_size(&mut overstated, 65 * 1024 * 1024);
    check(
        "overstated",
        ctx.upload(t, "3-overstated.zip", &overstated).await,
        "zip_too_large",
    );
    // KUDGURI が無い / 必須の列が無い / 読取日が日付でない
    let only_kudgivt = zip_of(&[("KUDGIVT.csv", kudgivt.clone())]);
    check(
        "no-kudguri",
        ctx.upload(t, "4-no-kudguri.zip", &only_kudgivt).await,
        "kudguri_not_found",
    );
    let no_columns = zip_of(&[
        ("KUDGURI.csv", b"a,b\r\n1,2\r\n".to_vec()),
        ("KUDGIVT.csv", kudgivt.clone()),
    ]);
    check(
        "bad-kudguri",
        ctx.upload(t, "5-bad-kudguri.zip", &no_columns).await,
        "kudguri_invalid",
    );
    // KUDGURI に行が在るのに KUDGIVT が無い / KUDGIVT の必須の列が無い
    let only_kudguri = zip_of(&[("KUDGURI.csv", kudguri.clone())]);
    check(
        "no-kudgivt",
        ctx.upload(t, "6-no-kudgivt.zip", &only_kudguri).await,
        "kudgivt_not_found",
    );
    let bad_kudgivt = zip_of(&[
        ("KUDGURI.csv", kudguri.clone()),
        ("KUDGIVT.csv", b"a,b\r\n1,2\r\n".to_vec()),
    ]);
    check(
        "bad-kudgivt",
        ctx.upload(t, "7-bad-kudgivt.zip", &bad_kudgivt).await,
        "kudgivt_invalid",
    );

    // 上限はテストから差し込める: 上限 100 バイトでは、普通の zip も大きすぎる
    let limits = IngestLimits {
        max_uncompressed_bytes: 100,
        ..IngestLimits::default()
    };
    assert_ne!(limits, IngestLimits::default());
    let small = tenant_router_with(limits)
        .with_state(ctx.state())
        .layer(Extension(TenantId(t)));
    let good = zip_of(&[
        ("KUDGURI.csv", kudguri.clone()),
        ("KUDGIVT.csv", kudgivt.clone()),
    ]);
    let body = multipart_body("file", Some("8-limited.zip"), &good);
    check(
        "limited",
        send(small, &multipart_type(), body).await,
        "zip_too_large",
    );

    // どれも運行・日別は入らず、履歴は failed (同じ語)。zip は保存先に置かれ、key も記録されている
    let failed = |filename: &str, label: &str| json!({ "filename": filename, "status": "failed", "error_message": label, "operations_count": 0, "has_key": true });
    let want_history = [
        failed("1-not-zip.zip", "invalid_zip"),
        failed("2-understated.zip", "invalid_zip"),
        failed("3-overstated.zip", "zip_too_large"),
        failed("4-no-kudguri.zip", "kudguri_not_found"),
        failed("5-bad-kudguri.zip", "kudguri_invalid"),
        failed("6-no-kudgivt.zip", "kudgivt_not_found"),
        failed("7-bad-kudgivt.zip", "kudgivt_invalid"),
        failed("8-limited.zip", "zip_too_large"),
    ];
    assert_eq!(ctx.history(t).await, want_history);
    assert_eq!(ctx.count(t, "dtako_operations").await, 0);
    assert_eq!(ctx.count(t, "dtako_daily_work_hours").await, 0);
    assert_eq!(ctx.store.objects().len(), 8);
    assert_eq!(ctx.log_lines(), []);
    let texts: Vec<&str> = bodies.iter().map(String::as_str).collect();
    ctx.assert_no_identifiers(t, &texts, &["U-2001", "D-ONE", ".zip", "運行NO", "missing"]);

    // KUDGURI が 0 行 (ヘッダーだけ): KUDGIVT が無くても 0 件で completed になり、分割も走る
    let empty = zip_of(&[("KUDGURI.csv", sjis_csv(KUDGURI_HEADER, &[]))]);
    let body = ctx.upload_ok(t, "9-empty.zip", &empty).await;
    assert_eq!(
        (&body["operations_count"], &body["status"]),
        (&json!(0), &json!("completed"))
    );
    assert_eq!(
        (&body["split_failed"], &body["split_unko_nos_total"]),
        (&json!(0), &json!(0))
    );
    let completed = json!({ "filename": "9-empty.zip", "status": "completed", "error_message": null, "operations_count": 0, "has_key": true });
    assert_eq!(ctx.history(t).await[8], completed);

    // 大きさを後ろに書く形の zip (合計が分からない → エントリごとの値を足して確かめる) も通る
    let mut streamed = zip_of(&[("KUDGURI.csv", kudguri), ("KUDGIVT.csv", kudgivt)]);
    flag_data_descriptor(&mut streamed);
    let body = ctx.upload_ok(t, "10-streamed.zip", &streamed).await;
    assert_eq!(body["operations_count"], 1);
    assert_eq!(body["split_unko_nos"], json!(["U-2001"]));
    assert_eq!(ctx.count(t, "dtako_daily_work_hours").await, 1);
    ctx.finish().await;
}

/// 保存先と DB の失敗は 500 (本文は固定の語、原因はログに段の名前と kind だけ)。履歴には段の名前が残る。
#[tokio::test(flavor = "multi_thread")]
async fn storage_and_db_failures_are_500() {
    let db = Embedded::start().await;
    // 運行の INSERT だけが落ちるようにする (準備の段は通り、取り込みの本体の途中で DB が失敗する)
    let su = db.superuser().await;
    let reject = "ALTER TABLE alc_api.dtako_operations ADD CONSTRAINT test_reject CHECK (unko_no <> 'U-REJECT')";
    su.inner.batch_execute(reject).await.unwrap();
    su.close().await;
    let ctx = Ctx::on(db).await;
    let t = ctx.tenant("Dtako Upload Failure Tenant").await;
    let internal = (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR.to_owned());

    // zip を保存先に置けない → 500。履歴は failed (key は記録されない)。DB には何も入らない
    ctx.store.fail_puts_containing("/uploads/", 1);
    let put_failed = ctx.upload(t, "1-put.zip", &sample_zip(60)).await;
    assert_eq!(put_failed, internal);
    let history = |filename: &str, status: &str, label: Option<&str>, has_key: bool| json!({ "filename": filename, "status": status, "error_message": label, "operations_count": 0, "has_key": has_key });
    assert_eq!(
        ctx.history(t).await,
        [history("1-put.zip", "failed", Some("storage"), false)]
    );
    assert_eq!(ctx.count(t, "employees").await, 0);
    assert_eq!(ctx.store.objects().len(), 0);

    // 取り込みの本体の途中で DB が失敗 → 500。運行も日別も入らず (1 つめの運行の入れ替えも戻る)、準備の行は残る
    let kudguri = [
        kudguri_line("U-3001", 1, "D-ONE", 2, 8, 17),
        kudguri_line("U-REJECT", 1, "D-TWO", 2, 8, 17),
    ];
    let kudgivt = [kudgivt_line("U-3001", 1, "D-ONE", 2, "08:15", "201", 540)];
    let db_failed = ctx
        .upload(t, "2-db.zip", &upload_zip(&kudguri, &kudgivt))
        .await;
    assert_eq!(db_failed, internal);
    let want_history = [
        history("1-put.zip", "failed", Some("storage"), false),
        history("2-db.zip", "failed", Some("db"), true),
    ];
    assert_eq!(ctx.history(t).await, want_history);
    assert_eq!(ctx.count(t, "dtako_operations").await, 0);
    assert_eq!(ctx.count(t, "dtako_daily_work_hours").await, 0);
    assert_eq!(ctx.count(t, "dtako_daily_work_segments").await, 0);
    assert_eq!(ctx.count(t, "employees").await, 2);
    assert_eq!(ctx.count(t, "dtako_offices").await, 1);
    // 分割は走っていない (保存先に在るのは zip だけ)
    assert_eq!(ctx.store.objects().len(), 1);
    assert_eq!(ctx.sleeper.slept(), Vec::<u64>::new());

    // 23514 = check_violation (識別子を含まない SQLSTATE だけ)
    let want_logs = [
        (LogLevel::Error, "upload failed: storage".to_owned()),
        (LogLevel::Error, "upload failed: db (23514)".to_owned()),
    ];
    assert_eq!(ctx.log_lines(), want_logs);
    let ids = ["U-3001", "U-REJECT", "D-ONE", ".zip", "test_reject"];
    ctx.assert_no_identifiers(t, &[&put_failed.1, &db_failed.1], &ids);

    // 切れた接続: 履歴を作る段で DB が失敗 → 500 (保存先には触れない)
    let Ctx {
        db,
        pg,
        store,
        sleeper,
        logs,
    } = ctx;
    let pg = pg.sever().await;
    let state = DtakoState {
        pg,
        store: store.clone(),
        sleeper,
        clock: Arc::new(FakeClock::default()),
        log: logs.sink(),
    };
    let app = tenant_router()
        .with_state(state)
        .layer(Extension(TenantId(t)));
    let body = multipart_body("file", Some("3-closed.zip"), &sample_zip(60));
    let closed = send(app, &multipart_type(), body).await;
    assert_eq!(closed, internal);
    assert_eq!(store.objects().len(), 1);
    let last = logs.all().pop().unwrap();
    assert_eq!(
        last,
        (LogLevel::Error, "upload failed: db (closed)".to_owned())
    );
    db.shutdown();
}

/// 分割が丸ごと失敗したら、待って全体をやり直す (最大 3 回・待ち 300ms と 800ms)。尽きても取り込みは 200 のまま。
#[tokio::test(flavor = "multi_thread")]
async fn split_is_retried_as_a_whole_and_does_not_fail_the_upload() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Upload Split Retry Tenant").await;

    // zip の読み直しが 2 回失敗 → 3 回目で通る
    ctx.store.fail_gets_containing("/uploads/", 2);
    let body = ctx.upload_ok(t, "retry.zip", &sample_zip(60)).await;
    assert_eq!(body["split_failed"], 0);
    assert_eq!(body["split_unko_nos"], json!(["U-1001", "U-1002"]));
    assert_eq!(ctx.sleeper.slept(), [300, 800]);
    let warn = |attempt: u32| {
        (
            LogLevel::Warn,
            format!("upload: split failed ({attempt}/3): storage"),
        )
    };
    assert_eq!(ctx.log_lines(), [warn(1), warn(2)]);
    let flags = |ctx_operations: Vec<Value>| -> Vec<bool> {
        let flag = |o: &Value| o["has_kudgivt"].as_bool().unwrap();
        ctx_operations.iter().map(flag).collect()
    };
    assert_eq!(flags(ctx.operations(t).await), [true, true, true]);

    // 3 回とも失敗 → 200 のまま `split_failed = 1` (運行NO の一覧は空)。運行と日別は入っていて、分割済みの印は付かない。
    // 3 回目の後は待たない
    let kudguri = [kudguri_line("U-4001", 1, "D-ONE", 6, 8, 17)];
    let kudgivt = [kudgivt_line("U-4001", 1, "D-ONE", 6, "08:15", "201", 540)];
    ctx.store.fail_gets_containing("/uploads/", 3);
    let body = ctx
        .upload_ok(t, "exhausted.zip", &upload_zip(&kudguri, &kudgivt))
        .await;
    let upload_id = body["upload_id"].as_str().unwrap().to_owned();
    let want = json!({
        "upload_id": upload_id,
        "operations_count": 1,
        "status": "completed",
        "split_failed": 1,
        "split_unko_nos": [],
        "split_unko_nos_total": 0,
        "split_failed_unko_nos": [],
        "split_failed_unko_nos_total": 0,
    });
    assert_eq!(body, want);
    assert_eq!(ctx.sleeper.slept(), [300, 800, 300, 800]);
    // 分割が尽きたので、日別の計算し直しはしない (取り込み時の日別が残る。Refs ippoan/alc-dtako-worker#23)
    let skipped = (
        LogLevel::Warn,
        "upload: daily recalc skipped: split failed".to_owned(),
    );
    assert_eq!(
        ctx.log_lines(),
        [warn(1), warn(2), warn(1), warn(2), warn(3), skipped]
    );
    assert_eq!(flags(ctx.operations(t).await), [true, true, true, false]);
    assert_eq!(ctx.count(t, "dtako_daily_work_hours").await, 4);
    let completed = json!({ "filename": "exhausted.zip", "status": "completed", "error_message": null, "operations_count": 1, "has_key": true });
    assert_eq!(ctx.history(t).await[1], completed);
    assert!(!ctx.keys(t).iter().any(|k| k.contains("U-4001")));

    // 後から分割の口を叩けば復旧する
    let app = tenant_router()
        .with_state(ctx.state())
        .layer(Extension(TenantId(t)));
    let req = Request::builder()
        .method("POST")
        .uri(format!("/split-csv/{upload_id}"));
    let res = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(flags(ctx.operations(t).await), [true, true, true, true]);

    ctx.assert_no_identifiers(t, &[], &["U-1001", "U-4001", "D-ONE", ".zip", &upload_id]);
    ctx.finish().await;
}

const REJECT: &str =
    "ALTER TABLE alc_api.dtako_operations ADD CONSTRAINT test_reject CHECK (unko_no <> 'U-REJECT')";
/// 既に行が在る表に足す形 (`NOT VALID` = 在る行は検査せず、これからの INSERT だけ落とす)。
const REJECT_NEW: &str = "ALTER TABLE alc_api.dtako_operations ADD CONSTRAINT test_reject CHECK (unko_no <> 'U-REJECT') NOT VALID";
const ACCEPT: &str = "ALTER TABLE alc_api.dtako_operations DROP CONSTRAINT test_reject";

/// 復旧: 取り込みの途中で失敗した履歴 (failed) を、やり直しの口で取り込み直す。zip は保存先のものを読み、置き直さない。
#[tokio::test(flavor = "multi_thread")]
async fn rerun_recovers_a_failed_upload_from_the_stored_zip() {
    let ctx = Ctx::start().await.run_as_superuser(REJECT).await;
    let t = ctx.tenant("Dtako Rerun Tenant").await;
    let kudguri = [
        kudguri_line("U-5001", 1, "D-ONE", 2, 8, 17),
        kudguri_line("U-REJECT", 1, "D-TWO", 2, 8, 17),
    ];
    let kudgivt = [
        kudgivt_line("U-5001", 1, "D-ONE", 2, "08:15", "201", 540),
        kudgivt_line("U-REJECT", 1, "D-TWO", 2, "08:15", "201", 480),
    ];
    let zip = upload_zip(&kudguri, &kudgivt);

    // アップロードが取り込みの本体の途中で失敗 → 履歴は failed。zip は保存先に在り、key も記録されている
    let (status, _) = ctx.upload(t, "broken.zip", &zip).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let failed = json!({ "filename": "broken.zip", "status": "failed", "error_message": "db", "operations_count": 0, "has_key": true });
    assert_eq!(ctx.history(t).await, [failed]);
    let ids = "SELECT id, r2_zip_key FROM dtako_upload_history WHERE tenant_id = $1";
    let stored = ctx.rows(t, ids).await.remove(0);
    let upload_id = stored["id"].as_str().unwrap().to_owned();
    let zip_key = stored["r2_zip_key"].as_str().unwrap().to_owned();
    assert_eq!(ctx.count(t, "dtako_operations").await, 0);

    // 落ちる原因を取り除いて、やり直す → 200。応答はアップロードの口と同じ形で、`upload_id` は path の id
    let ctx = ctx.run_as_superuser(ACCEPT).await;
    let (status, body) = ctx.rerun(t, &upload_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let want = json!({
        "upload_id": upload_id,
        "operations_count": 2,
        "status": "completed",
        "split_failed": 0,
        "split_unko_nos": ["U-5001", "U-REJECT"],
        "split_unko_nos_total": 2,
        "split_failed_unko_nos": [],
        "split_failed_unko_nos_total": 0,
    });
    assert_eq!(json_of(&body), want);
    // 履歴は同じ 1 行のまま completed と行数になる (新しい履歴は作らない。前の失敗の語は残る — 成功の印は語を消さない)
    let completed = json!({ "filename": "broken.zip", "status": "completed", "error_message": "db", "operations_count": 2, "has_key": true });
    assert_eq!(ctx.history(t).await, std::slice::from_ref(&completed));
    assert_eq!(ctx.rows(t, ids).await, [stored]);
    let op = |unko_no: &str, driver_cd: &str| json!({ "unko_no": unko_no, "crew_role": 1, "driver_cd": driver_cd, "has_kudgivt": true, "departure": "03-02 08:15" });
    assert_eq!(
        ctx.operations(t).await,
        [op("U-5001", "D-ONE"), op("U-REJECT", "D-TWO")]
    );
    assert_eq!(ctx.count(t, "dtako_daily_work_hours").await, 2);
    assert_eq!(ctx.count(t, "dtako_daily_work_segments").await, 2);
    let want_keys = [
        "T/unko/U-5001/KUDGIVT.csv".to_owned(),
        "T/unko/U-5001/KUDGURI.csv".to_owned(),
        "T/unko/U-REJECT/KUDGIVT.csv".to_owned(),
        "T/unko/U-REJECT/KUDGURI.csv".to_owned(),
        format!("T/uploads/{upload_id}/broken.zip"),
    ];
    assert_eq!(ctx.keys(t), want_keys);

    // もう一度やり直す → 200。同じ中身なので変更記録は増えない。zip は置き直されていない (PUT は最初のアップロードの 1 回だけ)
    let (status, again) = ctx.rerun(t, &upload_id).await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(json_of(&again), want);
    // 本文のキーの順は、アップロードの口と同じ (文字列として比べる)
    let want_text = format!(
        r#"{{"upload_id":"{upload_id}","operations_count":2,"status":"completed","split_failed":0,"split_unko_nos":["U-5001","U-REJECT"],"split_unko_nos_total":2,"split_failed_unko_nos":[],"split_failed_unko_nos_total":0}}"#
    );
    assert_eq!(again, want_text);
    assert_eq!(ctx.changes(t).await, Vec::<Value>::new());
    assert_eq!(ctx.history(t).await, [completed]);
    // やり直しの `Server-Timing` は、頭の 2 段が違う (失敗した応答には、終えた段までが載る)
    let stages = "zip_key;dur=7, get_zip;dur=7, parse;dur=7, prepare;dur=7, old_kudgivt;dur=7, apply;dur=7, split;dur=7, daily;dur=7";
    let timed = ctx.rerun_timing(t, &upload_id).await;
    assert_eq!(timed, (StatusCode::OK, Some(stages.to_owned())));
    let ctx = ctx.run_as_superuser(REJECT_NEW).await;
    let timed = ctx.rerun_timing(t, &upload_id).await;
    let until_apply = "zip_key;dur=7, get_zip;dur=7, parse;dur=7, prepare;dur=7, old_kudgivt;dur=7";
    let failed = (
        StatusCode::INTERNAL_SERVER_ERROR,
        Some(until_apply.to_owned()),
    );
    assert_eq!(timed, failed);
    let not_found = ctx.rerun_timing(t, &Uuid::new_v4().to_string()).await;
    assert_eq!(not_found, (StatusCode::NOT_FOUND, None));
    assert_eq!(ctx.store.put_calls(&zip_key), 1);
    assert_eq!(
        ctx.store.object(&zip_key),
        Some((zip, "application/zip".to_owned()))
    );

    // ログは、最初のアップロードの失敗と、落ちるようにし直した後のやり直しの失敗の 2 行だけ
    let want_logs = [
        (LogLevel::Error, "upload failed: db (23514)".to_owned()),
        (LogLevel::Error, "rerun failed: db (23514)".to_owned()),
    ];
    assert_eq!(ctx.log_lines(), want_logs);
    ctx.finish().await;
}

/// やり直せるのは、ヘッダーのテナントの、zip の key が在る履歴だけ (ほかは 404)。zip が保存先に無ければ 500、壊れていれば 400 で、
/// どちらも履歴に失敗の印が付く。
#[tokio::test(flavor = "multi_thread")]
async fn rerun_of_a_missing_or_unreadable_upload_fails_without_identifiers() {
    let ctx = Ctx::start().await;
    let a = ctx.tenant("Dtako Rerun Tenant A").await;
    let b = ctx.tenant("Dtako Rerun Tenant B").await;
    let (b_up, no_key, missing_zip, broken_zip) = {
        let mut c = ctx.pg.inner.lock().await;
        let b_up = upload(
            &mut c,
            b,
            "1-other.zip",
            "failed",
            Some("zips/other.zip"),
            0.0,
        )
        .await;
        let no_key = upload(&mut c, a, "2-nokey.zip", "processing", None, 0.0).await;
        let missing = upload(
            &mut c,
            a,
            "3-missing.zip",
            "processing",
            Some("zips/missing.zip"),
            0.0,
        )
        .await;
        let broken = upload(
            &mut c,
            a,
            "4-broken.zip",
            "processing",
            Some("zips/broken.zip"),
            0.0,
        )
        .await;
        (b_up, no_key, missing, broken)
    };
    ctx.store
        .seed("zips/other.zip", sample_zip(60), "application/zip");
    ctx.store
        .seed("zips/broken.zip", b"not a zip".to_vec(), "application/zip");
    let not_found = (StatusCode::NOT_FOUND, r#"{"error":"not_found"}"#.to_owned());

    // 存在しない id / 別テナントの履歴の id (保存先に zip が在っても) / zip の key が NULL の履歴
    let random = Uuid::new_v4();
    let unknown = ctx.rerun(a, &random.to_string()).await;
    assert_eq!(unknown, not_found);
    let other_tenant = ctx.rerun(a, &b_up.to_string()).await;
    assert_eq!(other_tenant, not_found);
    let null_key = ctx.rerun(a, &no_key.to_string()).await;
    assert_eq!(null_key, not_found);
    let untouched = |filename: &str, status: &str, has_key: bool| json!({ "filename": filename, "status": status, "error_message": null, "operations_count": 0, "has_key": has_key });
    assert_eq!(
        ctx.history(b).await,
        [untouched("1-other.zip", "failed", true)]
    );
    assert_eq!(ctx.count(a, "dtako_operations").await, 0);
    assert_eq!(ctx.count(b, "dtako_operations").await, 0);
    assert_eq!(ctx.log_lines(), []);

    // zip が保存先に無い → 500。履歴は failed (段の名前)
    let internal = (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR.to_owned());
    let no_zip = ctx.rerun(a, &missing_zip.to_string()).await;
    assert_eq!(no_zip, internal);
    // zip を読めない (GET の失敗) も同じ
    ctx.store.fail_gets_containing("zips/broken.zip", 1);
    let get_failed = ctx.rerun(a, &broken_zip.to_string()).await;
    assert_eq!(get_failed, internal);
    // 壊れた zip が置いてある → 400 と、アップロードの口と同じ語。履歴は failed (同じ語)
    let invalid = ctx.rerun(a, &broken_zip.to_string()).await;
    assert_eq!(
        invalid,
        (
            StatusCode::BAD_REQUEST,
            r#"{"error":"invalid_zip"}"#.to_owned()
        )
    );
    let failed = |filename: &str, label: &str| json!({ "filename": filename, "status": "failed", "error_message": label, "operations_count": 0, "has_key": true });
    let want_history = [
        untouched("2-nokey.zip", "processing", false),
        failed("3-missing.zip", "storage"),
        failed("4-broken.zip", "invalid_zip"),
    ];
    assert_eq!(ctx.history(a).await, want_history);
    let storage = (LogLevel::Error, "rerun failed: storage".to_owned());
    assert_eq!(ctx.log_lines(), [storage.clone(), storage]);
    assert_eq!(ctx.store.total_put_calls(), 0);

    // UUID でない id は 400 (口に届かない)。tenant ヘッダーの layer を通すと、ヘッダー無しは 401
    let (status, _) = ctx.rerun(a, "not-a-uuid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let guarded = tenant_router()
        .layer(middleware::from_fn(alc_core_wasm::require_tenant_header))
        .with_state(ctx.state());
    let (status, _) = post_rerun(guarded, &missing_zip.to_string()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let bodies = [
        &unknown.1,
        &other_tenant.1,
        &null_key.1,
        &no_zip.1,
        &get_failed.1,
        &invalid.1,
    ];
    let bodies: Vec<&str> = bodies.iter().map(|b| b.as_str()).collect();
    let ids = [random, b_up, no_key, missing_zip, broken_zip].map(|id| id.to_string());
    let mut needles: Vec<&str> = ids.iter().map(String::as_str).collect();
    needles.extend(["zips/", ".zip"]);
    ctx.assert_no_identifiers(a, &bodies, &needles);

    // 切れた接続: key を引く段で DB が失敗 → 500 (保存先には触れない)
    let Ctx {
        db,
        pg,
        store,
        sleeper,
        logs,
    } = ctx;
    let pg = pg.sever().await;
    let state = DtakoState {
        pg,
        store,
        sleeper,
        clock: Arc::new(FakeClock::default()),
        log: logs.sink(),
    };
    let app = tenant_router()
        .with_state(state)
        .layer(Extension(TenantId(a)));
    let closed = post_rerun(app, &missing_zip.to_string()).await;
    assert_eq!(closed, internal);
    let last = logs.all().pop().unwrap();
    assert_eq!(
        last,
        (LogLevel::Error, "rerun failed: db (closed)".to_owned())
    );
    db.shutdown();
}

// ---- 履歴の読み取り口 ----

/// 本文を bytes のまま返す形 (zip のダウンロード用) → (status, ヘッダー, 本文)。
async fn call_raw(app: Router, method: &str, path: &str) -> (StatusCode, HeaderMap, Vec<u8>) {
    let req = Request::builder().method(method).uri(path);
    let res = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    let (status, headers) = (res.status(), res.headers().clone());
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await;
    (status, headers, bytes.unwrap().to_vec())
}

/// GET → (status, 本文の文字列)。
async fn get_text(app: Router, path: &str) -> (StatusCode, String) {
    let (status, _, body) = call_raw(app, "GET", path).await;
    (status, String::from_utf8(body).unwrap())
}

/// ダウンロードの filename に残るのは、ASCII の英数字と `.`・`-`・`_` だけ。空になったら `download.zip`。
#[test]
fn download_filename_keeps_only_ascii_alphanumerics_dot_dash_underscore() {
    assert_eq!(safe_download_filename("csvdata.zip"), "csvdata.zip");
    assert_eq!(safe_download_filename("A-z_0.9.ZIP"), "A-z_0.9.ZIP");
    // 記号・空白・引用符・path の区切り・改行は落ちる
    let noisy = "my \"data\" (1)/..\\x;y=z\r\n.zip";
    assert_eq!(safe_download_filename(noisy), "mydata1..xyz.zip");
    // 非 ASCII (全角の英数字を含む) は落ちる
    assert_eq!(safe_download_filename("運行データ１２.zip"), ".zip");
    assert_eq!(safe_download_filename("運行データ"), "download.zip");
    assert_eq!(safe_download_filename(""), "download.zip");
    assert_eq!(safe_download_filename(" \"/ "), "download.zip");
}

/// 一覧 2 口の本文: キーの順と日時の書式を、文字列で固定する。テナントごとで、DB の失敗は 500。
#[tokio::test(flavor = "multi_thread")]
async fn upload_lists_return_fixed_key_order_and_datetime_formats() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako List Route Tenant").await;
    let other = ctx.tenant("Dtako List Route Other").await;
    let (one, two, three) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    // created_at は 3 通り: 小数が 6 桁・マイクロ秒の末尾が 0・小数が 0
    let insert = format!(
        "INSERT INTO dtako_upload_history (id, tenant_id, filename, status, error_message, r2_zip_key, created_at) VALUES \
         ('{one}', $1, 'one.zip', 'completed', NULL, 'k/one.zip', TIMESTAMPTZ '2026-03-02 01:02:03.123456+00'), \
         ('{two}', $1, 'two.zip', 'failed', 'invalid_zip', NULL, TIMESTAMPTZ '2026-03-02 01:02:02.120000+00'), \
         ('{three}', $1, 'three.zip', 'pending_retry', NULL, 'k/three.zip', TIMESTAMPTZ '2026-03-02 01:02:01+00')"
    );
    {
        let mut c = ctx.pg.inner.lock().await;
        assert_eq!(exec(&mut c, t, &insert).await, 3);
    }

    let uploads = get_text(ctx.app(t), "/uploads").await;
    let want = format!(
        r#"[{{"created_at":"2026-03-02T01:02:03.123456Z","error":null,"filename":"one.zip","id":"{one}","r2_zip_key":"k/one.zip","status":"completed"}},{{"created_at":"2026-03-02T01:02:02.120Z","error":"invalid_zip","filename":"two.zip","id":"{two}","r2_zip_key":null,"status":"failed"}},{{"created_at":"2026-03-02T01:02:01Z","error":null,"filename":"three.zip","id":"{three}","r2_zip_key":"k/three.zip","status":"pending_retry"}}]"#
    );
    assert_eq!(uploads, (StatusCode::OK, want));

    let pending = get_text(ctx.app(t), "/internal/pending").await;
    let want = format!(
        r#"[{{"created_at":"2026-03-02T01:02:02.120+00:00","error_message":"invalid_zip","filename":"two.zip","id":"{two}","status":"failed","tenant_id":"{t}"}},{{"created_at":"2026-03-02T01:02:01+00:00","error_message":null,"filename":"three.zip","id":"{three}","status":"pending_retry","tenant_id":"{t}"}}]"#
    );
    assert_eq!(pending, (StatusCode::OK, want));

    // 別のテナントのヘッダーでは空の配列
    let empty = (StatusCode::OK, "[]".to_owned());
    assert_eq!(get_text(ctx.app(other), "/uploads").await, empty);
    assert_eq!(get_text(ctx.app(other), "/internal/pending").await, empty);
    // 読み取りだけ (GET 以外は 405)。tenant ヘッダーの layer を通すと、ヘッダー無しは 401
    for path in ["/uploads", "/internal/pending"] {
        let (status, _, _) = call_raw(ctx.app(t), "POST", path).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        let guarded = tenant_router()
            .layer(middleware::from_fn(alc_core_wasm::require_tenant_header))
            .with_state(ctx.state());
        let (status, _, _) = call_raw(guarded, "GET", path).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
    assert_eq!(ctx.log_lines(), []);

    // 切れた接続: 500 と固定の語 (原因はログに段の名前と kind だけ)
    let Ctx {
        db,
        pg,
        store,
        sleeper,
        logs,
    } = ctx;
    let pg = pg.sever().await;
    let state = DtakoState {
        pg,
        store,
        sleeper,
        clock: Arc::new(FakeClock::default()),
        log: logs.sink(),
    };
    let app = tenant_router()
        .with_state(state)
        .layer(Extension(TenantId(t)));
    let internal = (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR.to_owned());
    assert_eq!(get_text(app.clone(), "/uploads").await, internal);
    assert_eq!(get_text(app.clone(), "/internal/pending").await, internal);
    assert_eq!(
        get_text(app, &format!("/internal/download/{one}")).await,
        internal
    );
    let want_logs = [
        (LogLevel::Error, "uploads failed: db (closed)".to_owned()),
        (LogLevel::Error, "pending failed: db (closed)".to_owned()),
        (LogLevel::Error, "download failed: db (closed)".to_owned()),
    ];
    assert_eq!(logs.all(), want_logs);
    db.shutdown();
}

/// ダウンロードの口: 保存先の zip をそのまま返す。行が無い・別テナント・key が無いは 404、保存先に無い・読めないは 500。
#[tokio::test(flavor = "multi_thread")]
async fn download_returns_the_stored_zip_with_a_safe_filename() {
    let ctx = Ctx::start().await;
    let a = ctx.tenant("Dtako Download Tenant A").await;
    let b = ctx.tenant("Dtako Download Tenant B").await;
    let zip = sample_zip(60);
    let (plain, noisy, non_ascii, no_key, missing) = {
        let mut c = ctx.pg.inner.lock().await;
        let plain = upload(
            &mut c,
            a,
            "csvdata.zip",
            "completed",
            Some("zips/plain.zip"),
            0.0,
        )
        .await;
        let noisy = upload(
            &mut c,
            a,
            "my \"data\" (1)/x;y.zip",
            "completed",
            Some("zips/noisy.zip"),
            0.0,
        )
        .await;
        let non_ascii = upload(
            &mut c,
            a,
            "運行データ",
            "completed",
            Some("zips/kana.zip"),
            0.0,
        )
        .await;
        let no_key = upload(&mut c, a, "nokey.zip", "processing", None, 0.0).await;
        let missing = upload(
            &mut c,
            a,
            "missing.zip",
            "completed",
            Some("zips/missing.zip"),
            0.0,
        )
        .await;
        (plain, noisy, non_ascii, no_key, missing)
    };
    for key in ["zips/plain.zip", "zips/noisy.zip", "zips/kana.zip"] {
        ctx.store.seed(key, zip.clone(), "application/zip");
    }
    let path = |id: Uuid| format!("/internal/download/{id}");
    let header =
        |headers: &HeaderMap, name: &str| headers.get(name).unwrap().to_str().unwrap().to_owned();

    // 200: 本文は保存先の bytes と同じ。Content-Type と Content-Disposition
    let (status, headers, body) = call_raw(ctx.app(a), "GET", &path(plain)).await;
    assert_eq!((status, body == zip), (StatusCode::OK, true));
    assert_eq!(header(&headers, "content-type"), "application/zip");
    assert_eq!(
        header(&headers, "content-disposition"),
        r#"attachment; filename="csvdata.zip""#
    );
    // 記号・空白・引用符を含む filename → 英数と . - _ だけが残る / 全部非 ASCII → download.zip
    let (status, headers, _) = call_raw(ctx.app(a), "GET", &path(noisy)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        header(&headers, "content-disposition"),
        r#"attachment; filename="mydata1xy.zip""#
    );
    let (status, headers, _) = call_raw(ctx.app(a), "GET", &path(non_ascii)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        header(&headers, "content-disposition"),
        r#"attachment; filename="download.zip""#
    );

    // 404: 行が無い / 別のテナントのヘッダーで同じ id / zip の key が入っていない
    let not_found = (StatusCode::NOT_FOUND, r#"{"error":"not_found"}"#.to_owned());
    let random = Uuid::new_v4();
    let unknown = get_text(ctx.app(a), &path(random)).await;
    assert_eq!(unknown, not_found);
    let other_tenant = get_text(ctx.app(b), &path(plain)).await;
    assert_eq!(other_tenant, not_found);
    let null_key = get_text(ctx.app(a), &path(no_key)).await;
    assert_eq!(null_key, not_found);
    assert_eq!(ctx.log_lines(), []);

    // 500: 保存先に無い / 保存先の失敗
    let internal = (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR.to_owned());
    let no_object = get_text(ctx.app(a), &path(missing)).await;
    assert_eq!(no_object, internal);
    ctx.store.fail_gets_containing("zips/plain.zip", 1);
    let get_failed = get_text(ctx.app(a), &path(plain)).await;
    assert_eq!(get_failed, internal);
    let storage = (LogLevel::Error, "download failed: storage".to_owned());
    assert_eq!(ctx.log_lines(), [storage.clone(), storage]);

    // 読み取りだけ: POST / PUT / DELETE は 405。UUID でない id は 400。tenant ヘッダー無しは 401。保存先に書いていない
    for method in ["POST", "PUT", "DELETE"] {
        let (status, _, _) = call_raw(ctx.app(a), method, &path(plain)).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{method}");
    }
    let (status, _, _) = call_raw(ctx.app(a), "GET", "/internal/download/not-a-uuid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let guarded = tenant_router()
        .layer(middleware::from_fn(alc_core_wasm::require_tenant_header))
        .with_state(ctx.state());
    let (status, _, _) = call_raw(guarded, "GET", &path(plain)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(ctx.store.total_put_calls(), 0);

    // エラーの本文とログに、id・filename・key が出ない
    let bodies = [
        &unknown.1,
        &other_tenant.1,
        &null_key.1,
        &no_object.1,
        &get_failed.1,
    ];
    let bodies: Vec<&str> = bodies.iter().map(|b| b.as_str()).collect();
    let ids = [random, plain, no_key, missing].map(|id| id.to_string());
    let mut needles: Vec<&str> = ids.iter().map(String::as_str).collect();
    needles.extend(["zips/", ".zip", "csvdata"]);
    ctx.assert_no_identifiers(a, &bodies, &needles);
    ctx.finish().await;
}

// ---- 月の全員の再計算 ----

/// `text/event-stream` の本文を event の列にする (空行で割り、`data:` の行を JSON に)。
fn sse_events(body: &str) -> Vec<Value> {
    let mut events = Vec::new();
    for message in body.split("\n\n").filter(|m| !m.is_empty()) {
        let data = message.strip_prefix("data: ").unwrap();
        events.push(serde_json::from_str(data).unwrap());
    }
    events
}

/// `POST /recalculate?{query}` → (status, event の列)。
async fn recalc(app: Router, query: &str) -> (StatusCode, Vec<Value>, String) {
    let req = Request::builder()
        .method("POST")
        .uri(format!("/recalculate?{query}"));
    let (status, _, body) = call(app, req.body(Body::empty()).unwrap()).await;
    let events = if status == StatusCode::OK {
        sse_events(&body)
    } else {
        Vec::new()
    };
    (status, events, body)
}

/// 2 人乗務の運行 (休息つき) と 1 人の運行の zip。2026-03 の運行。
fn recalc_zip() -> Vec<u8> {
    let kudguri = [
        kudguri_line("U-6001", 1, "D-ONE", 2, 6, 23),
        kudguri_line("U-6001", 2, "D-TWO", 2, 6, 23),
        kudguri_line("U-6002", 1, "D-ONE", 5, 9, 15),
    ];
    let kudgivt = [
        kudgivt_line("U-6001", 1, "D-ONE", 2, "06:15", "201", 240),
        kudgivt_line("U-6001", 1, "D-ONE", 2, "10:15", "302", 300),
        kudgivt_line("U-6001", 1, "D-ONE", 2, "15:15", "201", 420),
        kudgivt_line("U-6001", 2, "D-TWO", 2, "06:15", "302", 360),
        kudgivt_line("U-6001", 2, "D-TWO", 2, "12:15", "201", 600),
        kudgivt_line("U-6002", 1, "D-ONE", 5, "09:15", "201", 360),
    ];
    upload_zip(&kudguri, &kudgivt)
}

const DAYS_QUERY: &str = "SELECT e.driver_cd, h.work_date, h.start_time, h.total_work_minutes, h.total_drive_minutes, h.total_rest_minutes, \
     h.late_night_minutes, h.drive_minutes, h.cargo_minutes, h.total_distance, h.operation_count, h.unko_nos, \
     h.overlap_drive_minutes, h.overlap_cargo_minutes, h.overlap_break_minutes, h.overlap_restraint_minutes, h.ot_late_night_minutes \
     FROM dtako_daily_work_hours h JOIN employees e ON e.id = h.driver_id \
     WHERE h.tenant_id = $1 ORDER BY e.driver_cd, h.work_date, h.start_time";
const SEGMENTS_QUERY: &str = "SELECT e.driver_cd, s.work_date, s.unko_no, s.segment_index, s.work_minutes, s.labor_minutes, \
     s.late_night_minutes, s.drive_minutes, s.cargo_minutes, \
     to_char(s.start_at AT TIME ZONE 'UTC', 'MM-DD HH24:MI') AS start_at, to_char(s.end_at AT TIME ZONE 'UTC', 'MM-DD HH24:MI') AS end_at \
     FROM dtako_daily_work_segments s JOIN employees e ON e.id = s.driver_id \
     WHERE s.tenant_id = $1 ORDER BY e.driver_cd, s.work_date, s.start_at";

/// 再計算は、アップロードしたときと同じ日別とセグメントを作り直す (分割の出力は運行NO ごとに 1 回だけ読む。2 人乗務の
/// 運行も、休息の分数はアップロードしたときと同じ)。event の並び。フェリーの記録 (KUDGFRY) が在れば、それも計算に入る。
#[tokio::test(flavor = "multi_thread")]
async fn recalculate_rebuilds_the_same_daily_hours_as_the_upload() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Recalc Route Tenant").await;
    ctx.upload_ok(t, "recalc.zip", &recalc_zip()).await;
    let days = ctx.rows(t, DAYS_QUERY).await;
    let segments = ctx.rows(t, SEGMENTS_QUERY).await;
    let rest = |rows: &[Value]| -> Vec<(String, i64)> {
        let pick = |r: &Value| {
            (
                r["driver_cd"].as_str().unwrap().to_owned(),
                r["total_rest_minutes"].as_i64().unwrap(),
            )
        };
        rows.iter().map(pick).collect()
    };
    let want_rest = [
        ("D-ONE".to_owned(), 300),
        ("D-ONE".to_owned(), 0),
        ("D-TWO".to_owned(), 360),
    ];
    assert_eq!(rest(&days), want_rest);

    // 日別とセグメントを消してから再計算する → 同じ行が戻る (休息の分数も同じ)
    {
        let mut c = ctx.pg.inner.lock().await;
        exec(
            &mut c,
            t,
            "DELETE FROM dtako_daily_work_segments WHERE tenant_id = $1",
        )
        .await;
        exec(
            &mut c,
            t,
            "DELETE FROM dtako_daily_work_hours WHERE tenant_id = $1",
        )
        .await;
    }
    let (status, events, _) = recalc(ctx.app(t), "year=2026&month=3").await;
    assert_eq!(status, StatusCode::OK);
    // 運行の行は 3 (2 人乗務の運行は乗務員ごと)、日エントリは 3
    let want_events = [
        json!({ "event": "progress", "current": 0, "total": 3, "step": "start" }),
        json!({ "event": "progress", "current": 3, "total": 3, "step": "save" }),
        json!({ "event": "done", "total": 3, "success": 3, "failed": 0 }),
    ];
    assert_eq!(events, want_events);
    assert_eq!(ctx.rows(t, DAYS_QUERY).await, days);
    assert_eq!(ctx.rows(t, SEGMENTS_QUERY).await, segments);
    // もう一度呼んでも同じ (上げ直しと同じく、その運行の古い行を消してから入れる)
    let (_, again, _) = recalc(ctx.app(t), "year=2026&month=3").await;
    assert_eq!(again, want_events);
    assert_eq!(ctx.rows(t, DAYS_QUERY).await, days);

    // フェリーの記録が在る運行は、その分数が計算に入る (1 時間のフェリー)
    let ferry = "運行NO,1,2,3,4,5,6,7,8,9,開始,終了\nU-6002,1,2,3,4,5,6,7,8,9,2026/03/05 10:15:00,2026/03/05 11:15:00\n";
    ctx.store.seed(
        &format!("{t}/unko/U-6002/KUDGFRY.csv"),
        encoding_rs::SHIFT_JIS.encode(ferry).0.into_owned(),
        "text/csv",
    );
    let (_, events, _) = recalc(ctx.app(t), "year=2026&month=3").await;
    assert_eq!(events, want_events);
    let with_ferry = ctx.rows(t, DAYS_QUERY).await;
    assert_eq!((&with_ferry[0], &with_ferry[2]), (&days[0], &days[2]));
    // U-6002 の日 (運転 360 分) からフェリーの 60 分が引かれる。ほかの欄は同じ
    let mut want_ferry_day = days[1].clone();
    for column in ["total_work_minutes", "total_drive_minutes", "drive_minutes"] {
        assert_eq!(days[1][column], 360, "{column}");
        want_ferry_day[column] = json!(300);
    }
    assert_eq!(with_ferry[1], want_ferry_day);
    assert_eq!(ctx.log_lines(), []);
    ctx.finish().await;
}

/// 失敗は stream の中の `error` (固定の語)。月が不正・KUDGIVT が無い・DB の失敗。1 人の保存が失敗したら、その人の分は
/// 戻り、そこで止まる (先に保存した人の分は残る)。別テナントには触れない。本文とログに識別子が出ない。
#[tokio::test(flavor = "multi_thread")]
async fn recalculate_reports_fixed_words_and_saves_per_driver() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Recalc Error Tenant").await;
    let other = ctx.tenant("Dtako Recalc Other Tenant").await;
    let error = |message: &str| json!({ "event": "error", "message": message });

    // 運行が無い月: start (0 件) → done
    let (status, events, _) = recalc(ctx.app(t), "year=2026&month=3").await;
    assert_eq!(status, StatusCode::OK);
    let empty = [
        json!({ "event": "progress", "current": 0, "total": 0, "step": "start" }),
        json!({ "event": "done", "total": 0, "success": 0, "failed": 0 }),
    ];
    assert_eq!(events, empty);
    // 月が不正: 最初の event が error
    let (status, events, invalid_body) = recalc(ctx.app(t), "year=2026&month=13").await;
    assert_eq!(
        (status, events),
        (StatusCode::OK, vec![error("month_invalid")])
    );
    // 12 月 (月末は翌年の 1 月 1 日の前日) も運行が無ければ start → done
    let (_, events, _) = recalc(ctx.app(t), "year=2025&month=12").await;
    assert_eq!(events, empty);
    // query が無い・数でない: axum の既定の 400 (口に届かない)
    for query in ["", "year=2026", "year=x&month=3"] {
        let (status, _, _) = recalc(ctx.app(t), query).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
    }

    // 2 人の運行を上げて、別テナントにも同じものを上げる
    let kudguri = [
        kudguri_line("U-7001", 1, "D-ONE", 2, 8, 17),
        kudguri_line("U-REJECT", 1, "D-TWO", 3, 8, 17),
    ];
    let kudgivt = [
        kudgivt_line("U-7001", 1, "D-ONE", 2, "08:15", "201", 540),
        kudgivt_line("U-REJECT", 1, "D-TWO", 3, "08:15", "201", 540),
    ];
    let zip = upload_zip(&kudguri, &kudgivt);
    ctx.upload_ok(t, "two.zip", &zip).await;
    ctx.upload_ok(other, "other.zip", &zip).await;
    let other_days = ctx.rows(other, DAYS_QUERY).await;

    // KUDGIVT が保存先に無い (運行は在る) → kudgivt_not_found。日別は変わらない
    let mut kudgivt_keys: Vec<String> = Vec::new();
    for unko_no in ["U-7001", "U-REJECT"] {
        let key = format!("{t}/unko/{unko_no}/KUDGIVT.csv");
        kudgivt_keys.push(key.clone());
    }
    let saved: Vec<(Vec<u8>, String)> = kudgivt_keys
        .iter()
        .map(|k| ctx.store.object(k).unwrap())
        .collect();
    for key in &kudgivt_keys {
        ctx.store.remove(key);
    }
    let days_before = ctx.rows(t, DAYS_QUERY).await;
    let (_, events, not_found_body) = recalc(ctx.app(t), "year=2026&month=3").await;
    let start = json!({ "event": "progress", "current": 0, "total": 2, "step": "start" });
    assert_eq!(events, [start.clone(), error("kudgivt_not_found")]);
    assert_eq!(ctx.rows(t, DAYS_QUERY).await, days_before);
    let warn = (
        LogLevel::Warn,
        "recalculate: KUDGIVT unavailable for 2 operation(s)".to_owned(),
    );
    assert_eq!(ctx.log_lines(), std::slice::from_ref(&warn));
    for (key, (bytes, content_type)) in kudgivt_keys.iter().zip(saved) {
        ctx.store.seed(key, bytes, &content_type);
    }

    // 検査用の制約を 2 つ足す: 未登録のイベントCD 999 の分類を足せない / U-REJECT のセグメントを入れられない
    let ctx = ctx
        .run_as_superuser(
            "ALTER TABLE alc_api.dtako_event_classifications ADD CONSTRAINT test_reject_class CHECK (event_cd <> '999') NOT VALID; \
             ALTER TABLE alc_api.dtako_daily_work_segments ADD CONSTRAINT test_reject_segment CHECK (unko_no <> 'U-REJECT') NOT VALID",
        )
        .await;

    // 分類を読む段で DB が失敗する (分割の出力の KUDGIVT に未登録のイベントCD が在る) → internal_error。日別は変わらない
    let key = format!("{t}/unko/U-7001/KUDGIVT.csv");
    let (original, content_type) = ctx.store.object(&key).unwrap();
    let mut with_unknown = String::from_utf8(original.clone()).unwrap();
    with_unknown.push_str(&format!(
        "{}\n",
        kudgivt_line("U-7001", 1, "D-ONE", 2, "17:15", "999", 10)
    ));
    ctx.store
        .seed(&key, with_unknown.into_bytes(), &content_type);
    let (_, events, class_body) = recalc(ctx.app(t), "year=2026&month=3").await;
    assert_eq!(events, [start.clone(), error("internal_error")]);
    assert_eq!(ctx.rows(t, DAYS_QUERY).await, days_before);
    ctx.store.seed(&key, original, &content_type);

    // 1 人の保存が失敗する: 乗務員CD の順に保存するので D-ONE が先に保存され、D-TWO の保存で落ちる
    // (D-TWO のセグメントの INSERT が検査用の制約に当たる)
    let mark = "UPDATE dtako_daily_work_hours SET total_work_minutes = 1 WHERE tenant_id = $1";
    {
        let mut c = ctx.pg.inner.lock().await;
        assert_eq!(exec(&mut c, t, mark).await, 2);
    }
    let (_, events, failed_body) = recalc(ctx.app(t), "year=2026&month=3").await;
    assert_eq!(events, [start, error("internal_error")]);
    let work = "SELECT e.driver_cd, h.total_work_minutes FROM dtako_daily_work_hours h \
                JOIN employees e ON e.id = h.driver_id WHERE h.tenant_id = $1 ORDER BY e.driver_cd";
    // D-ONE は保存し直された (540)。D-TWO は transaction が戻り、印を付けた値のまま
    let want_work = [
        json!({ "driver_cd": "D-ONE", "total_work_minutes": 540 }),
        json!({ "driver_cd": "D-TWO", "total_work_minutes": 1 }),
    ];
    assert_eq!(ctx.rows(t, work).await, want_work);
    let db_error = (LogLevel::Error, "recalculate failed: db (23514)".to_owned());
    assert_eq!(ctx.log_lines(), [warn, db_error.clone(), db_error]);
    // 別テナントの日別は、どれにも触れられていない
    assert_eq!(ctx.rows(other, DAYS_QUERY).await, other_days);

    // tenant ヘッダーの layer を通すと、ヘッダー無しは 401
    let guarded = tenant_router()
        .layer(middleware::from_fn(alc_core_wasm::require_tenant_header))
        .with_state(ctx.state());
    let (status, _, _) = recalc(guarded, "year=2026&month=3").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // 切れた接続: 運行を読む段で DB が失敗 → internal_error
    let ctx_logs = ctx.logs.clone();
    let bodies = [&invalid_body, &not_found_body, &class_body, &failed_body];
    let bodies: Vec<&str> = bodies.iter().map(|b| b.as_str()).collect();
    ctx.assert_no_identifiers(
        t,
        &bodies,
        &["U-7001", "U-REJECT", "D-ONE", "D-TWO", &other.to_string()],
    );
    let Ctx {
        db,
        pg,
        store,
        sleeper,
        logs,
    } = ctx;
    let pg = pg.sever().await;
    let state = DtakoState {
        pg,
        store,
        sleeper,
        clock: Arc::new(FakeClock::default()),
        log: logs.sink(),
    };
    let app = tenant_router()
        .with_state(state)
        .layer(Extension(TenantId(t)));
    let (status, events, _) = recalc(app, "year=2026&month=3").await;
    assert_eq!(
        (status, events),
        (StatusCode::OK, vec![error("internal_error")])
    );
    let last = ctx_logs.all().pop().unwrap();
    assert_eq!(
        last,
        (
            LogLevel::Error,
            "recalculate failed: db (closed)".to_owned()
        )
    );
    db.shutdown();
}

// ---- 乗務員ごとの再計算 ----

/// `POST {uri}` (body は `(content-type, 本文)`) → (status, event の列, 本文)。
async fn post_events(
    app: Router,
    uri: &str,
    body: Option<(&str, &str)>,
) -> (StatusCode, Vec<Value>, String) {
    let req = Request::builder().method("POST").uri(uri);
    let req = match body {
        Some((content_type, body)) => req
            .header("content-type", content_type)
            .body(Body::from(body.to_owned())),
        None => req.body(Body::empty()),
    };
    let (status, _, body) = call(app, req.unwrap()).await;
    let events = if status == StatusCode::OK {
        sse_events(&body)
    } else {
        Vec::new()
    };
    (status, events, body)
}

/// `POST /recalculate-driver?year=2026&month={month}&driver_id={id}`。
async fn recalc_driver(app: Router, month: u32, id: &str) -> (StatusCode, Vec<Value>, String) {
    let uri = format!("/recalculate-driver?year=2026&month={month}&driver_id={id}");
    post_events(app, &uri, None).await
}

/// `POST /recalculate-drivers` (JSON `{year: 2026, month, driver_ids}`)。
async fn recalc_drivers(app: Router, month: u32, ids: &[&str]) -> (StatusCode, Vec<Value>, String) {
    let body = json!({ "year": 2026, "month": month, "driver_ids": ids }).to_string();
    let body = Some(("application/json", body.as_str()));
    post_events(app, "/recalculate-drivers", body).await
}

/// 乗務員ごとの再計算の zip 2 本 (2026-03 の運行)。取り込みが運行ごとの分割の出力を置く。
/// 1 本目 = [`recalc_zip`] の運行 (U-6001 = D-ONE・D-TWO の 2 人乗務、U-6002 = D-ONE) に、KUDGURI に無い運行NO の
/// D-ONE の休息 (U-6099。03/05 = U-6002 の日。取り込みは zip の全行を渡すので数えるが、分割の出力には残らない) を足したもの。
/// 2 本目 = 別の乗務員 D-THREE の運行 (U-8001)。
fn driver_zips() -> (Vec<u8>, Vec<u8>) {
    let first = upload_zip(
        &[
            kudguri_line("U-6001", 1, "D-ONE", 2, 6, 23),
            kudguri_line("U-6001", 2, "D-TWO", 2, 6, 23),
            kudguri_line("U-6002", 1, "D-ONE", 5, 9, 15),
        ],
        &driver_zip_kudgivt()[..7],
    );
    let second = upload_zip(
        &[kudguri_line("U-8001", 1, "D-THREE", 10, 8, 17)],
        &driver_zip_kudgivt()[7..],
    );
    (first, second)
}

/// [`driver_zips`] の KUDGIVT の行 (1 本目が先頭 7 行、2 本目が残り)。
fn driver_zip_kudgivt() -> Vec<String> {
    vec![
        kudgivt_line("U-6001", 1, "D-ONE", 2, "06:15", "201", 240),
        kudgivt_line("U-6001", 1, "D-ONE", 2, "10:15", "302", 300),
        kudgivt_line("U-6001", 1, "D-ONE", 2, "15:15", "201", 420),
        kudgivt_line("U-6001", 2, "D-TWO", 2, "06:15", "302", 360),
        kudgivt_line("U-6001", 2, "D-TWO", 2, "12:15", "201", 600),
        kudgivt_line("U-6002", 1, "D-ONE", 5, "09:15", "201", 360),
        kudgivt_line("U-6099", 1, "D-ONE", 5, "20:15", "302", 30),
        kudgivt_line("U-8001", 1, "D-THREE", 10, "08:15", "201", 300),
        kudgivt_line("U-8001", 1, "D-THREE", 10, "13:15", "302", 120),
    ]
}

impl Ctx {
    /// 乗務員CD → 乗務員の id (文字列)。
    async fn driver_id(&self, tenant_id: Uuid, driver_cd: &str) -> String {
        let query = format!("SELECT id::text AS id FROM employees WHERE tenant_id = $1 AND driver_cd = '{driver_cd}'");
        let rows = self.rows(tenant_id, &query).await;
        rows[0]["id"].as_str().unwrap().to_owned()
    }

    /// 日別とセグメントを消す (`driver_cd` が `Some` なら、その乗務員の分だけ)。
    async fn clear_days(&self, tenant_id: Uuid, driver_cd: Option<&str>) {
        let only = driver_cd
            .map(|cd| {
                format!(" AND driver_id IN (SELECT id FROM employees WHERE driver_cd = '{cd}')")
            })
            .unwrap_or_default();
        let mut c = self.pg.inner.lock().await;
        for table in ["dtako_daily_work_segments", "dtako_daily_work_hours"] {
            let sql = format!("DELETE FROM {table} WHERE tenant_id = $1{only}");
            exec(&mut c, tenant_id, &sql).await;
        }
    }
}

/// 日別の行を比べる形 (乗務員CD・日・開始時刻・分数・運行NO)。
fn day_view(row: &Value) -> Value {
    json!([
        row["driver_cd"],
        row["work_date"],
        row["start_time"],
        row["total_work_minutes"],
        row["total_drive_minutes"],
        row["total_rest_minutes"],
        row["drive_minutes"],
        row["cargo_minutes"],
        row["unko_nos"],
    ])
}

/// 乗務員 1 人の再計算は、月の全員の口と同じ運行ごとの分割の出力を読む。乗務員の運行の外にある運行NO の行 (U-6099 の休息) は
/// 数えないので、月の全員の再計算と同じになる (取り込みの後の計算し直しも同じ数え方)。
/// 計算に残す行 (その乗務員の運行の行) で計算した結果は、その行だけを共有の `compute_daily_hours` に渡した結果と同じ。
/// ほかの乗務員の日別には触れない。
#[tokio::test(flavor = "multi_thread")]
async fn recalculate_driver_reads_the_split_outputs_like_the_month_recalculation() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Driver Recalc Tenant").await;
    let (first, second) = driver_zips();
    ctx.upload_ok(t, "first.zip", &first).await;
    ctx.upload_ok(t, "second.zip", &second).await;
    let days = ctx.rows(t, DAYS_QUERY).await;
    let rest = |rows: &[Value]| -> Vec<(String, i64)> {
        let pick = |r: &Value| {
            let cd = r["driver_cd"].as_str().unwrap().to_owned();
            (cd, r["total_rest_minutes"].as_i64().unwrap())
        };
        rows.iter().map(pick).collect()
    };
    let rest_of = |list: [(&str, i64); 4]| -> Vec<(String, i64)> {
        list.into_iter().map(|(cd, m)| (cd.to_owned(), m)).collect()
    };
    // 取り込みの後の計算し直し (乗務員の再計算の口と同じ数え方) で、D-ONE の 03/05 の休息 30 分 (KUDGURI に無い運行NO の行) は
    // 数えない (取り込み時の計算は zip の全行を渡すので数えるが、計算し直しが上書きする。Refs ippoan/alc-dtako-worker#23)
    let uploaded = [
        ("D-ONE", 300),
        ("D-ONE", 0),
        ("D-THREE", 120),
        ("D-TWO", 360),
    ];
    assert_eq!(rest(&days), rest_of(uploaded));

    // 月の全員の再計算 (その休息は数えない)
    ctx.clear_days(t, None).await;
    let (status, _, _) = recalc(ctx.app(t), "year=2026&month=3").await;
    assert_eq!(status, StatusCode::OK);
    let month_days = ctx.rows(t, DAYS_QUERY).await;
    let month_segments = ctx.rows(t, SEGMENTS_QUERY).await;
    let counted = [
        ("D-ONE", 300),
        ("D-ONE", 0),
        ("D-THREE", 120),
        ("D-TWO", 360),
    ];
    assert_eq!(rest(&month_days), rest_of(counted));

    // D-ONE の日別を消してから再計算する → 月の全員の再計算と同じ行が戻る。ほかの乗務員の行はそのまま
    ctx.clear_days(t, Some("D-ONE")).await;
    let one = ctx.driver_id(t, "D-ONE").await;
    let (status, events, body) = recalc_driver(ctx.app(t), 3, &one).await;
    assert_eq!(status, StatusCode::OK);
    // 運行の行は 2 (U-6001 の主・U-6002)、日エントリは 2
    let want_events = [
        json!({ "event": "progress", "current": 0, "total": 0, "step": "start" }),
        json!({ "event": "progress", "current": 2, "total": 2, "step": "save" }),
        json!({ "event": "done", "total": 2 }),
    ];
    assert_eq!(events, want_events);
    assert_eq!(ctx.rows(t, DAYS_QUERY).await, month_days);
    assert_eq!(ctx.rows(t, SEGMENTS_QUERY).await, month_segments);
    let views: Vec<Value> = month_days.iter().map(day_view).collect();

    // 絞らずに計算した結果: 全部の zip の全部の KUDGIVT の行 (重複を落とす) と D-ONE の運行で計算する
    let text = sjis_csv(KUDGIVT_HEADER, &driver_zip_kudgivt());
    let text = alc_csv_parser::decode_shift_jis(&text);
    let all = alc_csv_parser::kudgivt::parse_kudgivt(&text).unwrap();
    let mut all = alc_csv_parser::kudgivt::dedup_kudgivt_rows(all);
    assert_eq!(all.len(), 9);
    let ops = {
        let mut c = ctx.pg.inner.lock().await;
        let id = Uuid::parse_str(&one).unwrap();
        let (start, end) = (
            chrono::NaiveDate::from_ymd_opt(2026, 3, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2026, 4, 1).unwrap(),
        );
        let ops = alc_dtako_upload::pg::driver_operations_for_recalc(&mut c, t, id, start, end);
        ops.await.unwrap().unwrap().1
    };
    let rows: Vec<_> = ops
        .into_iter()
        .map(|op| alc_csv_parser::kudguri::RecalcOperation::from(op).into_kudguri_row())
        .collect();
    // 運行NO が D-ONE の運行に入っていない行 (U-6099・U-8001) は渡さない
    all.retain(|e| rows.iter().any(|r| r.unko_no == e.unko_no));
    assert_eq!(all.len(), 6);
    let classify = |e: &alc_csv_parser::kudgivt::KudgivtRow| {
        let class = alc_csv_parser::work_segments::default_classification(&e.event_cd).1;
        (e.event_cd.clone(), class)
    };
    let classifications = all.iter().map(classify).collect();
    let ferry = std::collections::HashMap::new();
    let unfiltered =
        alc_compare::upload_daily::compute_daily_hours(&rows, &all, &classifications, &ferry);
    let mut computed: Vec<Value> = unfiltered
        .iter()
        .map(|((cd, date, start), h)| {
            json!([
                cd,
                date.to_string(),
                start.format("%H:%M:%S").to_string(),
                h.total_work_minutes,
                h.saved_total_drive_minutes(),
                h.rest_event_minutes,
                h.drive_minutes,
                h.cargo_minutes,
                h.unko_nos,
            ])
        })
        .collect();
    computed.sort_by_key(|v| v.to_string());
    let saved_one: Vec<Value> = views.iter().filter(|v| v[0] == "D-ONE").cloned().collect();
    assert_eq!(computed, saved_one);
    // 運行のない月は start → done (0 件)
    let (_, events, _) = recalc_driver(ctx.app(t), 2, &one).await;
    let empty = [
        json!({ "event": "progress", "current": 0, "total": 0, "step": "start" }),
        json!({ "event": "done", "total": 0 }),
    ];
    assert_eq!(events, empty);
    // 分割の当たらない KUDGIVT (U-6099) の Warn は取り込みのもの。再計算のログは無い
    let recalc_logs: Vec<_> = ctx
        .log_lines()
        .into_iter()
        .filter(|(_, m)| m.starts_with("recalculate"))
        .collect();
    assert_eq!(recalc_logs, []);
    ctx.assert_no_identifiers(t, &[&body], &["U-6001", "U-6002", "D-ONE", &one]);
    ctx.finish().await;
}

/// 一括は乗務員ごとに分割の出力を読んで計算して保存する (引き当てられない人・失敗した人は数えて続ける)。
/// 失敗は固定の語: 月が不正・乗務員が居ない・query / body が読めない (4xx)・DB の失敗。
/// 別テナントには触れない。本文とログに識別子が出ない。
#[tokio::test(flavor = "multi_thread")]
async fn recalculate_drivers_reads_split_outputs_and_reports_fixed_words() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Drivers Recalc Tenant").await;
    let other = ctx.tenant("Dtako Drivers Recalc Other").await;
    let (first, second) = driver_zips();
    ctx.upload_ok(t, "first.zip", &first).await;
    ctx.upload_ok(t, "second.zip", &second).await;
    ctx.upload_ok(other, "other.zip", &first).await;
    let other_days = ctx.rows(other, DAYS_QUERY).await;
    let error = |message: &str| json!({ "event": "error", "message": message });
    let one = ctx.driver_id(t, "D-ONE").await;
    let three = ctx.driver_id(t, "D-THREE").await;
    let missing = Uuid::new_v4().to_string();
    let other_one = ctx.driver_id(other, "D-ONE").await;

    // 月の全員の再計算 (一括の結果の基準)
    ctx.clear_days(t, None).await;
    let (status, _, _) = recalc(ctx.app(t), "year=2026&month=3").await;
    assert_eq!(status, StatusCode::OK);
    let days = ctx.rows(t, DAYS_QUERY).await;
    // 一括: 2 人 + 居ない 1 人 → 2 人の日別が戻る (月の全員の再計算と同じ行)
    ctx.clear_days(t, None).await;
    let (status, events, batch_body) =
        recalc_drivers(ctx.app(t), 3, &[&one, &three, &missing]).await;
    assert_eq!(status, StatusCode::OK);
    let want = [
        json!({ "event": "batch_start", "total_drivers": 3 }),
        json!({ "event": "progress", "current": 1, "total": 3 }),
        json!({ "event": "progress", "current": 2, "total": 3 }),
        json!({ "event": "progress", "current": 3, "total": 3 }),
        json!({ "event": "batch_done", "total": 3, "done": 2, "errors": 1 }),
    ];
    assert_eq!(events, want);
    let not_d_two: Vec<Value> = days
        .iter()
        .filter(|r| r["driver_cd"] != "D-TWO")
        .cloned()
        .collect();
    assert_eq!(ctx.rows(t, DAYS_QUERY).await, not_d_two);
    let not_found = (
        LogLevel::Warn,
        "recalculate-drivers: driver not found".to_owned(),
    );
    let recalc_logs = |ctx: &Ctx| -> Vec<(LogLevel, String)> {
        let lines = ctx.log_lines().into_iter();
        lines
            .filter(|(_, m)| m.starts_with("recalculate"))
            .collect()
    };
    assert_eq!(recalc_logs(&ctx), std::slice::from_ref(&not_found));
    // 空の一覧: batch_start → batch_done (0 人)
    let (_, events, _) = recalc_drivers(ctx.app(t), 3, &[]).await;
    let empty = [
        json!({ "event": "batch_start", "total_drivers": 0 }),
        json!({ "event": "batch_done", "total": 0, "done": 0, "errors": 0 }),
    ];
    assert_eq!(events, empty);
    // 別テナントの乗務員は居ないのと同じ
    let (_, events, _) = recalc_driver(ctx.app(t), 3, &other_one).await;
    let start = json!({ "event": "progress", "current": 0, "total": 0, "step": "start" });
    assert_eq!(events, [start.clone(), error("driver_not_found")]);

    // 月が不正・乗務員が居ない (1 人の口)
    let (_, events, invalid_body) = recalc_driver(ctx.app(t), 13, &one).await;
    assert_eq!(events, [start.clone(), error("month_invalid")]);
    let (_, events, missing_body) = recalc_driver(ctx.app(t), 3, &missing).await;
    assert_eq!(events, [start.clone(), error("driver_not_found")]);
    let (_, events, _) = recalc_drivers(ctx.app(t), 13, &[&one]).await;
    let batch_start = json!({ "event": "batch_start", "total_drivers": 1 });
    assert_eq!(events, [batch_start.clone(), error("month_invalid")]);
    // query・body が読めない: 4xx と固定の語 (入力の値を返さない)
    let bad_query = |q: &str| format!("/recalculate-driver?{q}");
    for (q, want) in [
        ("year=2026&month=3", StatusCode::BAD_REQUEST),
        ("year=2026&month=3&driver_id=D-ONE", StatusCode::BAD_REQUEST),
    ] {
        let (status, _, body) = post_events(ctx.app(t), &bad_query(q), None).await;
        assert_eq!(
            (status, body.as_str()),
            (want, r#"{"error":"invalid_query"}"#)
        );
    }
    let json_type = "application/json";
    for (body, content_type, want) in [
        ("{", json_type, StatusCode::BAD_REQUEST),
        (
            r#"{"year":2026,"month":3,"driver_ids":["D-ONE"]}"#,
            json_type,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            r#"{"year":2026,"month":3,"driver_ids":[]}"#,
            "text/plain",
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
    ] {
        let sent = Some((content_type, body));
        let (status, _, body) = post_events(ctx.app(t), "/recalculate-drivers", sent).await;
        assert_eq!(
            (status, body.as_str()),
            (want, r#"{"error":"invalid_body"}"#)
        );
    }

    let days_now = ctx.rows(t, DAYS_QUERY).await;

    // 検査用の制約を 2 つ足す: 未登録のイベントCD 999 の分類を足せない / U-8001 (D-THREE) のセグメントを入れられない
    let ctx = ctx
        .run_as_superuser(
            "ALTER TABLE alc_api.dtako_event_classifications ADD CONSTRAINT test_reject_class CHECK (event_cd <> '999') NOT VALID; \
             ALTER TABLE alc_api.dtako_daily_work_segments ADD CONSTRAINT test_reject_segment CHECK (unko_no <> 'U-8001') NOT VALID",
        )
        .await;
    // 1 人の保存が失敗する → 1 人の口は internal_error、一括は数えて続ける
    let (_, events, save_body) = recalc_driver(ctx.app(t), 3, &three).await;
    assert_eq!(events, [start.clone(), error("internal_error")]);
    let (_, events, _) = recalc_drivers(ctx.app(t), 3, &[&three, &one]).await;
    let two = |done: i64, errors: i64| {
        vec![
            json!({ "event": "batch_start", "total_drivers": 2 }),
            json!({ "event": "progress", "current": 1, "total": 2 }),
            json!({ "event": "progress", "current": 2, "total": 2 }),
            json!({ "event": "batch_done", "total": 2, "done": done, "errors": errors }),
        ]
    };
    assert_eq!(events, two(1, 1));
    assert_eq!(ctx.rows(t, DAYS_QUERY).await, days_now);
    // 分類を足す段で失敗する (D-ONE の運行の分割の出力に未登録のイベントCD が在る)
    let key = format!("{t}/unko/U-6001/KUDGIVT.csv");
    let (kept, content_type) = ctx.store.object(&key).unwrap();
    let unknown = kudgivt_line("U-6001", 1, "D-ONE", 2, "22:15", "999", 10);
    let text = String::from_utf8(kept.clone()).unwrap();
    let text = format!("{}\r\n{unknown}\r\n", text.trim_end());
    ctx.store.seed(&key, text.into_bytes(), &content_type);
    let (_, events, class_body) = recalc_driver(ctx.app(t), 3, &one).await;
    assert_eq!(events, [start.clone(), error("internal_error")]);
    let (_, events, _) = recalc_drivers(ctx.app(t), 3, &[&one]).await;
    let one_failed = vec![
        batch_start.clone(),
        json!({ "event": "progress", "current": 1, "total": 1 }),
        json!({ "event": "batch_done", "total": 1, "done": 0, "errors": 1 }),
    ];
    assert_eq!(events, one_failed);
    ctx.store.seed(&key, kept, &content_type);
    let failed = |name: &str| (LogLevel::Error, format!("{name} failed: db (23514)"));
    let driver_failed = |kind: &str| {
        (
            LogLevel::Warn,
            format!("recalculate-drivers: driver failed: db ({kind})"),
        )
    };
    let want_logs = vec![
        not_found.clone(),
        failed("recalculate-driver"),
        driver_failed("23514"),
        failed("recalculate-driver"),
        driver_failed("23514"),
    ];
    assert_eq!(recalc_logs(&ctx), want_logs);

    // 運行の表を読めない (42501): 1 人の口は internal_error、一括はその人を数えて続ける
    let ctx = ctx
        .run_as_superuser("REVOKE SELECT ON alc_api.dtako_operations FROM alc_api_app")
        .await;
    let (_, events, _) = recalc_driver(ctx.app(t), 3, &one).await;
    assert_eq!(events, [start.clone(), error("internal_error")]);
    // 一括は、その人たちを引けず、消す対象の運行NO の一覧 (月の運行) も読めないので、全体を internal_error で終える
    let (_, events, _) = recalc_drivers(ctx.app(t), 3, &[&one, &three]).await;
    let both = json!({ "event": "batch_start", "total_drivers": 2 });
    assert_eq!(events, [both, error("internal_error")]);
    let tail: Vec<_> = recalc_logs(&ctx).split_off(want_logs.len());
    let kinds: Vec<&str> = tail.iter().map(|(_, m)| m.as_str()).collect();
    assert_eq!(
        kinds,
        [
            "recalculate-driver failed: db (42501)",
            "recalculate-drivers: driver failed: db (42501)",
            "recalculate-drivers: driver failed: db (42501)",
            "recalculate-drivers failed: db (42501)",
        ]
    );
    // 別テナントの日別は、どれにも触れられていない
    {
        let ctx = ctx
            .run_as_superuser("GRANT SELECT ON alc_api.dtako_operations TO alc_api_app")
            .await;
        assert_eq!(ctx.rows(other, DAYS_QUERY).await, other_days);

        // tenant ヘッダーの layer を通すと、ヘッダー無しは 401
        let guarded = tenant_router()
            .layer(middleware::from_fn(alc_core_wasm::require_tenant_header))
            .with_state(ctx.state());
        let (status, _, _) = recalc_driver(guarded.clone(), 3, &one).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _, _) = recalc_drivers(guarded, 3, &[&one]).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let bodies = [
            &batch_body,
            &invalid_body,
            &missing_body,
            &save_body,
            &class_body,
        ];
        let bodies: Vec<&str> = bodies.iter().map(|b| b.as_str()).collect();
        let needles = [
            "U-6001", "U-6002", "U-8001", "D-ONE", "D-THREE", "x-", &one, &three, &missing,
        ];
        ctx.assert_no_identifiers(t, &bodies, &needles);
        ctx.finish().await;
    }
}

/// 一括で 1 人だけ KUDGIVT が 1 件も無い (行はある) と、その人だけ errors に数えて飛ばし、ほかの乗務員は保存する
/// (ログは固定の語)。1 人の口は `error{kudgivt_not_found}` で終わる。
#[tokio::test(flavor = "multi_thread")]
async fn recalculate_drivers_counts_only_the_driver_without_kudgivt() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Drivers No Kudgivt Tenant").await;
    let (first, second) = driver_zips();
    ctx.upload_ok(t, "first.zip", &first).await;
    ctx.upload_ok(t, "second.zip", &second).await;
    let (status, _, _) = recalc(ctx.app(t), "year=2026&month=3").await;
    assert_eq!(status, StatusCode::OK);
    let days = ctx.rows(t, DAYS_QUERY).await;
    // D-THREE の運行 (U-8001) の分割の出力の KUDGIVT を、見出しだけのものに替える
    let key = format!("{t}/unko/U-8001/KUDGIVT.csv");
    let (kept, content_type) = ctx.store.object(&key).unwrap();
    let text = String::from_utf8(kept).unwrap();
    let header = text.lines().next().unwrap();
    ctx.store
        .seed(&key, format!("{header}\r\n").into_bytes(), &content_type);
    let one = ctx.driver_id(t, "D-ONE").await;
    let three = ctx.driver_id(t, "D-THREE").await;

    ctx.clear_days(t, None).await;
    let (status, events, body) = recalc_drivers(ctx.app(t), 3, &[&three, &one]).await;
    assert_eq!(status, StatusCode::OK);
    let want = [
        json!({ "event": "batch_start", "total_drivers": 2 }),
        json!({ "event": "progress", "current": 1, "total": 2 }),
        json!({ "event": "progress", "current": 2, "total": 2 }),
        json!({ "event": "batch_done", "total": 2, "done": 1, "errors": 1 }),
    ];
    assert_eq!(events, want);
    // D-ONE は保存され、D-THREE は入らない
    let only_one: Vec<Value> = days
        .iter()
        .filter(|r| r["driver_cd"] == "D-ONE")
        .cloned()
        .collect();
    assert_eq!(ctx.rows(t, DAYS_QUERY).await, only_one);
    let driver_failed = (
        LogLevel::Warn,
        "recalculate-drivers: driver failed: kudgivt_not_found".to_owned(),
    );
    let recalc_logs: Vec<_> = ctx
        .log_lines()
        .into_iter()
        .filter(|(_, m)| m.starts_with("recalculate"))
        .collect();
    assert_eq!(recalc_logs, [driver_failed]);
    // 1 人の口は固定の語で終わる
    let (_, events, body_one) = recalc_driver(ctx.app(t), 3, &three).await;
    let start = json!({ "event": "progress", "current": 0, "total": 0, "step": "start" });
    let not_found = json!({ "event": "error", "message": "kudgivt_not_found" });
    assert_eq!(events, [start, not_found]);
    ctx.assert_no_identifiers(t, &[&body, &body_one], &["U-8001", "D-THREE", &three]);
    ctx.finish().await;
}

impl Ctx {
    /// 古い日別の行を足す (`driver_cd` の乗務員の 2026-03-`day`。`unko_no` を運行NO に持つ。今の計算には出ない)。
    async fn stale_day(&self, tenant_id: Uuid, driver_cd: &str, day: u32, unko_no: &str) {
        let sql = format!(
            "INSERT INTO dtako_daily_work_hours (tenant_id, driver_id, work_date, start_time, total_work_minutes, \
             total_drive_minutes, total_rest_minutes, late_night_minutes, drive_minutes, cargo_minutes, total_distance, \
             operation_count, unko_nos) \
             SELECT $1, id, DATE '2026-03-{day:02}', TIME '08:00', 1, 1, 0, 0, 1, 0, 0, 1, ARRAY['{unko_no}'] \
             FROM employees WHERE tenant_id = $1 AND driver_cd = '{driver_cd}'"
        );
        let mut c = self.pg.inner.lock().await;
        assert_eq!(exec(&mut c, tenant_id, &sql).await, 1);
    }

    /// 古い日別の行 (2026-03-20 より後の日) の `(乗務員CD, 日)`。
    async fn stale_days(&self, tenant_id: Uuid) -> Vec<(String, String)> {
        let query = "SELECT e.driver_cd, h.work_date FROM dtako_daily_work_hours h \
                     JOIN employees e ON e.id = h.driver_id \
                     WHERE h.tenant_id = $1 AND h.work_date > DATE '2026-03-20' ORDER BY e.driver_cd, h.work_date";
        let rows = self.rows(tenant_id, query).await;
        let pick = |r: &Value| {
            let cd = r["driver_cd"].as_str().unwrap().to_owned();
            (cd, r["work_date"].as_str().unwrap().to_owned())
        };
        rows.iter().map(pick).collect()
    }

    /// 古い行を 3 つ足す: D-ONE の、月の別の乗務員の運行 (U-8001) を持つ行 (3/25)・日エントリを保存しない運行 (U-6003。乗務員が無い) を持つ行 (3/26)・D-THREE の、U-6003 を持つ行 (3/27)。
    async fn add_stale_days(&self, tenant_id: Uuid) {
        self.stale_day(tenant_id, "D-ONE", 25, "U-8001").await;
        self.stale_day(tenant_id, "D-ONE", 26, "U-6003").await;
        self.stale_day(tenant_id, "D-THREE", 27, "U-6003").await;
    }
}

/// 3 つの再計算の口は、消す対象の運行NO を、その月の再計算の対象の運行のものに揃える。今の計算に出ない運行NO を持つ古い行
/// (別の乗務員の運行・日エントリを保存しない運行) は、月の全員・1 人・一括のどれでも消える。消すのは、
/// 日エントリの在る乗務員の行だけ (1 人の口は、ほかの乗務員の同じ形の行を消さない)。結果は 3 口とも同じ。
#[tokio::test(flavor = "multi_thread")]
async fn recalculation_deletes_stale_days_of_the_months_operations_in_every_entry() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Recalc Delete Scope Tenant").await;
    let (first, second) = driver_zips();
    ctx.upload_ok(t, "first.zip", &first).await;
    ctx.upload_ok(t, "second.zip", &second).await;
    // 乗務員の無い運行 (U-6003。日エントリを保存しない。月の運行には入るが、どの乗務員の運行にも入らない)
    let no_entry = upload_zip(&[kudguri_line("U-6003", 1, "D-ONE", 20, 8, 17)], &[]);
    ctx.upload_ok(t, "no-entry.zip", &no_entry).await;
    {
        let mut c = ctx.pg.inner.lock().await;
        let unset = "UPDATE dtako_operations SET driver_id = NULL WHERE tenant_id = $1 AND unko_no = 'U-6003'";
        assert_eq!(exec(&mut c, t, unset).await, 1);
    }
    let (status, _, _) = recalc(ctx.app(t), "year=2026&month=3").await;
    assert_eq!(status, StatusCode::OK);
    let baseline = ctx.rows(t, DAYS_QUERY).await;
    let one_of = |rows: &[Value]| -> Vec<Value> {
        let one = |r: &&Value| r["driver_cd"] == "D-ONE";
        rows.iter().filter(one).cloned().collect()
    };
    let baseline_one = one_of(&baseline);
    assert_eq!(baseline_one.len(), 2);
    assert_eq!(ctx.stale_days(t).await, []);
    let one = ctx.driver_id(t, "D-ONE").await;
    let only_three = [("D-THREE".to_owned(), "2026-03-27".to_owned())];

    // 1 人の口: D-ONE の古い 2 行は消え、D-THREE の同じ形の行は残る。D-ONE の結果は月の全員の口と同じ
    ctx.add_stale_days(t).await;
    assert_eq!(ctx.stale_days(t).await.len(), 3);
    let (status, events, _) = recalc_driver(ctx.app(t), 3, &one).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        events.last().unwrap(),
        &json!({ "event": "done", "total": 2 })
    );
    assert_eq!(ctx.stale_days(t).await, only_three);
    assert_eq!(one_of(&ctx.rows(t, DAYS_QUERY).await), baseline_one);

    // 一括の口: 同じ
    ctx.stale_day(t, "D-ONE", 25, "U-8001").await;
    ctx.stale_day(t, "D-ONE", 26, "U-6003").await;
    assert_eq!(ctx.stale_days(t).await.len(), 3);
    let (status, events, _) = recalc_drivers(ctx.app(t), 3, &[&one]).await;
    assert_eq!(status, StatusCode::OK);
    let done = json!({ "event": "batch_done", "total": 1, "done": 1, "errors": 0 });
    assert_eq!(events.last().unwrap(), &done);
    assert_eq!(ctx.stale_days(t).await, only_three);
    assert_eq!(one_of(&ctx.rows(t, DAYS_QUERY).await), baseline_one);

    // 月の全員の口: 3 行とも消える (D-THREE も日エントリが在る)。結果は同じ
    ctx.stale_day(t, "D-ONE", 25, "U-8001").await;
    ctx.stale_day(t, "D-ONE", 26, "U-6003").await;
    let (status, _, _) = recalc(ctx.app(t), "year=2026&month=3").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ctx.stale_days(t).await, []);
    assert_eq!(ctx.rows(t, DAYS_QUERY).await, baseline);
    ctx.finish().await;
}

// ---- 取り込みの後の日別の計算し直し (Refs ippoan/alc-dtako-worker#23) ----

/// 同じ乗務員 (D-ONE) の 2 運行の KUDGURI。1 本目 (U-7001) の帰着 03/02 21:15 から 7 時間で 2 本目 (U-7002) が 03/03 04:15 に出る
/// (日跨ぎの 480 分未満) ので、乗務員の月をまとめて計算すると 1 つの勤務日に束ねられる。
fn bundled_kudguri() -> Vec<String> {
    vec![
        kudguri_line("U-7001", 1, "D-ONE", 2, 8, 21),
        kudguri_line("U-7002", 1, "D-ONE", 3, 4, 12),
    ]
}

/// [`bundled_kudguri`] の KUDGIVT (どちらも運転だけ)。
fn bundled_kudgivt() -> Vec<String> {
    vec![
        kudgivt_line("U-7001", 1, "D-ONE", 2, "08:15", "201", 780),
        kudgivt_line("U-7002", 1, "D-ONE", 3, "04:15", "201", 480),
    ]
}

/// [`bundled_kudguri`] の運行を 1 つずつ別の zip にしたもの。
fn bundled_zips() -> (Vec<u8>, Vec<u8>) {
    let (kudguri, kudgivt) = (bundled_kudguri(), bundled_kudgivt());
    let first = upload_zip(&kudguri[..1], &kudgivt[..1]);
    let second = upload_zip(&kudguri[1..], &kudgivt[1..]);
    (first, second)
}

/// `kudguri`・`kudgivt` の行だけを共有の `compute_daily_hours` に渡した日別 ([`day_view`] の形・並び。既定の分類・フェリーなし)。
fn computed_days(kudguri: &[String], kudgivt: &[String]) -> Vec<Value> {
    let files = [
        ("KUDGURI.csv".to_owned(), sjis_csv(KUDGURI_HEADER, kudguri)),
        ("KUDGIVT.csv".to_owned(), sjis_csv(KUDGIVT_HEADER, kudgivt)),
    ];
    let rows = alc_csv_parser::kudguri_rows_in(&files).unwrap().unwrap();
    let events = alc_csv_parser::kudgivt_rows_in(&files).unwrap().unwrap();
    let classify = |e: &alc_csv_parser::kudgivt::KudgivtRow| {
        let class = alc_csv_parser::work_segments::default_classification(&e.event_cd).1;
        (e.event_cd.clone(), class)
    };
    let classifications = events.iter().map(classify).collect();
    let ferry = std::collections::HashMap::new();
    let daily =
        alc_compare::upload_daily::compute_daily_hours(&rows, &events, &classifications, &ferry);
    let mut views: Vec<Value> = daily
        .iter()
        .map(|((cd, date, start), h)| {
            json!([
                cd,
                date.to_string(),
                start.format("%H:%M:%S").to_string(),
                h.total_work_minutes,
                h.saved_total_drive_minutes(),
                h.rest_event_minutes,
                h.drive_minutes,
                h.cargo_minutes,
                h.unko_nos,
            ])
        })
        .collect();
    views.sort_by_key(|v| v.to_string());
    views
}

impl Ctx {
    /// 日別の計算し直しの GET の上限を `max_gets` にした口。
    fn limited_app(&self, tenant_id: Uuid, max_gets: usize) -> Router {
        let limits = IngestLimits {
            max_daily_recalc_gets: max_gets,
            ..IngestLimits::default()
        };
        tenant_router_with(limits)
            .with_state(self.state())
            .layer(Extension(TenantId(tenant_id)))
    }

    /// `app` に zip を送り、200 を確かめて本文を返す。
    async fn upload_via(&self, app: Router, filename: &str, zip: &[u8]) -> String {
        let body = multipart_body("file", Some(filename), zip);
        let (status, body) = send(app, &multipart_type(), body).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }

    /// 運行ごとの分割の出力 (`{テナント}/unko/`) への GET が呼ばれた回数の合計。
    fn unko_gets(&self, tenant_id: Uuid) -> u32 {
        let calls = self
            .store
            .get_calls_containing(&format!("{tenant_id}/unko/"));
        calls.iter().map(|(_, n)| n).sum()
    }

    /// 日別の計算し直しのログ (計算し直しの Warn と、読めない KUDGIVT の Warn)。
    fn daily_logs(&self) -> Vec<(LogLevel, String)> {
        let lines = self.log_lines().into_iter();
        let daily = |(_, m): &(LogLevel, String)| {
            m.contains("daily recalc") || m.contains("KUDGIVT unavailable for")
        };
        lines.filter(daily).collect()
    }

    /// 日別を [`day_view`] の形で (乗務員CD・日・開始時刻の順)。
    async fn day_views(&self, tenant_id: Uuid) -> Vec<Value> {
        self.rows(tenant_id, DAYS_QUERY)
            .await
            .iter()
            .map(day_view)
            .collect()
    }
}

fn warn(message: &str) -> (LogLevel, String) {
    (LogLevel::Warn, message.to_owned())
}

/// 取り込み (アップロード・やり直し) の後、今回の乗務員 × 月を、乗務員の再計算の口と同じ数え方で計算し直す。別々の zip で
/// 取り込んだ同じ乗務員の 2 運行は、2 本目の取り込み直後に、再計算の口を打った後と同じ日別・セグメントになる。
/// 陽性対照: 計算し直しを通さない値 (上限 0 で飛ばした取り込み = 2 本目の zip だけで計算した値) とは違う。
/// やり直しの口も計算し直す。
#[tokio::test(flavor = "multi_thread")]
async fn upload_recalculates_the_drivers_month_like_the_driver_recalculation() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Daily Recalc Tenant").await;
    let (first, second) = bundled_zips();
    ctx.upload_ok(t, "first.zip", &first).await;
    let gets_before = ctx.unko_gets(t);
    let (status, body) = ctx.upload(t, "second.zip", &second).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // 2 本目の計算し直しの GET = 乗務員の月の運行 2 つ × (KUDGIVT・KUDGFRY)
    assert_eq!(ctx.unko_gets(t) - gets_before, 4);
    let days = ctx.rows(t, DAYS_QUERY).await;
    let segments = ctx.rows(t, SEGMENTS_QUERY).await;
    let views = ctx.day_views(t).await;
    // 03/02 に始まる勤務日に 2 運行が束ねられている
    assert_eq!(views[0][1], "2026-03-02");
    assert_eq!(views[0][8], json!(["U-7001", "U-7002"]));

    // 乗務員の再計算の口を打っても、日別もセグメントも変わらない
    let one = ctx.driver_id(t, "D-ONE").await;
    let (status, events, _) = recalc_driver(ctx.app(t), 3, &one).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        events.last().unwrap(),
        &json!({ "event": "done", "total": 2 })
    );
    assert_eq!(ctx.rows(t, DAYS_QUERY).await, days);
    assert_eq!(ctx.rows(t, SEGMENTS_QUERY).await, segments);
    assert_eq!(ctx.daily_logs(), []);

    // 陽性対照: 計算し直しを飛ばす (上限 0) と、2 本目の日別は 2 本目の zip だけで計算した値のまま。それは束ねた値と違う
    let raw = ctx.tenant("Dtako Daily Recalc Raw").await;
    ctx.upload_via(ctx.limited_app(raw, 0), "first.zip", &first)
        .await;
    let raw_body = ctx
        .upload_via(ctx.limited_app(raw, 0), "second.zip", &second)
        .await;
    let (kudguri, kudgivt) = (bundled_kudguri(), bundled_kudgivt());
    let only_first = computed_days(&kudguri[..1], &kudgivt[..1]);
    let only_second = computed_days(&kudguri[1..], &kudgivt[1..]);
    let raw_views = ctx.day_views(raw).await;
    assert_eq!(raw_views, [only_first, only_second.clone()].concat());
    assert_ne!(raw_views, views);
    assert!(!views.contains(&only_second[0]));
    let skipped = warn("upload: daily recalc skipped over the GET limit: 1");
    assert_eq!(ctx.daily_logs(), [skipped.clone(), skipped]);

    // やり直しの口 (上限は既定) は計算し直す → 束ねた値になる
    let raw_id = json_of(&raw_body)["upload_id"].as_str().unwrap().to_owned();
    let (status, again) = ctx.rerun(raw, &raw_id).await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(ctx.day_views(raw).await, views);
    assert_eq!(ctx.rows(raw, SEGMENTS_QUERY).await, segments);
    assert_eq!(ctx.daily_logs().len(), 2);
    let needles = ["U-7001", "U-7002", "D-ONE", &one, &raw_id];
    ctx.assert_no_identifiers(t, &[], &needles);
    ctx.assert_no_identifiers(raw, &[], &needles);
    ctx.finish().await;
}

/// 計算し直しができないとき、取り込みは成功のまま (200・本文の形は同じ) で、日別は取り込み時の値が残る。ログは固定の語と件数だけ。
/// - 分割が尽きた (PUT が全部失敗): 計算し直さない
/// - 分割の出力の KUDGIVT を読めない (GET の失敗): その乗務員 × 月を失敗に数える
/// - 保存が落ちる (検査用の制約): 同じ
#[tokio::test(flavor = "multi_thread")]
async fn upload_keeps_the_upload_time_days_when_the_daily_recalc_fails() {
    let ctx = Ctx::start().await;
    let (first, second) = bundled_zips();
    // 取り込み時の値 (計算し直しを飛ばした取り込み)
    let raw = ctx.tenant("Dtako Daily Keep Raw").await;
    ctx.upload_via(ctx.limited_app(raw, 0), "first.zip", &first)
        .await;
    ctx.upload_via(ctx.limited_app(raw, 0), "second.zip", &second)
        .await;
    let raw_days = ctx.rows(raw, DAYS_QUERY).await;
    let raw_segments = ctx.rows(raw, SEGMENTS_QUERY).await;
    let skipped = warn("upload: daily recalc skipped over the GET limit: 1");
    assert_eq!(ctx.daily_logs(), [skipped.clone(), skipped.clone()]);

    // 分割の PUT が全部失敗する (2 エントリ) → 200・split_failed = 2・取り込み時の値のまま
    let s = ctx.tenant("Dtako Daily Keep Split").await;
    ctx.upload_ok(s, "first.zip", &first).await;
    ctx.store.fail_puts_containing(&format!("{s}/unko/"), 100);
    let (status, split_body) = ctx.upload(s, "second.zip", &second).await;
    assert_eq!(status, StatusCode::OK, "{split_body}");
    assert_eq!(json_of(&split_body)["split_failed"], 2);
    assert_eq!(ctx.rows(s, DAYS_QUERY).await, raw_days);
    assert_eq!(ctx.rows(s, SEGMENTS_QUERY).await, raw_segments);
    let split_failed = warn("upload: daily recalc skipped: split failed");
    let mut want = vec![skipped.clone(), skipped, split_failed];
    assert_eq!(ctx.daily_logs(), want);

    // 分割の出力を読めない → 200・取り込み時の値のまま・失敗の件数
    let g = ctx.tenant("Dtako Daily Keep Get").await;
    ctx.upload_ok(g, "first.zip", &first).await;
    ctx.store.fail_gets_containing(&format!("{g}/unko/"), 100);
    let (status, get_body) = ctx.upload(g, "second.zip", &second).await;
    assert_eq!(status, StatusCode::OK, "{get_body}");
    assert_eq!(json_of(&get_body)["split_failed"], 0);
    assert_eq!(ctx.rows(g, DAYS_QUERY).await, raw_days);
    assert_eq!(ctx.rows(g, SEGMENTS_QUERY).await, raw_segments);
    want.push(warn("upload: KUDGIVT unavailable for 2 operation(s)"));
    want.push(warn("upload: daily recalc failed: 1"));
    assert_eq!(ctx.daily_logs(), want);

    // 保存が落ちる (束ねた日エントリ = 運行NO 2 つ、を入れられない検査用の制約) → 200・取り込み時の値のまま・失敗の件数
    let ctx = ctx
        .run_as_superuser(
            "ALTER TABLE alc_api.dtako_daily_work_hours ADD CONSTRAINT test_reject_bundled \
             CHECK (cardinality(unko_nos) < 2) NOT VALID",
        )
        .await;
    let d = ctx.tenant("Dtako Daily Keep Db").await;
    ctx.upload_ok(d, "first.zip", &first).await;
    let (status, db_body) = ctx.upload(d, "second.zip", &second).await;
    assert_eq!(status, StatusCode::OK, "{db_body}");
    assert_eq!(ctx.rows(d, DAYS_QUERY).await, raw_days);
    assert_eq!(ctx.rows(d, SEGMENTS_QUERY).await, raw_segments);
    want.push(warn("upload: daily recalc failed: 1"));
    assert_eq!(ctx.daily_logs(), want);
    // 本文の形 (8 フィールドとその順) は変わらない
    let keys: Vec<String> = [&split_body, &get_body, &db_body]
        .iter()
        .map(|b| {
            let v: serde_json::Map<String, Value> = serde_json::from_str(b).unwrap();
            v.keys().cloned().collect::<Vec<_>>().join(",")
        })
        .collect();
    assert!(split_body.starts_with(r#"{"upload_id":""#));
    assert_eq!(keys[0], keys[1]);
    assert_eq!(keys[1], keys[2]);
    let needles = ["U-7001", "U-7002", "D-ONE"];
    for tenant_id in [raw, s, g, d] {
        ctx.assert_no_identifiers(tenant_id, &[], &needles);
    }
    ctx.finish().await;
}

/// 計算し直しの GET が上限を越える乗務員 × 月から先は飛ばす (件数だけ Warn)。上限 2 = 運行 1 つぶん: 3 人のうち 1 人だけ計算し直す。
#[tokio::test(flavor = "multi_thread")]
async fn upload_skips_the_daily_recalc_over_the_get_limit() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("Dtako Daily Limit Tenant").await;
    let zip = upload_zip(
        &[
            kudguri_line("U-8001", 1, "D-THREE", 10, 8, 17),
            kudguri_line("U-9001", 1, "D-FOUR", 11, 8, 17),
            kudguri_line("U-9002", 1, "D-FIVE", 12, 8, 17),
        ],
        &[
            kudgivt_line("U-8001", 1, "D-THREE", 10, "08:15", "201", 300),
            kudgivt_line("U-9001", 1, "D-FOUR", 11, "08:15", "201", 300),
            kudgivt_line("U-9002", 1, "D-FIVE", 12, "08:15", "201", 300),
        ],
    );
    let body = ctx
        .upload_via(ctx.limited_app(t, 2), "three.zip", &zip)
        .await;
    assert_eq!(json_of(&body)["split_failed"], 0);
    assert_eq!(ctx.unko_gets(t), 2);
    let skipped = warn("upload: daily recalc skipped over the GET limit: 2");
    assert_eq!(ctx.daily_logs(), [skipped]);
    // 既定の上限では 3 人とも計算し直す (運行 3 つ × 2)
    ctx.upload_ok(t, "three-again.zip", &zip).await;
    assert_eq!(ctx.unko_gets(t), 2 + 3 * 2 + 3);
    assert_eq!(ctx.daily_logs().len(), 1);
    // 成功の本文は運行NO の一覧を返す (アップロードの口の 8 フィールド)。識別子を見るのはログだけ
    assert_eq!(json_of(&body)["split_unko_nos_total"], 3);
    ctx.assert_no_identifiers(t, &[], &["U-8001", "U-9001", "D-THREE", "D-FOUR"]);
    ctx.finish().await;
}
