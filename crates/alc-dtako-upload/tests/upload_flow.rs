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
use alc_dtako_upload::routes::{tenant_router, tenant_router_with, DtakoState};
use alc_dtako_upload::split::LogLevel;
use alc_dtako_upload::store::GET_CONCURRENCY;
use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::{middleware, Extension, Router};
use embedded::{rows_json, tenant, upload, Embedded, Held, APP_ROLE};
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
    let stages = "history;dur=7, put_zip;dur=7, parse;dur=7, prepare;dur=7, old_kudgivt;dur=7, apply;dur=7, split;dur=7";
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

    // 初回: 既に在る運行が無いので、前回の KUDGIVT は読まない (GET は分割の zip の読み直しの 1 本だけ)
    ctx.upload_ok(t, "first.zip", &zip_of_minutes(100)).await;
    assert_eq!(ctx.store.max_get_running(), 1);

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
    assert_eq!(
        ctx.log_lines(),
        [warn(1), warn(2), warn(1), warn(2), warn(3)]
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
    let stages = "zip_key;dur=7, get_zip;dur=7, parse;dur=7, prepare;dur=7, old_kudgivt;dur=7, apply;dur=7, split;dur=7";
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
