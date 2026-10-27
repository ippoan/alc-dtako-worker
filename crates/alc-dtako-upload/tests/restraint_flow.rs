//! 拘束時間の帳票の口 (`GET /restraint-report`・`POST /restraint-report/compare-csv`、W5c-3) を、口から DB まで通して確かめる (native)。
//!
//! DB はテストの process の中で起こす組み込みの PostgreSQL (`embedded/mod.rs`)、保存先・ログは偽物 (`fakes/mod.rs`)。
//! 期待値は rust-alc-api の同じ口 (`tests/mock_tests/mock_dtako_restraint_report_test.rs`) と同じ。乗務員CD・名前・運行NO は作り物。
//! 日の行・週の小計の細かい規則は `src/restraint/{report,compare}.rs` の単体テスト (rust から写したもの) が縛る。

// 共用の土台・偽物のうち、このファイルが使わないものが在る
#[allow(dead_code)]
mod embedded;
#[allow(dead_code)]
mod fakes;

use std::sync::Arc;

use alc_core_wasm::TenantId;
use alc_dtako_upload::routes::{tenant_router, DtakoState};
use alc_dtako_upload::split::LogLevel;
use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::{Extension, Router};
use embedded::{employee, exec, tenant, Embedded, Held, APP_ROLE};
use fakes::{FakeClock, FakeSleeper, FakeStore, Logs};
use futures_util::lock::Mutex;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

const BOUNDARY: &str = "----restraint-boundary-3c9e";

struct Ctx {
    db: Embedded,
    pg: Held<Arc<Mutex<PgClient>>>,
    logs: Logs,
}

impl Ctx {
    async fn start() -> Self {
        let db = Embedded::start().await;
        let pg = db.shared(APP_ROLE).await;
        Self {
            db,
            pg,
            logs: Logs::default(),
        }
    }

    fn app(&self, tenant_id: Uuid) -> Router {
        let state = DtakoState {
            pg: self.pg.inner.clone(),
            store: Arc::new(FakeStore::default()),
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
        let Self { db, pg, logs } = self;
        pg.close().await;
        let su = db.superuser().await;
        su.inner.batch_execute(sql).await.unwrap();
        su.close().await;
        let pg = db.shared(APP_ROLE).await;
        Self { db, pg, logs }
    }

    async fn finish(self) {
        self.pg.close().await;
        self.db.shutdown();
    }

    async fn tenant(&self, name: &str) -> Uuid {
        let mut c = self.pg.inner.lock().await;
        tenant(&mut c, name).await
    }

    async fn driver(&self, t: Uuid, cd: &str, name: &str) -> Uuid {
        let mut c = self.pg.inner.lock().await;
        employee(&mut c, t, None, Some(cd), name, false).await
    }

    async fn exec(&self, t: Uuid, sql: &str) {
        let mut c = self.pg.inner.lock().await;
        assert_eq!(exec(&mut c, t, sql).await, 1, "{sql}");
    }

    /// 区間 1 行。日時は `+00` の壁時計 (取り込みは JST の壁時計を UTC として保存する)。
    async fn segment(
        &self,
        t: Uuid,
        d: Uuid,
        day: &str,
        unko_no: &str,
        idx: i32,
        start: &str,
        end: &str,
        mins: (i32, i32, i32),
    ) {
        let (work, drive, cargo) = mins;
        let sql = format!(
            "INSERT INTO dtako_daily_work_segments (tenant_id, driver_id, work_date, unko_no, segment_index, start_at, \
             end_at, work_minutes, labor_minutes, drive_minutes, cargo_minutes) \
             VALUES ($1, '{d}', DATE '{day}', '{unko_no}', {idx}, TIMESTAMPTZ '{start}+00', TIMESTAMPTZ '{end}+00', \
             {work}, {work}, {drive}, {cargo})"
        );
        self.exec(t, &sql).await;
    }

    /// 日別 1 行 (拘束・運転・荷役・休息・深夜・重複の拘束・時間外深夜)。
    async fn daily(&self, t: Uuid, d: Uuid, day: &str, start: &str, v: [i32; 7]) {
        let [total, drive, cargo, rest, late, overlap, ot_late] = v;
        let sql = format!(
            "INSERT INTO dtako_daily_work_hours (tenant_id, driver_id, work_date, start_time, total_work_minutes, \
             drive_minutes, cargo_minutes, total_rest_minutes, late_night_minutes, overlap_restraint_minutes, \
             ot_late_night_minutes, total_distance, operation_count, unko_nos) \
             VALUES ($1, '{d}', DATE '{day}', TIME '{start}', {total}, {drive}, {cargo}, {rest}, {late}, {overlap}, \
             {ot_late}, 0, 1, ARRAY['X'])"
        );
        self.exec(t, &sql).await;
    }

    /// 運行 1 行 (運行日と出発)。
    async fn operation(&self, t: Uuid, d: Uuid, unko_no: &str, day: &str, departure: &str) {
        let sql = format!(
            "INSERT INTO dtako_operations (tenant_id, unko_no, crew_role, reading_date, operation_date, driver_id, departure_at) \
             VALUES ($1, '{unko_no}', 1, DATE '{day}', DATE '{day}', '{d}', TIMESTAMPTZ '{departure}+00')"
        );
        self.exec(t, &sql).await;
    }

    async fn call(&self, t: Uuid, req: Request<Body>) -> (StatusCode, String) {
        let res = self.app(t).oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await;
        (status, String::from_utf8(bytes.unwrap().to_vec()).unwrap())
    }

    async fn get(&self, t: Uuid, path: &str) -> (StatusCode, String) {
        let req = Request::builder().method(Method::GET).uri(path);
        self.call(t, req.body(Body::empty()).unwrap()).await
    }

    async fn get_json(&self, t: Uuid, path: &str) -> Value {
        let (status, body) = self.get(t, path).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        serde_json::from_str(&body).unwrap()
    }

    async fn post_csv(&self, t: Uuid, query: &str, body: Vec<u8>) -> (StatusCode, String) {
        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("/restraint-report/compare-csv{query}"))
            .header(
                "content-type",
                format!("multipart/form-data; boundary={BOUNDARY}"),
            );
        self.call(t, req.body(Body::from(body)).unwrap()).await
    }
}

/// multipart/form-data の body (field 1 つ)。
fn multipart_body(bytes: &[u8]) -> Vec<u8> {
    let mut body = format!(
        "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"r.csv\"\r\nContent-Type: text/csv\r\n\r\n"
    )
    .into_bytes();
    body.extend_from_slice(bytes);
    body.extend_from_slice(format!("\r\n--{BOUNDARY}--\r\n").as_bytes());
    body
}

/// 拘束時間管理表の CSV (乗務員 1 人)。日の行は `(日付, 始業以降の欄)`。
fn csv(name: &str, cd: &str, days: &[&str]) -> String {
    let mut s = format!(
        "拘束時間管理表 (2026年 2月分)\n\n氏名,{name},乗務員コード,{cd}\n\
         日付,始業時刻,終業時刻,運転時間,重複運転時間,荷役時間,重複荷役時間,休憩時間,重複休憩時間,時間,重複時間,拘束時間小計,重複拘束時間小計,拘束時間合計,拘束時間累計,前運転平均,後運転平均,休息時間,実働時間,時間外時間,深夜時間,時間外深夜時間,摘要1,摘要2\n"
    );
    for d in days {
        s.push_str(d);
        s.push('\n');
    }
    s.push_str("合計,,,1:00,,,,,,,,1:00,,,,,,,,,,,,\n");
    s
}

/// 1 か月の帳票を DB から組み立てる。名前・区間・日別 (同日 2 行を含む)・運行の始業と終業・前日の運転・年度の累計。
#[tokio::test(flavor = "multi_thread")]
async fn restraint_report_reads_the_month() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("restraint-month").await;
    let d = ctx.driver(t, "R1", "テスト 拘束").await;

    // 前日 (1/31) の最初の区間の運転 = 前運転平均の 1 日目に使う
    ctx.segment(
        t,
        d,
        "2026-01-31",
        "P1",
        0,
        "2026-01-31 08:00",
        "2026-01-31 09:00",
        (60, 100, 0),
    )
    .await;
    // 年度 (2025-04 〜 2026-01) の累計
    ctx.daily(t, d, "2025-05-10", "08:00", [600, 0, 0, 0, 0, 0, 0])
        .await;
    // 2/2 (月): 区間 2 本 (運行NO 2 つ)、日別なし → 区間から 1 行。始業は運行の出発、終業は運行の在る運行NO の区間の最後の終わり
    ctx.segment(
        t,
        d,
        "2026-02-02",
        "U1",
        0,
        "2026-02-02 06:10",
        "2026-02-02 10:00",
        (230, 120, 30),
    )
    .await;
    ctx.segment(
        t,
        d,
        "2026-02-02",
        "U2",
        0,
        "2026-02-02 11:00",
        "2026-02-02 15:45",
        (285, 200, 40),
    )
    .await;
    ctx.operation(t, d, "U1", "2026-02-02", "2026-02-02 06:05")
        .await;
    // 2/9 (月): 日別 2 行 (始業の違う運行) → 2 行。始業は日別の行の start_time、終業は 1 行目だけ
    ctx.segment(
        t,
        d,
        "2026-02-09",
        "U3",
        0,
        "2026-02-09 04:00",
        "2026-02-09 13:00",
        (540, 400, 60),
    )
    .await;
    ctx.daily(t, d, "2026-02-09", "04:00", [540, 400, 60, 600, 30, 15, 10])
        .await;
    ctx.daily(t, d, "2026-02-09", "22:30", [120, 90, 0, 0, 0, 0, 0])
        .await;

    let path = format!("/restraint-report?driver_id={d}&year=2026&month=2");
    let v = ctx.get_json(t, &path).await;
    assert_eq!(v["driver_id"], json!(d.to_string()));
    assert_eq!(v["driver_name"], "テスト 拘束");
    assert_eq!(
        (v["year"].clone(), v["month"].clone()),
        (json!(2026), json!(2))
    );
    assert_eq!(v["max_restraint_minutes"], 16500);
    let days = v["days"].as_array().unwrap();
    assert_eq!(days.len(), 29); // 28 日 + 2/9 の 2 行目

    // 2/1 (日) は休み。前日の運転は休みで 0 に戻る
    assert_eq!(days[0]["is_holiday"], true);
    assert_eq!(days[0]["remarks"], "休");

    let feb2 = &days[1];
    assert_eq!(feb2["date"], "2026-02-02");
    assert_eq!(feb2["start_time"], "6:05");
    // 終業は運行の在る運行NO (U1) の区間の最後の終わり。U2 は運行が無いので数えない (rust と同じ JOIN)
    assert_eq!(feb2["end_time"], "10:00");
    assert_eq!(feb2["drive_minutes"], 320);
    assert_eq!(feb2["cargo_minutes"], 70);
    assert_eq!(feb2["restraint_main_minutes"], 515);
    assert_eq!(feb2["break_minutes"], 125);
    assert_eq!(feb2["drive_avg_before"], 160);
    assert_eq!(feb2["drive_average_minutes"], 160.0);
    assert_eq!(feb2["overtime_minutes"], 0);
    assert_eq!(feb2["rest_period_minutes"], Value::Null);
    assert_eq!(
        feb2["operations"],
        json!([
            {"unko_no": "U1", "drive_minutes": 120, "cargo_minutes": 30, "break_minutes": 80, "restraint_minutes": 230},
            {"unko_no": "U2", "drive_minutes": 200, "cargo_minutes": 40, "break_minutes": 45, "restraint_minutes": 285}
        ])
    );

    let feb9 = &days[8];
    assert_eq!(feb9["date"], "2026-02-09");
    assert_eq!(feb9["start_time"], "4:00");
    assert_eq!(feb9["end_time"], "13:00");
    assert_eq!(feb9["drive_minutes"], 400);
    assert_eq!(feb9["restraint_total_minutes"], 555);
    assert_eq!(feb9["rest_period_minutes"], 600);
    assert_eq!(feb9["late_night_minutes"], 30);
    assert_eq!(feb9["actual_work_minutes"], 460);
    assert_eq!(feb9["overtime_minutes"], 0);
    assert_eq!(feb9["overtime_late_night_minutes"], 10);
    let feb9b = &days[9];
    assert_eq!(feb9b["date"], "2026-02-09");
    assert_eq!(feb9b["start_time"], "22:30");
    assert_eq!(feb9b["end_time"], Value::Null);
    assert_eq!(feb9b["operations"], json!([]));
    assert_eq!(feb9b["drive_avg_before"], 245);

    assert_eq!(
        v["weekly_subtotals"],
        json!([
            {"week_end_date": "2026-02-07", "drive_minutes": 320, "cargo_minutes": 70, "break_minutes": 125, "restraint_minutes": 515},
            {"week_end_date": "2026-02-14", "drive_minutes": 490, "cargo_minutes": 60, "break_minutes": 110, "restraint_minutes": 660}
        ])
    );
    let total = &v["monthly_total"];
    assert_eq!(total["restraint_minutes"], 1175);
    assert_eq!(total["fiscal_year_cumulative_minutes"], 600);
    assert_eq!(total["fiscal_year_total_minutes"], 1775);
    assert_eq!(total["overlap_restraint_minutes"], 15);

    // 4 月は年度の累計を読まない (0)。乗務員が居なければ名前は空のまま 200
    let other = Uuid::new_v4();
    let v = ctx
        .get_json(
            t,
            &format!("/restraint-report?driver_id={other}&year=2026&month=4"),
        )
        .await;
    assert_eq!(v["driver_name"], "");
    assert_eq!(v["days"].as_array().unwrap().len(), 30);
    assert_eq!(v["weekly_subtotals"], json!([]));
    assert_eq!(v["monthly_total"]["fiscal_year_cumulative_minutes"], 0);
    ctx.finish().await;
}

/// query の欠け・日付にならない年月は 400 (本文は rust と同じ)。
#[tokio::test(flavor = "multi_thread")]
async fn restraint_report_rejects_bad_queries() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("restraint-bad").await;
    let d = Uuid::new_v4();
    let (status, _) = ctx.get(t, "/restraint-report?year=2026&month=2").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, body) = ctx
        .get(
            t,
            &format!("/restraint-report?driver_id={d}&year=2026&month=13"),
        )
        .await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::BAD_REQUEST, "invalid year/month")
    );
    ctx.finish().await;
}

/// DB の失敗は 500 で本文は `internal server error` (rust と同じ)。ログは段の名前と kind だけ。
#[tokio::test(flavor = "multi_thread")]
async fn restraint_report_db_failures_are_500() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("restraint-db-fail").await;
    let d = ctx.driver(t, "R1", "テスト 拘束").await;
    let path = format!("/restraint-report?driver_id={d}&year=2026&month=2");
    let ctx = ctx
        .run_as_superuser("REVOKE SELECT ON alc_api.dtako_daily_work_segments FROM alc_api_app")
        .await;
    let (status, body) = ctx.get(t, &path).await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
    );
    assert_eq!(
        ctx.logs.all().pop().unwrap(),
        (
            LogLevel::Error,
            "restraint-report inputs failed: db (42501)".to_owned()
        )
    );
    let ctx = ctx
        .run_as_superuser("REVOKE SELECT ON alc_api.employees FROM alc_api_app")
        .await;
    let (status, _) = ctx.get(t, &path).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        ctx.logs.all().pop().unwrap(),
        (
            LogLevel::Error,
            "restraint-report driver failed: db (42501)".to_owned()
        )
    );
    let (status, body) = ctx
        .post_csv(
            t,
            "",
            multipart_body(csv("テスト", "R1", &["2月1日,休,"]).as_bytes()),
        )
        .await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
    );
    assert_eq!(
        ctx.logs.all().pop().unwrap(),
        (
            LogLevel::Error,
            "restraint-report drivers failed: db (42501)".to_owned()
        )
    );
    ctx.finish().await;
}

/// CSV の乗務員ごとに、乗務員CD の当たる乗務員の帳票と比べる。当たらない乗務員は system なし、`driver_cd` で絞れる。
#[tokio::test(flavor = "multi_thread")]
async fn compare_csv_matches_drivers() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("restraint-compare").await;
    let d = ctx.driver(t, "C1", "テスト 比較").await;
    ctx.segment(
        t,
        d,
        "2026-02-02",
        "U1",
        0,
        "2026-02-02 06:00",
        "2026-02-02 07:00",
        (60, 60, 0),
    )
    .await;
    ctx.operation(t, d, "U1", "2026-02-02", "2026-02-02 06:00")
        .await;

    // 2/2 の運転は CSV 1:30・システム 1:00 で差分 1 件 (小計・合計・累計・実働も 1:00 で一致させる)
    let body = format!(
        "{}{}",
        csv(
            "テスト 比較",
            "C1",
            &[
                "2月1日,休,",
                "2月2日,6:00,7:00,1:30,,,,,,,,1:00,,1:00,1:00,,,,1:00,,,,,"
            ]
        ),
        csv("未登録 乗務員", "C9", &["2月1日,休,"]),
    );
    let (status, res) = ctx.post_csv(t, "", multipart_body(body.as_bytes())).await;
    assert_eq!(status, StatusCode::OK, "{res}");
    let v: Value = serde_json::from_str(&res).unwrap();
    let rows = v.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["driver_cd"], "C1");
    assert_eq!(rows[0]["driver_id"], json!(d.to_string()));
    assert_eq!(rows[0]["csv"]["days"].as_array().unwrap().len(), 2);
    assert_eq!(rows[0]["system"]["days"].as_array().unwrap().len(), 28);
    assert_eq!(rows[0]["system"]["total_drive"], "1:00");
    assert_eq!(
        rows[0]["diffs"],
        json!([{"date": "2月2日", "field": "運転", "csv_val": "1:30", "sys_val": "1:00", "known_bug": null}])
    );
    assert_eq!(
        (
            rows[0]["known_bug_diffs"].clone(),
            rows[0]["unknown_diffs"].clone()
        ),
        (json!(0), json!(1))
    );
    assert_eq!(rows[1]["driver_cd"], "C9");
    assert_eq!(rows[1]["driver_id"], Value::Null);
    assert_eq!(rows[1]["system"], Value::Null);
    assert_eq!(rows[1]["diffs"], json!([]));

    let (status, res) = ctx
        .post_csv(t, "?driver_cd=C9", multipart_body(body.as_bytes()))
        .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&res).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 1);
    assert_eq!(v[0]["driver_cd"], "C9");

    // CSV の月が日付にならない (13 月) と帳票が組めず、当たった乗務員でも system なし
    let body13 = csv("テスト 比較", "C1", &["13月1日,休,"]);
    let (status, res) = ctx.post_csv(t, "", multipart_body(body13.as_bytes())).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&res).unwrap();
    assert_eq!(v[0]["driver_id"], json!(d.to_string()));
    assert_eq!(v[0]["system"], Value::Null);
    ctx.finish().await;
}

/// CSV が無い・空・読めない・乗務員の無い CSV は 400 (本文は rust と同じ)。
#[tokio::test(flavor = "multi_thread")]
async fn compare_csv_rejects_bad_uploads() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("restraint-compare-bad").await;
    // field が 1 つも無い
    let (status, body) = ctx
        .post_csv(t, "", format!("--{BOUNDARY}--\r\n").into_bytes())
        .await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::BAD_REQUEST, "CSVファイルが空です")
    );
    // field はあるが中身が空
    let (status, body) = ctx.post_csv(t, "", multipart_body(b"")).await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::BAD_REQUEST, "CSVファイルが空です")
    );
    // 乗務員の行が無い
    let (status, body) = ctx.post_csv(t, "", multipart_body(b"a,b\n")).await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::BAD_REQUEST, "ドライバーが見つかりません")
    );
    // 頭の壊れた multipart
    let (status, body) = ctx
        .post_csv(
            t,
            "",
            format!("--{BOUNDARY}\r\nbroken\r\n\r\n").into_bytes(),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.starts_with("multipart error: "), "{body}");
    // 中身の途中で切れた multipart
    let (status, body) = ctx
        .post_csv(
            t,
            "",
            format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\nabc")
                .into_bytes(),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.starts_with("field read error: "), "{body}");
    ctx.finish().await;
}
