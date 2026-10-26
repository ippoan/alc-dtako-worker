//! 画面向けの読み取りと分類の更新の口 (W5c-1) を、口から DB・保存先まで通して確かめる (native)。
//!
//! DB はテストの process の中で起こす組み込みの PostgreSQL (`embedded/mod.rs`)、保存先・ログは偽物 (`fakes/mod.rs`)。
//! 期待値は rust-alc-api の同じ口 (`alc_dtako::dtako_*`) の応答。乗務員CD・名前・運行NO は作り物。

// 共用の土台・偽物のうち、このファイルが使わないものが在る
#[allow(dead_code)]
mod embedded;
#[allow(dead_code)]
mod fakes;

use std::io::Write;
use std::sync::Arc;

use alc_core_wasm::TenantId;
use alc_dtako_upload::routes::{tenant_router, DtakoState};
use alc_dtako_upload::screen::ARCHIVE_MANIFEST_KEY;
use alc_dtako_upload::split::LogLevel;
use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::{Extension, Router};
use embedded::{employee, exec, rows_json, tenant, Embedded, Held, APP_ROLE};
use fakes::{FakeClock, FakeSleeper, FakeStore, Logs};
use futures_util::lock::Mutex;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

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

    async fn driver(&self, t: Uuid, cd: Option<&str>, name: &str, deleted: bool) -> Uuid {
        let mut c = self.pg.inner.lock().await;
        employee(&mut c, t, None, cd, name, deleted).await
    }

    /// `$1` = tenant_id の文を流す (値はテストの固定値を文に直に書く)。
    async fn exec(&self, t: Uuid, sql: &str) -> u64 {
        let mut c = self.pg.inner.lock().await;
        exec(&mut c, t, sql).await
    }

    async fn rows(&self, t: Uuid, query: &str) -> Vec<Value> {
        let mut c = self.pg.inner.lock().await;
        rows_json(&mut c, t, query).await
    }

    async fn call(
        &self,
        t: Uuid,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, String) {
        let mut req = Request::builder().method(method).uri(path);
        let body = match body {
            Some(v) => {
                req = req.header("content-type", "application/json");
                Body::from(v.to_string())
            }
            None => Body::empty(),
        };
        let res = self.app(t).oneshot(req.body(body).unwrap()).await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await;
        (status, String::from_utf8(bytes.unwrap().to_vec()).unwrap())
    }

    async fn get(&self, t: Uuid, path: &str) -> (StatusCode, String) {
        self.call(t, Method::GET, path, None).await
    }

    async fn get_json(&self, t: Uuid, path: &str) -> Value {
        let (status, body) = self.get(t, path).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        serde_json::from_str(&body).unwrap()
    }
}

/// 運行 1 行 (乗務員・車両は在れば)。日時は `+09` 付きの文字列。
#[allow(clippy::too_many_arguments)]
fn op_sql(
    unko_no: &str,
    crew_role: i32,
    reading: &str,
    driver: Option<Uuid>,
    vehicle: Option<Uuid>,
    departure: Option<&str>,
    prefix: Option<&str>,
) -> String {
    let q = |v: Option<String>| v.map_or("NULL".to_owned(), |v| format!("'{v}'"));
    format!(
        "INSERT INTO dtako_operations (tenant_id, unko_no, crew_role, reading_date, operation_date, driver_id, vehicle_id, \
         departure_at, total_distance, safety_score, has_kudgivt, r2_key_prefix, raw_data) \
         VALUES ($1, '{unko_no}', {crew_role}, DATE '{reading}', DATE '{reading}', {}, {}, {}, 12.5, 80.0, TRUE, {}, \
         '{{\"k\": 1}}')",
        q(driver.map(|d| d.to_string())),
        q(vehicle.map(|v| v.to_string())),
        departure.map_or("NULL".to_owned(), |d| format!("TIMESTAMPTZ '{d}'")),
        q(prefix.map(str::to_owned)),
    )
}

/// 乗務員・車両・日別・セグメントの一覧: テナントで絞り、ページの番号と件数を整える。
#[tokio::test(flavor = "multi_thread")]
async fn lists_drivers_vehicles_and_daily_pages() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("screen-lists").await;
    let other = ctx.tenant("screen-lists-other").await;
    let d2 = ctx.driver(t, Some("D2"), "TEST-DRIVER TWO", false).await;
    let d1 = ctx.driver(t, Some("D1"), "TEST-DRIVER ONE", false).await;
    let idle = ctx.driver(t, Some("D3"), "TEST-DRIVER IDLE", false).await;
    let gone = ctx.driver(t, Some("D4"), "TEST-DRIVER GONE", true).await;
    let o1 = ctx.driver(other, Some("D1"), "TEST-OTHER", false).await;
    for (unko_no, d) in [
        ("2603010800", d2),
        ("2603010900", d1),
        ("2603011000", d1),
        ("2603011100", gone),
    ] {
        let sql = op_sql(unko_no, 1, "2026-03-01", Some(d), None, None, None);
        assert_eq!(ctx.exec(t, &sql).await, 1);
    }
    let sql = op_sql("2603011200", 1, "2026-03-01", Some(o1), None, None, None);
    assert_eq!(ctx.exec(other, &sql).await, 1);
    let _ = idle;

    // 運行の在る、論理削除されていない乗務員だけ (乗務員CD の順、重複なし)
    let drivers = ctx.get_json(t, "/drivers").await;
    assert_eq!(
        drivers,
        json!([
            { "id": d1, "tenant_id": t, "driver_cd": "D1", "driver_name": "TEST-DRIVER ONE" },
            { "id": d2, "tenant_id": t, "driver_cd": "D2", "driver_name": "TEST-DRIVER TWO" },
        ])
    );

    ctx.exec(t, "INSERT INTO dtako_vehicles (tenant_id, vehicle_cd, vehicle_name) VALUES ($1, 'V2', 'TEST-TRUCK 2'), ($1, 'V1', 'TEST-TRUCK 1')").await;
    ctx.exec(other, "INSERT INTO dtako_vehicles (tenant_id, vehicle_cd, vehicle_name) VALUES ($1, 'V0', 'TEST-OTHER')").await;
    let vehicles = ctx.get_json(t, "/vehicles").await;
    let cds: Vec<&str> = vehicles
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["vehicle_cd"].as_str().unwrap())
        .collect();
    assert_eq!(cds, ["V1", "V2"]);
    assert_eq!(vehicles[0]["vehicle_name"], "TEST-TRUCK 1");
    assert_eq!(vehicles[0]["tenant_id"], json!(t));

    // 日別の労働時間 3 日 (d1 が 2 日、d2 が 1 日)
    for (d, day) in [(d1, "2026-03-02"), (d1, "2026-03-01"), (d2, "2026-03-03")] {
        let sql = format!(
            "INSERT INTO dtako_daily_work_hours (tenant_id, driver_id, work_date, start_time, total_work_minutes, \
             drive_minutes, total_distance, operation_count, unko_nos) \
             VALUES ($1, '{d}', DATE '{day}', TIME '05:30', 480, 300, 101.5, 2, ARRAY['A','B'])"
        );
        assert_eq!(ctx.exec(t, &sql).await, 1);
    }
    let page = ctx.get_json(t, "/daily-hours").await;
    assert_eq!(
        (
            page["total"].clone(),
            page["page"].clone(),
            page["per_page"].clone()
        ),
        (json!(3), json!(1), json!(50))
    );
    let days: Vec<&str> = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["work_date"].as_str().unwrap())
        .collect();
    assert_eq!(days, ["2026-03-01", "2026-03-02", "2026-03-03"]);
    let first = &page["items"][0];
    assert_eq!(first["start_time"], "05:30:00");
    assert_eq!(first["total_work_minutes"], 480);
    assert_eq!(first["total_rest_minutes"], Value::Null);
    assert_eq!(first["total_distance"], 101.5);
    assert_eq!(first["unko_nos"], json!(["A", "B"]));
    assert_eq!(first["late_night_minutes"], 0);
    // 絞り込みとページ (page 0 は 1、per_page は 200 で切る)
    let q = format!(
        "/daily-hours?driver_id={d1}&date_from=2026-03-02&date_to=2026-03-31&page=0&per_page=999"
    );
    let page = ctx.get_json(t, &q).await;
    assert_eq!(
        (
            page["total"].clone(),
            page["page"].clone(),
            page["per_page"].clone()
        ),
        (json!(1), json!(1), json!(200))
    );
    let page = ctx.get_json(t, "/daily-hours?per_page=2&page=2").await;
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["items"][0]["work_date"], "2026-03-03");

    // セグメント (日付・乗務員CD・始まりの順)
    for (d, day, unko_no, start) in [
        (d2, "2026-03-01", "S3", "2026-03-01 06:00+09"),
        (d1, "2026-03-01", "S2", "2026-03-01 09:00+09"),
        (d1, "2026-03-01", "S1", "2026-03-01 07:00+09"),
    ] {
        let sql = format!(
            "INSERT INTO dtako_daily_work_segments (tenant_id, driver_id, work_date, unko_no, segment_index, start_at, \
             end_at, work_minutes, labor_minutes, drive_minutes) \
             VALUES ($1, '{d}', DATE '{day}', '{unko_no}', 0, TIMESTAMPTZ '{start}', TIMESTAMPTZ '{start}' + interval '1 hour', 60, 50, 40)"
        );
        assert_eq!(ctx.exec(t, &sql).await, 1);
    }
    let page = ctx.get_json(t, "/work-times").await;
    let order: Vec<&str> = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["unko_no"].as_str().unwrap())
        .collect();
    assert_eq!(order, ["S1", "S2", "S3"]);
    assert_eq!(page["items"][0]["start_at"], "2026-02-28T22:00:00Z");
    assert_eq!(page["items"][0]["labor_minutes"], 50);
    assert!(
        page["items"][0].get("drive_minutes").is_none(),
        "work-times は要約の欄だけ"
    );
    let page = ctx
        .get_json(t, &format!("/work-times?driver_id={d2}"))
        .await;
    assert_eq!(page["total"], 1);

    let path = format!("/daily-hours/{d1}/2026-03-01/segments");
    let segs = ctx.get_json(t, &path).await;
    let order: Vec<&str> = segs["segments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["unko_no"].as_str().unwrap())
        .collect();
    assert_eq!(order, ["S1", "S2"]);
    assert_eq!(segs["segments"][0]["drive_minutes"], 40);
    assert_eq!(segs["segments"][0]["tenant_id"], json!(t));
    // 他のテナントからは見えない
    assert_eq!(ctx.get_json(other, &path).await, json!({ "segments": [] }));
    ctx.finish().await;
}

/// 運行の一覧・カレンダー・1 件・手で消す (変更記録に残す)。
#[tokio::test(flavor = "multi_thread")]
async fn operations_list_calendar_get_and_delete() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("screen-ops").await;
    let other = ctx.tenant("screen-ops-other").await;
    let d1 = ctx.driver(t, Some("D1"), "TEST-DRIVER ONE", false).await;
    ctx.exec(t, "INSERT INTO dtako_vehicles (tenant_id, vehicle_cd, vehicle_name) VALUES ($1, 'V1', 'TEST-TRUCK')").await;
    let v1: Uuid = ctx
        .rows(t, "SELECT id FROM dtako_vehicles WHERE tenant_id = $1")
        .await[0]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    for sql in [
        op_sql(
            "2603010800",
            2,
            "2026-03-01",
            Some(d1),
            Some(v1),
            Some("2026-03-01 08:00+09"),
            None,
        ),
        op_sql(
            "2603010800",
            1,
            "2026-03-01",
            Some(d1),
            Some(v1),
            Some("2026-03-01 08:00+09"),
            None,
        ),
        op_sql("2603020800", 1, "2026-03-02", None, None, None, None),
        op_sql("2602280800", 1, "2026-02-28", Some(d1), None, None, None),
    ] {
        assert_eq!(ctx.exec(t, &sql).await, 1);
    }
    ctx.exec(
        other,
        &op_sql("2603019999", 1, "2026-03-01", None, None, None, None),
    )
    .await;

    // 読取日の新しい順・運行NO の順、乗務員と車両は LEFT JOIN
    let list = ctx.get_json(t, "/operations").await;
    assert_eq!(
        (
            list["total"].clone(),
            list["page"].clone(),
            list["per_page"].clone()
        ),
        (json!(4), json!(1), json!(50))
    );
    let order: Vec<(String, i64)> = list["operations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| {
            (
                o["unko_no"].as_str().unwrap().to_owned(),
                o["crew_role"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(order[0], ("2603020800".to_owned(), 1));
    assert_eq!(order[3], ("2602280800".to_owned(), 1));
    assert_eq!(list["operations"][0]["driver_name"], Value::Null);
    let first_with_vehicle = &list["operations"][1];
    assert_eq!(first_with_vehicle["vehicle_cd"], "V1");
    assert_eq!(first_with_vehicle["vehicle_name"], "TEST-TRUCK");
    assert_eq!(first_with_vehicle["driver_name"], "TEST-DRIVER ONE");
    assert_eq!(first_with_vehicle["total_distance"], 12.5);
    assert_eq!(first_with_vehicle["economy_score"], Value::Null);
    assert_eq!(first_with_vehicle["has_kudgivt"], true);
    assert_eq!(first_with_vehicle["operation_date"], "2026-03-01");
    // 絞り込み (内部用の口も同じ)
    let q = "?date_from=2026-03-01&date_to=2026-03-01&driver_cd=D1&vehicle_cd=V1&per_page=1&page=2";
    for base in ["/operations", "/internal/operations"] {
        let list = ctx.get_json(t, &format!("{base}{q}")).await;
        assert_eq!(list["total"], 2, "{base}");
        assert_eq!(list["operations"][0]["crew_role"], 2, "{base}");
    }

    // カレンダー (読取日ごとの件数。12 月は翌年の 1 月の前日まで、13 月は 400)
    let cal = ctx
        .get_json(t, "/operations/calendar?year=2026&month=3")
        .await;
    assert_eq!(
        cal,
        json!({ "year": 2026, "month": 3, "dates": [
            { "date": "2026-03-01", "count": 2 },
            { "date": "2026-03-02", "count": 1 },
        ] })
    );
    let dec = ctx
        .get_json(t, "/operations/calendar?year=2025&month=12")
        .await;
    assert_eq!(dec["dates"], json!([]));
    assert_eq!(
        ctx.get(t, "/operations/calendar?year=2026&month=13")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    // 翌月の 1 日が作れない (日付の上限の年の 12 月) も 400
    assert_eq!(
        ctx.get(t, "/operations/calendar?year=262142&month=12")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        ctx.get(t, "/operations/calendar?year=2026").await.0,
        StatusCode::BAD_REQUEST
    );

    // 1 件 (crew_role の順、全欄)。無ければ 404
    let ops = ctx.get_json(t, "/operations/2603010800").await;
    assert_eq!(ops.as_array().unwrap().len(), 2);
    assert_eq!(ops[0]["crew_role"], 1);
    assert_eq!(ops[0]["raw_data"], json!({ "k": 1 }));
    assert_eq!(ops[0]["departure_at"], "2026-02-28T23:00:00Z");
    assert_eq!(ops[0]["vehicle_id"], json!(v1));
    assert_eq!(ops[0]["office_id"], Value::Null);
    assert!(ops[0]["uploaded_at"].is_string());
    assert_eq!(
        ctx.get(t, "/operations/NOPE").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        ctx.get(other, "/operations/2603010800").await.0,
        StatusCode::NOT_FOUND
    );

    // 手で消す: 全 crew_role を消し、消す前の行を crew_role ごとに記録する。無ければ 404
    let (status, body) = ctx
        .call(t, Method::DELETE, "/operations/2603010800", None)
        .await;
    assert_eq!((status, body.as_str()), (StatusCode::NO_CONTENT, ""));
    assert_eq!(
        ctx.get(t, "/operations/2603010800").await.0,
        StatusCode::NOT_FOUND
    );
    let changes = ctx
        .rows(t, "SELECT crew_role, driver_cd, reason, upload_id, before, after FROM dtako_operation_changes WHERE tenant_id = $1 ORDER BY crew_role")
        .await;
    let before =
        json!({ "driver_cd": "D1", "departure_at": "2026-02-28T23:00:00Z", "return_at": null });
    assert_eq!(
        changes,
        [
            json!({ "crew_role": 1, "driver_cd": "D1", "reason": "manual_delete", "upload_id": null, "before": before, "after": null }),
            json!({ "crew_role": 2, "driver_cd": "D1", "reason": "manual_delete", "upload_id": null, "before": before, "after": null }),
        ]
    );
    assert_eq!(
        ctx.call(t, Method::DELETE, "/operations/2603010800", None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    ctx.finish().await;
}

/// 運行の CSV: 接頭辞が在ればその下、無ければ `{tenant}/unko/{unko_no}/`。種類の別名・空の行・読めない・無い。
#[tokio::test(flavor = "multi_thread")]
async fn operation_csv_reads_store_as_json() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("screen-csv").await;
    ctx.exec(
        t,
        &op_sql(
            "U1",
            1,
            "2026-03-01",
            None,
            None,
            None,
            Some("custom/prefix"),
        ),
    )
    .await;
    ctx.store.seed(
        "custom/prefix/KUDGIVT.csv",
        "運行NO, イベント名\r\nU1, 運転 \r\n\r\nU1,休憩\r\n"
            .as_bytes()
            .to_vec(),
        "text/csv",
    );
    ctx.store.seed(
        &format!("{t}/unko/U2/SOKUDODATA.csv"),
        b"a,b\n1,2\n".to_vec(),
        "text/csv",
    );

    assert_eq!(
        ctx.get_json(t, "/operations/U1/csv/KUDGIVT").await["rows"][0][0],
        "U1"
    );
    let csv = ctx.get_json(t, "/operations/U1/csv/Events").await;
    assert_eq!(
        csv,
        json!({ "headers": ["運行NO", "イベント名"], "rows": [["U1", "運転"], ["U1", "休憩"]] })
    );
    // 運行が無ければ既定の key
    let csv = ctx.get_json(t, "/operations/U2/csv/sokudo").await;
    assert_eq!(csv, json!({ "headers": ["a", "b"], "rows": [["1", "2"]] }));
    for kind in [
        "kudguri", "kudgfry", "ferry", "ferries", "kudgsir", "tolls", "speed",
    ] {
        assert_eq!(
            ctx.get(t, &format!("/operations/U1/csv/{kind}")).await.0,
            StatusCode::NOT_FOUND,
            "{kind}"
        );
    }
    assert_eq!(
        ctx.get(t, "/operations/U1/csv/zip").await.0,
        StatusCode::BAD_REQUEST
    );
    // 保存先が読めないのも 404 (段の名前だけをログに)
    ctx.store.fail_gets_containing("custom/prefix", 1);
    assert_eq!(
        ctx.get(t, "/operations/U1/csv/events").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        ctx.logs.all().pop().unwrap(),
        (
            LogLevel::Error,
            "operation-csv failed: store (get)".to_owned()
        )
    );
    ctx.finish().await;
}

/// 変更記録: 乗務員CD (列・before・after のどれか) で引き、期間は運行の日付 (運行NO の先頭 6 桁、読めなければ出発日)。
#[tokio::test(flavor = "multi_thread")]
async fn operation_changes_filter_by_operation_date() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("screen-changes").await;
    let other = ctx.tenant("screen-changes-other").await;
    let empty = ctx
        .get_json(
            t,
            "/dtako/operation-changes?driver_cd=D1&from=2026-03-01&to=2026-03-31",
        )
        .await;
    assert_eq!(
        empty,
        json!({ "driver_cd": "D1", "from": "2026-03-01", "to": "2026-03-31", "recording_since": null, "changes": [] })
    );
    for (unko_no, driver_cd, before, after, at) in [
        (
            "260305080000",
            "'D1'",
            "NULL",
            "NULL",
            "2026-04-01 00:00+00",
        ),
        (
            "260410080000",
            "'D1'",
            "NULL",
            "NULL",
            "2026-04-11 00:00+00",
        ),
        (
            "XYZ",
            "NULL",
            r#"'{"driver_cd":"D1","departure_at":"2026-03-09T23:00:00Z"}'"#,
            "NULL",
            "2026-03-10 00:00+00",
        ),
        (
            "XYZ2",
            "NULL",
            "NULL",
            r#"'{"driver_cd":"D1","departure_at":"bad"}'"#,
            "2026-03-10 00:00+00",
        ),
        (
            "260306080000",
            "'D2'",
            "NULL",
            "NULL",
            "2026-03-06 00:00+00",
        ),
    ] {
        let sql = format!(
            "INSERT INTO dtako_operation_changes (tenant_id, unko_no, crew_role, driver_cd, reason, before, after, recorded_at) \
             VALUES ($1, '{unko_no}', 1, {driver_cd}, 'reupload', {before}, {after}, TIMESTAMPTZ '{at}')"
        );
        assert_eq!(ctx.exec(t, &sql).await, 1);
    }
    ctx.exec(other, "INSERT INTO dtako_operation_changes (tenant_id, unko_no, crew_role, driver_cd, reason, recorded_at) VALUES ($1, '260301000000', 1, 'D1', 'reupload', TIMESTAMPTZ '2020-01-01 00:00+00')").await;

    let res = ctx
        .get_json(
            t,
            "/dtako/operation-changes?driver_cd=D1&from=2026-03-01&to=2026-03-31",
        )
        .await;
    assert_eq!(res["recording_since"], "2026-03-06T00:00:00Z");
    let unko: Vec<&str> = res["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["unko_no"].as_str().unwrap())
        .collect();
    assert_eq!(unko, ["260305080000", "XYZ"]);
    assert_eq!(
        res["changes"][1]["before"]["departure_at"],
        "2026-03-09T23:00:00Z"
    );
    assert_eq!(res["changes"][0]["reason"], "reupload");
    assert_eq!(res["changes"][0]["after"], Value::Null);
    assert_eq!(
        ctx.get(
            t,
            "/dtako/operation-changes?driver_cd=D1&from=2026-03-31&to=2026-03-01"
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    ctx.finish().await;
}

/// イベントの分類の一覧と更新 (値の検査・無い id・他のテナント)。
#[tokio::test(flavor = "multi_thread")]
async fn event_classifications_list_and_update() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("screen-class").await;
    let other = ctx.tenant("screen-class-other").await;
    ctx.exec(t, "INSERT INTO dtako_event_classifications (tenant_id, event_cd, event_name, classification) VALUES ($1, '202', 'TEST-B', 'cargo'), ($1, '201', 'TEST-A', 'drive')").await;
    let list = ctx.get_json(t, "/event-classifications").await;
    let cds: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["event_cd"].as_str().unwrap())
        .collect();
    assert_eq!(cds, ["201", "202"]);
    assert_eq!(list[0]["event_name"], "TEST-A");
    assert!(list[0]["created_at"].is_string());
    let id = list[0]["id"].as_str().unwrap().to_owned();

    let (status, body) = ctx
        .call(
            t,
            Method::PUT,
            &format!("/event-classifications/{id}"),
            Some(json!({ "classification": "nap" })),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        "Invalid classification 'nap'. Must be one of: drive, cargo, rest_split, break, ignore"
    );
    let (status, body) = ctx
        .call(
            t,
            Method::PUT,
            &format!("/event-classifications/{id}"),
            Some(json!({ "classification": "break" })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let row: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        (row["event_cd"].clone(), row["classification"].clone()),
        (json!("201"), json!("break"))
    );
    // 無い id・他のテナントの id は 404
    for (tenant, id) in [(t, Uuid::new_v4().to_string()), (other, id.clone())] {
        let (status, body) = ctx
            .call(
                tenant,
                Method::PUT,
                &format!("/event-classifications/{id}"),
                Some(json!({ "classification": "drive" })),
            )
            .await;
        assert_eq!(
            (status, body.as_str()),
            (StatusCode::NOT_FOUND, "Not found")
        );
    }
    assert_eq!(
        ctx.get_json(t, "/event-classifications").await[0]["classification"],
        "break"
    );
    ctx.finish().await;
}

/// スクレイプの履歴の記録と一覧 (新しい順、既定 50 件、offset)。
#[tokio::test(flavor = "multi_thread")]
async fn scrape_history_save_and_list() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("screen-scrape").await;
    let (status, body) = ctx
        .call(
            t,
            Method::POST,
            "/scraper/history",
            Some(json!({ "target_date": "2026-03-01", "comp_id": "C1", "status": "ok" })),
        )
        .await;
    assert_eq!((status, body.as_str()), (StatusCode::NO_CONTENT, ""));
    ctx.exec(t, "INSERT INTO dtako_scrape_history (tenant_id, target_date, comp_id, status, message, created_at) VALUES ($1, DATE '2026-02-01', 'C0', 'error', 'TEST-MESSAGE', now() - interval '1 day')").await;
    let list = ctx.get_json(t, "/scraper/history").await;
    assert_eq!(list.as_array().unwrap().len(), 2);
    assert_eq!(
        (list[0]["comp_id"].clone(), list[0]["message"].clone()),
        (json!("C1"), Value::Null)
    );
    assert_eq!(list[1]["message"], "TEST-MESSAGE");
    assert_eq!(list[1]["target_date"], "2026-02-01");
    let list = ctx.get_json(t, "/scraper/history?limit=1&offset=1").await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["comp_id"], "C0");
    assert_eq!(
        ctx.call(
            t,
            Method::POST,
            "/scraper/history",
            Some(json!({ "comp_id": "C1" }))
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    ctx.finish().await;
}

/// 車両の現在地のログ 1 行。
fn log_sql(
    vehicle_cd: i32,
    at: &str,
    branch_cd: i32,
    area: &str,
    all_state: &str,
    speed: f32,
) -> String {
    format!(
        "INSERT INTO dtakologs (tenant_id, data_date_time, vehicle_cd, vehicle_name, driver_name, branch_cd, address_disp_p, \
         all_state, state2, speed, gps_latitude) \
         VALUES ($1, '{at}', {vehicle_cd}, 'TEST-TRUCK {vehicle_cd}', 'TEST-DRIVER', {branch_cd}, '{area}', '{all_state}', 'S2', {speed}, 35.5)"
    )
}

fn gz(text: &str) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(text.as_bytes()).unwrap();
    enc.finish().unwrap()
}

/// 車両の現在地のログ: 最新・絞り込み・日時・範囲 (保存先のアーカイブも読む)、画面の形 (PascalCase) への変換。
#[tokio::test(flavor = "multi_thread")]
async fn dtakologs_reads_db_and_archive() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("screen-logs").await;
    let other = ctx.tenant("screen-logs-other").await;
    for sql in [
        log_sql(
            1,
            "2026-03-02T08:00:00+09:00",
            10,
            "TEST-AREA-A",
            "Drive",
            74.9,
        ),
        log_sql(
            1,
            "2026-03-02T09:00:00+09:00",
            10,
            "TEST-AREA-A",
            "Rest",
            0.0,
        ),
        log_sql(
            2,
            "2026-03-02T08:30:00+09:00",
            20,
            "TEST-AREA-B",
            "Stop",
            30.0,
        ),
    ] {
        assert_eq!(ctx.exec(t, &sql).await, 1);
    }
    ctx.exec(
        other,
        &log_sql(
            3,
            "2026-03-02T10:00:00+09:00",
            10,
            "TEST-AREA-A",
            "Drive",
            1.0,
        ),
    )
    .await;

    let current = ctx.get_json(t, "/dtako-logs/current").await;
    assert_eq!(current.as_array().unwrap().len(), 2);
    assert_eq!(
        current[0],
        json!({
            "GPSDirection": 0.0, "GPSLatitude": 35.5, "GPSLongitude": 0.0, "VehicleCD": 1,
            "VehicleName": "TEST-TRUCK 1", "DriverName": "TEST-DRIVER", "AddressDispC": null,
            "DataDateTime": "2026-03-02T09:00:00+09:00", "AddressDispP": "TEST-AREA-A", "SubDriverCD": 0,
            "AllState": "Rest", "ReciveTypeColorName": null, "AllStateEx": null, "State2": "S2",
            "AllStateFontColor": null, "Speed": ""
        })
    );
    // 運転・休憩・休息以外は State2 を空に、速度は小数 1 桁
    assert_eq!(
        (current[1]["State2"].clone(), current[1]["Speed"].clone()),
        (json!(""), json!(30.0))
    );

    let select = ctx
        .get_json(t, "/dtako-logs/current/select?branch_cd=20")
        .await;
    assert_eq!(select.as_array().unwrap().len(), 1);
    assert_eq!(select[0]["VehicleCD"], 2);
    let select = ctx
        .get_json(
            t,
            "/dtako-logs/current/select?address_disp_p=TEST-AREA-A&vehicle_cds=x,%201,",
        )
        .await;
    assert_eq!(select.as_array().unwrap().len(), 1);
    let select = ctx
        .get_json(t, "/dtako-logs/current/select?vehicle_cds=x")
        .await;
    assert_eq!(
        select.as_array().unwrap().len(),
        2,
        "読める車両CD が無ければ絞らない"
    );

    let at = ctx
        .get_json(
            t,
            "/dtako-logs/by-date?date_time=2026-03-02T08:00:00%2B09:00",
        )
        .await;
    assert_eq!(at.as_array().unwrap().len(), 1);
    assert_eq!(at[0]["Speed"], 74.9);
    assert_eq!(at[0]["State2"], "S2");
    let at = ctx
        .get_json(
            t,
            "/dtako-logs/by-date?date_time=2026-03-02T08:00:00%2B09:00&vehicle_cd=2",
        )
        .await;
    assert_eq!(at, json!([]));

    // 範囲: 目録が無ければ DB だけ (古い順に並べ直す)
    let range = "/dtako-logs/by-date-range?start_date_time=2026-03-01T00:00:00%2B09:00&end_date_time=2026-03-02T23:59:59%2B09:00";
    let rows = ctx.get_json(t, range).await;
    let times: Vec<&str> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["DataDateTime"].as_str().unwrap())
        .collect();
    assert_eq!(
        times,
        [
            "2026-03-02T08:00:00+09:00",
            "2026-03-02T08:30:00+09:00",
            "2026-03-02T09:00:00+09:00"
        ]
    );

    // 目録: このテナントの 3 日 (範囲外・読めない・gzip でない) と 1 日 (読める)
    let archived = [
        r#"{"_archive_header":true}"#,
        "",
        "not json",
        r#"{"vehicle_cd":1,"data_date_time":"2026-03-01T07:00:00+09:00","vehicle_name":"TEST-TRUCK 1","speed":12.34,"all_state":"Drive","state2":"S9","sub_driver_cd":5}"#,
        r#"{"vehicle_cd":2,"data_date_time":"2026-03-01T07:30:00+09:00"}"#,
        // DB と同じ日時・車両は 1 行にまとめる
        r#"{"vehicle_cd":1,"data_date_time":"2026-03-02T08:00:00+09:00"}"#,
    ]
    .join("\n");
    ctx.store.seed(
        "archive/t/2026-03-01.jsonl.gz",
        gz(&archived),
        "application/gzip",
    );
    ctx.store.seed(
        "archive/t/2026-02-27.jsonl.gz",
        b"plain".to_vec(),
        "application/gzip",
    );
    let manifest = json!({ "archived_dates": {
        t.to_string(): {
            "2026-03-01": { "r2_key": "archive/t/2026-03-01.jsonl.gz", "row_count": 4 },
            "2026-02-27": { "r2_key": "archive/t/2026-02-27.jsonl.gz" },
            "2026-02-28": { "r2_key": "archive/t/missing.jsonl.gz" },
            "2026-03-05": { "r2_key": "archive/t/2026-03-01.jsonl.gz" },
        },
        other.to_string(): { "2026-03-01": { "r2_key": "archive/o/x.gz" } },
    } });
    ctx.store.seed(
        ARCHIVE_MANIFEST_KEY,
        manifest.to_string().into_bytes(),
        "application/json",
    );
    let wide = "/dtako-logs/by-date-range?start_date_time=2026-02-27&end_date_time=2026-03-02T23:59:59%2B09:00";
    let rows = ctx.get_json(t, wide).await;
    let times: Vec<(&str, i64)> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["DataDateTime"].as_str().unwrap(),
                r["VehicleCD"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        times,
        [
            ("2026-03-01T07:00:00+09:00", 1),
            ("2026-03-01T07:30:00+09:00", 2),
            ("2026-03-02T08:00:00+09:00", 1),
            ("2026-03-02T08:30:00+09:00", 2),
            ("2026-03-02T09:00:00+09:00", 1),
        ]
    );
    assert_eq!(rows[0]["Speed"], 12.3);
    assert_eq!(rows[0]["State2"], "S9");
    assert_eq!(rows[0]["SubDriverCD"], 5);
    assert_eq!(rows[1]["VehicleName"], "");
    let warns: Vec<String> = ctx
        .logs
        .all()
        .into_iter()
        .filter(|(l, _)| *l == LogLevel::Warn)
        .map(|(_, m)| m)
        .collect();
    assert_eq!(warns.len(), 2);
    assert!(warns.contains(&"dtako-logs archive skipped: unreadable".to_owned()));
    assert!(warns.contains(&"dtako-logs archive skipped: not gzip".to_owned()));
    // 車両で絞る (アーカイブも)・日付の部分が ISO でない書き方
    let one = ctx
        .get_json(t, "/dtako-logs/by-date-range?start_date_time=2026-03-01&end_date_time=2026-03-02T23:59:59%2B09:00&vehicle_cd=2")
        .await;
    assert_eq!(one.as_array().unwrap().len(), 2);
    // ISO でない書き方: 日付の部分は先頭 10 文字 (`Mar 2 2026` のまま目録の日付と文字列で比べるので 03-05 も入る。
    // 03-05 の置き場は 03-01 と同じなので並べ直して 1 行にまとまる)。DB は日時として読む (UTC の 03-02 00:00 まで)
    let words = ctx
        .get_json(
            t,
            "/dtako-logs/by-date-range?start_date_time=2026-03-01&end_date_time=Mar%202%202026",
        )
        .await;
    assert_eq!(words.as_array().unwrap().len(), 5);
    // 目録にテナントが無い・目録が読めない
    let bare = ctx.tenant("screen-logs-bare").await;
    assert_eq!(ctx.get_json(bare, wide).await, json!([]));
    ctx.store.fail_gets_containing("_manifest", 1);
    assert_eq!(ctx.get_json(t, range).await.as_array().unwrap().len(), 3);
    ctx.finish().await;
}

/// DB の失敗はどの口も 500 で、本文に DB のエラー文を載せない (段の名前と kind だけをログに)。
#[tokio::test(flavor = "multi_thread")]
async fn db_failures_are_500_without_db_text() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("screen-db-fail").await;
    let id = Uuid::new_v4();
    let d = Uuid::new_v4();
    let ctx = ctx
        .run_as_superuser(
            "REVOKE ALL ON alc_api.employees, alc_api.dtako_vehicles, alc_api.dtako_daily_work_hours, \
             alc_api.dtako_daily_work_segments, alc_api.dtako_operations, alc_api.dtako_operation_changes, \
             alc_api.dtako_event_classifications, alc_api.dtako_scrape_history, alc_api.dtakologs FROM alc_api_app",
        )
        .await;
    let cases: Vec<(Method, String, Option<Value>, &str, &str)> = vec![
        (Method::GET, "/drivers".into(), None, "drivers", ""),
        (Method::GET, "/vehicles".into(), None, "vehicles", ""),
        (Method::GET, "/work-times".into(), None, "work-times", ""),
        (Method::GET, "/daily-hours".into(), None, "daily-hours", ""),
        (
            Method::GET,
            format!("/daily-hours/{d}/2026-03-01/segments"),
            None,
            "daily-segments",
            "",
        ),
        (Method::GET, "/operations".into(), None, "operations", ""),
        (
            Method::GET,
            "/operations/calendar?year=2026&month=3".into(),
            None,
            "operations-calendar",
            "",
        ),
        (Method::GET, "/operations/U1".into(), None, "operation", ""),
        (
            Method::DELETE,
            "/operations/U1".into(),
            None,
            "operation-delete",
            "",
        ),
        (
            Method::GET,
            "/operations/U1/csv/events".into(),
            None,
            "operation-csv",
            "",
        ),
        (
            Method::GET,
            "/dtako/operation-changes?driver_cd=D1&from=2026-03-01&to=2026-03-31".into(),
            None,
            "operation-changes",
            "",
        ),
        (
            Method::GET,
            "/event-classifications".into(),
            None,
            "event-classifications",
            "",
        ),
        (
            Method::PUT,
            format!("/event-classifications/{id}"),
            Some(json!({ "classification": "drive" })),
            "event-classification-update",
            "internal server error",
        ),
        (
            Method::GET,
            "/scraper/history".into(),
            None,
            "scraper-history",
            "internal server error",
        ),
        (
            Method::POST,
            "/scraper/history".into(),
            Some(json!({ "target_date": "2026-03-01", "comp_id": "C1", "status": "ok" })),
            "scraper-history-save",
            "internal server error",
        ),
        (
            Method::GET,
            "/dtako-logs/current".into(),
            None,
            "dtako-logs-current",
            "",
        ),
        (
            Method::GET,
            "/dtako-logs/current/select".into(),
            None,
            "dtako-logs-select",
            "",
        ),
        (
            Method::GET,
            "/dtako-logs/by-date?date_time=x".into(),
            None,
            "dtako-logs-by-date",
            "",
        ),
        (
            Method::GET,
            "/dtako-logs/by-date-range?start_date_time=2026-03-01&end_date_time=2026-03-02".into(),
            None,
            "dtako-logs-by-date-range",
            "",
        ),
    ];
    for (method, path, body, what, text) in cases {
        let (status, got) = ctx.call(t, method, &path, body).await;
        assert_eq!(
            (status, got.as_str()),
            (StatusCode::INTERNAL_SERVER_ERROR, text),
            "{path}"
        );
        assert_eq!(
            ctx.logs.all().pop().unwrap(),
            (LogLevel::Error, format!("{what} failed: db (42501)")),
            "{path}"
        );
    }
    ctx.finish().await;
}
