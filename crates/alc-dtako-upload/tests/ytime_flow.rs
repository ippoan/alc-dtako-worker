//! Y時間 export の口 (`GET /dtako/y-time-export`・`POST /dtako/y-time-rows`、W5c-2) を、口から DB・保存先まで通して確かめる (native)。
//!
//! DB はテストの process の中で起こす組み込みの PostgreSQL (`embedded/mod.rs`)、保存先・ログは偽物 (`fakes/mod.rs`)。
//! 期待値は rust-alc-api の同じ口 (`tests/mock_tests/mock_dtako_y_time_export_test.rs`) と同じ。乗務員CD・名前・運行NO は作り物。
//! 行の組み立ての細かい規則は `src/ytime/{builder,from_shifts,segments}.rs` の単体テスト (rust から写したもの) が縛る。

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

    async fn driver(&self, t: Uuid, cd: &str, name: &str) -> Uuid {
        let mut c = self.pg.inner.lock().await;
        employee(&mut c, t, None, Some(cd), name, false).await
    }

    /// 分割済みの運行 1 行。日時は `+00` の壁時計 (取り込みは JST の壁時計を UTC として保存する)。
    async fn op(&self, t: Uuid, driver: Uuid, unko_no: &str, dep: Option<&str>, ret: Option<&str>) {
        let at = |v: Option<&str>| v.map_or("NULL".to_owned(), |v| format!("TIMESTAMPTZ '{v}+00'"));
        let sql = format!(
            "INSERT INTO dtako_operations (tenant_id, unko_no, crew_role, reading_date, driver_id, departure_at, return_at, has_kudgivt) \
             VALUES ($1, '{unko_no}', 1, DATE '2024-04-15', '{driver}', {}, {}, TRUE)",
            at(dep),
            at(ret)
        );
        let mut c = self.pg.inner.lock().await;
        assert_eq!(exec(&mut c, t, &sql).await, 1);
    }

    async fn call(
        &self,
        t: Uuid,
        method: Method,
        path: &str,
        body: Option<String>,
    ) -> (StatusCode, String) {
        let mut req = Request::builder().method(method).uri(path);
        let body = match body {
            Some(v) => {
                req = req.header("content-type", "application/json");
                Body::from(v)
            }
            None => Body::empty(),
        };
        let res = self.app(t).oneshot(req.body(body).unwrap()).await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await;
        (status, String::from_utf8(bytes.unwrap().to_vec()).unwrap())
    }

    async fn get_json(&self, t: Uuid, path: &str) -> Value {
        let (status, body) = self.call(t, Method::GET, path, None).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        serde_json::from_str(&body).unwrap()
    }
}

/// 最低限のヘッダー + 1 区間ぶんの行 (走行 30 分・休憩 60 分。rust のテストと同じ)。
fn minimal_kudgivt_csv() -> String {
    "運行NO,読取日,事業所CD,事業所名,車輌CD,車輌名,乗務員CD1,乗務員名１,対象乗務員区分,開始日時,イベントCD,イベント名,開始走行距離,終了走行距離,区間時間,区間距離,開始市町村CD,開始市町村名,終了市町村CD,終了市町村名,開始場所CD,開始場所名,終了場所CD,終了場所名\n\
     U001,2024/04/15 00:00:00,1,本社,1,車1,D001,テスト 一郎,1,2024/04/15 09:00:00,201,走行,0.0,10.0,30,10.0,1,a,1,a,,,,\n\
     U001,2024/04/15 00:00:00,1,本社,1,車1,D001,テスト 一郎,1,2024/04/15 12:00:00,301,休憩,10.0,10.0,60,0.0,1,a,1,a,,,,\n"
        .to_string()
}

/// 乗務員の照会・期間・運行の列挙・保存先の読み・行の組み立てを口から通す。読めない運行は warning。
#[tokio::test(flavor = "multi_thread")]
async fn y_time_export_reads_operations_and_kudgivt() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("ytime").await;
    let d1 = ctx.driver(t, "D1", "テスト 一郎").await;

    // 乗務員CD が無ければ 404、期間が逆なら 400 (本文は rust と同じ)
    let (status, body) = ctx
        .call(
            t,
            Method::GET,
            "/dtako/y-time-export?driver_cd=NOPE&from=2024-04-01&to=2024-04-30",
            None,
        )
        .await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::NOT_FOUND, "driver_cd not found: NOPE")
    );
    let (status, body) = ctx
        .call(
            t,
            Method::GET,
            "/dtako/y-time-export?driver_cd=D1&from=2024-04-30&to=2024-04-01",
            None,
        )
        .await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::BAD_REQUEST, "from > to")
    );

    // 運行が無ければ行も warning も無い
    let empty = ctx
        .get_json(
            t,
            "/dtako/y-time-export?driver_cd=D1&from=2024-04-15&to=2024-04-15",
        )
        .await;
    assert_eq!(
        empty,
        json!({ "driver": { "cd": "D1", "name": "テスト 一郎" }, "period": { "from": "2024-04-15", "to": "2024-04-15" },
                "rows": [], "warnings": [] })
    );

    ctx.op(
        t,
        d1,
        "U_OK",
        Some("2024-04-15 09:00"),
        Some("2024-04-15 18:00"),
    )
    .await;
    ctx.op(
        t,
        d1,
        "U_MISSING",
        Some("2024-04-15 09:00"),
        Some("2024-04-15 18:00"),
    )
    .await;
    ctx.op(t, d1, "U_NOTIMES", Some("2024-04-15 09:00"), None)
        .await;
    ctx.op(
        t,
        d1,
        "U_EMPTY",
        Some("2024-04-15 09:00"),
        Some("2024-04-15 18:00"),
    )
    .await;
    let csv = minimal_kudgivt_csv();
    let (sjis, _, _) = encoding_rs::SHIFT_JIS.encode(&csv);
    ctx.store.seed(
        &format!("{t}/unko/U_OK/KUDGIVT.csv"),
        sjis.to_vec(),
        "text/csv",
    );
    ctx.store.seed(
        &format!("{t}/unko/U_EMPTY/KUDGIVT.csv"),
        Vec::new(),
        "text/csv",
    );

    let res = ctx
        .get_json(
            t,
            "/dtako/y-time-export?driver_cd=D1&from=2024-04-15&to=2024-04-15",
        )
        .await;
    let rows = res["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "expected 1 row, got {rows:?}");
    let r = &rows[0];
    assert_eq!(r["date"], "2024-04-15");
    assert_eq!(r["previous_day_start"], false);
    assert_eq!(r["start_minutes_of_day"].as_i64().unwrap(), 9 * 60);
    assert_eq!(r["end_minutes_from_bucket_date"].as_i64().unwrap(), 13 * 60);
    assert_eq!(r["rest_today_5_22"].as_i64().unwrap(), 60);
    assert_eq!(r["rest_prev_5_22"].as_i64().unwrap(), 0);
    assert_eq!(r["rest_today_0_5"].as_i64().unwrap(), 0);
    assert_eq!(r["rest_next_5_22"].as_i64().unwrap(), 0);
    let warnings: Vec<&str> = res["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w.as_str().unwrap())
        .collect();
    assert_eq!(
        warnings,
        [
            "U_NOTIMES: departure_at/return_at が不足、skip",
            "U_EMPTY: KUDGIVT 取得失敗 (csv parse error: empty CSV)",
            "U_MISSING: KUDGIVT 取得失敗 (storage error: not found)",
        ]
    );

    // 保存先が読めないのも warning (固定の語)
    ctx.store.fail_gets_containing("U_OK", 1);
    let res = ctx
        .get_json(
            t,
            "/dtako/y-time-export?driver_cd=D1&from=2024-04-15&to=2024-04-15",
        )
        .await;
    assert_eq!(res["rows"], json!([]));
    assert!(res["warnings"].as_array().unwrap().contains(&json!(
        "U_OK: KUDGIVT 取得失敗 (storage error: object store error (get))"
    )));
    ctx.finish().await;
}

/// DB の失敗は 500 で本文は `internal error` (rust と同じ)。ログは段の名前と kind だけ。
#[tokio::test(flavor = "multi_thread")]
async fn y_time_export_db_failures_are_500() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("ytime-db-fail").await;
    ctx.driver(t, "D1", "テスト 一郎").await;
    let ctx = ctx
        .run_as_superuser("REVOKE SELECT ON alc_api.dtako_operations FROM alc_api_app")
        .await;
    let path = "/dtako/y-time-export?driver_cd=D1&from=2024-04-15&to=2024-04-15";
    let (status, body) = ctx.call(t, Method::GET, path, None).await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
    );
    assert_eq!(
        ctx.logs.all().pop().unwrap(),
        (
            LogLevel::Error,
            "y-time-export operations failed: db (42501)".to_owned()
        )
    );
    let ctx = ctx
        .run_as_superuser("REVOKE SELECT ON alc_api.employees FROM alc_api_app")
        .await;
    let (status, body) = ctx.call(t, Method::GET, path, None).await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
    );
    assert_eq!(
        ctx.logs.all().pop().unwrap(),
        (
            LogLevel::Error,
            "y-time-export driver failed: db (42501)".to_owned()
        )
    );
    ctx.finish().await;
}

fn y_time_rows_body() -> Value {
    json!({
        "from": "2024-04-01",
        "to": "2024-04-30",
        "shifts": [
            { "start": "2024-04-03 22:10:00", "end": "2024-04-04 09:05:00",
              "non_working": [
                  { "start": "2024-04-04 02:00:00", "end": "2024-04-04 03:00:00", "kind": "break_event" }
              ],
              "note": null },
            { "start": "2024-04-06 08:00:00", "end": "2024-04-06 17:00:00", "non_working": null }
        ]
    })
}

/// 勤務の列から行を返す (DB も保存先も読まない)。検証の破れと読めない body は 400。
#[tokio::test(flavor = "multi_thread")]
async fn y_time_rows_returns_rows_and_400s() {
    let ctx = Ctx::start().await;
    let t = Uuid::new_v4();
    let (status, body) = ctx
        .call(
            t,
            Method::POST,
            "/dtako/y-time-rows",
            Some(y_time_rows_body().to_string()),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let body: Value = serde_json::from_str(&body).unwrap();
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "expected 1 row, got {rows:?}");
    // 22:10 → 翌 09:05 (実働 9h55m) は終業日の行
    assert_eq!(rows[0]["date"], "2024-04-04");
    assert_eq!(rows[0]["previous_day_start"], true);
    assert_eq!(rows[0]["start_minutes_of_day"], 22 * 60 + 10);
    assert_eq!(rows[0]["end_minutes_from_bucket_date"], 9 * 60 + 5);
    assert_eq!(rows[0]["rest_today_0_5"], 60);
    assert_eq!(body["warnings"], json!([]));
    assert_eq!(
        body["excluded"],
        json!([{ "start": "2024-04-06 08:00:00", "end": "2024-04-06 17:00:00", "reason": "no_non_working" }])
    );

    // 検証の破れ (終業が始業より前)
    let mut bad = y_time_rows_body();
    bad["shifts"][0]["end"] = "2024-04-03 21:00:00".into();
    let (status, text) = ctx
        .call(t, Method::POST, "/dtako/y-time-rows", Some(bad.to_string()))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(text.contains("shifts[0]"), "{text}");
    // 時刻の形が違う・JSON でない body も 400
    let mut bad = y_time_rows_body();
    bad["shifts"][0]["start"] = "2024-04-03T22:10:00".into();
    let (status, _) = ctx
        .call(t, Method::POST, "/dtako/y-time-rows", Some(bad.to_string()))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = ctx
        .call(t, Method::POST, "/dtako/y-time-rows", Some("{".to_owned()))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // 2MB を超える body も受ける (上限は 20MB。rust と同じ)
    let mut big = y_time_rows_body();
    big["shifts"][1]["note"] = "x".repeat(3 * 1024 * 1024).into();
    let (status, body) = ctx
        .call(t, Method::POST, "/dtako/y-time-rows", Some(big.to_string()))
        .await;
    assert_eq!(status, StatusCode::OK, "{}", &body[..body.len().min(200)]);
    ctx.finish().await;
}
