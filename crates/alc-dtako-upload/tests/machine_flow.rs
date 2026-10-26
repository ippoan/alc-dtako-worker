//! 機械の取り込みの口 (`POST /dtako-logs/bulk`・`POST /dvr/notifications`・`POST /dvr/files/{id}`、W5a) を、
//! 口から DB・保存先まで通して確かめる (native)。
//!
//! DB はテストの process の中で起こす組み込みの PostgreSQL (`embedded/mod.rs`)、保存先・待ち・ログは偽物 (`fakes/mod.rs`)。
//! 期待値は rust-alc-api の同じ口 (`alc_dtako::dtako_logs`・`alc_dtako::dvr_notifications`) の応答と行。
//! 車両・機器・ファイル名は作り物。テナント ID はテストごとの乱数。

// 共用の土台・偽物のうち、このファイルが使わないものが在る
#[allow(dead_code)]
mod embedded;
#[allow(dead_code)]
mod fakes;

use std::sync::Arc;

use alc_core_wasm::TenantId;
use alc_dtako_upload::dvr::{validate, DvrNotificationCreate, MAX_FILE_BYTES};
use alc_dtako_upload::pg;
use alc_dtako_upload::routes::{tenant_router, DtakoState};
use alc_dtako_upload::split::LogLevel;
use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::{Extension, Router};
use embedded::{exec, rows_json, tenant, Embedded, Held, APP_ROLE};
use fakes::{FakeClock, FakeSleeper, FakeStore, Logs};
use futures_util::lock::Mutex;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

/// テスト 1 本ぶんの土台: DB (接続 1 本)・偽の保存先・溜めるログ。
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

    /// 接続を閉じ切って superuser で `sql` を流し、口の接続を張り直す (同時に張れる接続は 1 本)。保存先・ログはそのまま。
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

    async fn tenant(&self, name: &str) -> Uuid {
        let mut c = self.pg.inner.lock().await;
        tenant(&mut c, name).await
    }

    async fn rows(&self, tenant_id: Uuid, query: &str) -> Vec<Value> {
        let mut c = self.pg.inner.lock().await;
        rows_json(&mut c, tenant_id, query).await
    }

    async fn exec(&self, tenant_id: Uuid, sql: &str) -> u64 {
        let mut c = self.pg.inner.lock().await;
        exec(&mut c, tenant_id, sql).await
    }

    async fn post(&self, tenant_id: Uuid, path: &str, body: Body) -> (StatusCode, String) {
        let req = Request::builder().method("POST").uri(path);
        let req = req.header("content-type", "application/json");
        let res = self.app(tenant_id).oneshot(req.body(body).unwrap());
        let res = res.await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await;
        (status, String::from_utf8(bytes.unwrap().to_vec()).unwrap())
    }

    async fn post_json(&self, tenant_id: Uuid, path: &str, body: &Value) -> (StatusCode, String) {
        self.post(tenant_id, path, Body::from(body.to_string()))
            .await
    }

    /// 通知を起票して、応答の `pending` の id を返す (入れる順と同じ)。
    async fn ingest(&self, tenant_id: Uuid, items: Value) -> Vec<Uuid> {
        let (status, body) = self
            .post_json(tenant_id, "/dvr/notifications", &json!({ "items": items }))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let body: Value = serde_json::from_str(&body).unwrap();
        let pending = body["pending"].as_array().unwrap();
        pending
            .iter()
            .map(|p| p["id"].as_str().unwrap().parse().unwrap())
            .collect()
    }

    /// 口の接続を閉じ切ってから DB を止める (接続を残したまま止めると、止める側が待ち続ける)。
    async fn finish(self) {
        self.pg.close().await;
        self.db.shutdown();
    }

    fn last_log(&self) -> (LogLevel, String) {
        self.logs.all().pop().unwrap()
    }
}

fn json_of(body: &str) -> Value {
    serde_json::from_str(body).unwrap()
}

/// 車両の現在地のログ 1 行 (PascalCase。主キー以外は `extra` で足す)。
fn log_row(at: Option<&str>, vehicle_cd: i32, extra: Value) -> Value {
    let mut row = json!({ "DataDateTime": at, "VehicleCD": vehicle_cd });
    for (k, v) in extra.as_object().unwrap() {
        row[k] = v.clone();
    }
    row
}

const LOG_COLS: &str = "SELECT data_date_time, vehicle_cd, type, all_state_ryout_color, branch_cd, gps_latitude, speed, driver_name, \
     vehicle_icon_label_for_vehicle FROM alc_api.dtakologs WHERE tenant_id = $1 ORDER BY vehicle_cd, data_date_time";

/// 一括 upsert: 新しい行は入れ、同じ主キーは上書きする。省いた列は既定値、日時の無い行は既定の日時で入る。
/// 1 文の上限 (500 行) を超えても全部入る。空の配列は DB を引かずに 0 件。テナントの外の行は見えない。
#[tokio::test(flavor = "multi_thread")]
async fn dtakologs_bulk_inserts_overwrites_and_keeps_defaults() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("logs").await;
    let other = ctx.tenant("logs-other").await;

    let rows = json!([
        log_row(
            Some("2026-10-26T09:00:00+09:00"),
            101,
            json!({ "__type": "VehicleState", "BranchCD": 3, "GPSLatitude": 43031700, "Speed": 42.5,
                    "DriverName": "TEST-DRIVER", "VehicleIconLabelForVehicle": "101" })
        ),
        log_row(None, 102, json!({})),
    ]);
    let (status, body) = ctx.post_json(t, "/dtako-logs/bulk", &rows).await;
    assert_eq!(status, StatusCode::OK);
    let want = json!({ "success": true, "records_added": 2, "total_records": 2, "message": "Upserted 2 records" });
    assert_eq!(json_of(&body), want);
    let got = ctx.rows(t, LOG_COLS).await;
    assert_eq!(
        got,
        vec![
            json!({ "data_date_time": "2026-10-26T09:00:00+09:00", "vehicle_cd": 101, "type": "VehicleState",
                    "all_state_ryout_color": "Transparent", "branch_cd": 3, "gps_latitude": 43031700, "speed": 42.5,
                    "driver_name": "TEST-DRIVER", "vehicle_icon_label_for_vehicle": "101" }),
            json!({ "data_date_time": "2020-01-01T00:00:00+09:00", "vehicle_cd": 102, "type": "",
                    "all_state_ryout_color": "Transparent", "branch_cd": 0, "gps_latitude": 0, "speed": 0,
                    "driver_name": null, "vehicle_icon_label_for_vehicle": null }),
        ]
    );

    // 同じ主キーは上書き (行は増えない)
    let again = json!([log_row(
        Some("2026-10-26T09:00:00+09:00"),
        101,
        json!({ "Speed": 10.0, "AllStateRyoutColor": "Red" })
    )]);
    let (status, body) = ctx.post_json(t, "/dtako-logs/bulk", &again).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_of(&body)["records_added"], 1);
    let got = ctx.rows(t, LOG_COLS).await;
    assert_eq!(got.len(), 2);
    assert_eq!(got[0]["speed"], 10);
    assert_eq!(got[0]["all_state_ryout_color"], "Red");
    assert_eq!(
        got[0]["driver_name"],
        Value::Null,
        "省いた列は NULL で上書きする (rust と同じ)"
    );

    // 1 文の上限を超える (500 + 1 行)
    let many: Vec<Value> = (0..501)
        .map(|i| {
            log_row(
                Some(&format!("2026-10-25T{:02}:{:02}:00+09:00", i / 60, i % 60)),
                200,
                json!({}),
            )
        })
        .collect();
    let (status, body) = ctx.post_json(t, "/dtako-logs/bulk", &json!(many)).await;
    assert_eq!(status, StatusCode::OK);
    let want = json!({ "success": true, "records_added": 501, "total_records": 501, "message": "Upserted 501 records" });
    assert_eq!(json_of(&body), want);
    let count = ctx
        .rows(
            t,
            "SELECT COUNT(*) AS n FROM alc_api.dtakologs WHERE tenant_id = $1 AND vehicle_cd = 200",
        )
        .await;
    assert_eq!(count, vec![json!({ "n": 501 })]);

    // 空の配列
    let (status, body) = ctx.post_json(t, "/dtako-logs/bulk", &json!([])).await;
    assert_eq!(status, StatusCode::OK);
    let want = json!({ "success": true, "records_added": 0, "total_records": 0, "message": "No records provided" });
    assert_eq!(json_of(&body), want);
    let zero = {
        let mut c = ctx.pg.inner.lock().await;
        pg::dtakologs_bulk_upsert(&mut c, t, Vec::new())
            .await
            .unwrap()
    };
    assert_eq!(zero, 0, "空なら transaction を開かない");

    // テナントの外
    assert!(ctx.rows(other, LOG_COLS).await.is_empty());

    // 形の違う body は axum の Json が弾く (DB を引かない)
    let (status, _) = ctx.post(t, "/dtako-logs/bulk", Body::from("{}")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    ctx.finish().await;
}

/// 一括 upsert の DB の失敗は 500 で本文は空。ログは段の名前と kind だけ。
#[tokio::test(flavor = "multi_thread")]
async fn dtakologs_bulk_db_failure_is_500_with_fixed_log() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("logs-fail").await;
    let ctx = ctx
        .run_as_superuser("REVOKE INSERT ON alc_api.dtakologs FROM alc_api_app")
        .await;
    let rows = json!([log_row(Some("2026-10-26T09:00:00+09:00"), 101, json!({}))]);
    let (status, body) = ctx.post_json(t, "/dtako-logs/bulk", &rows).await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, "")
    );
    let (level, message) = ctx.last_log();
    assert_eq!(level, LogLevel::Error);
    assert!(
        message.starts_with("dtako-logs bulk failed: db ("),
        "{message}"
    );
    assert!(!message.contains(&t.to_string()));
    ctx.finish().await;
}

fn dvr_item(serial_no: &str, file_name: &str) -> Value {
    json!({ "serial_no": serial_no, "file_name": file_name })
}

const DVR_COLS: &str = "SELECT serial_no, file_name, vehicle_cd, vehicle_name, driver_name, event_type, \
     to_char(dvr_datetime AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS') AS dvr_datetime_utc, source_url, \
     file_status, attempts, r2_key, size_bytes, last_error \
     FROM alc_api.dvr_notifications WHERE tenant_id = $1 ORDER BY serial_no, file_name";

/// 通知のバッチ: 空・上限超え・検査に通らない行の在るバッチは 400 で、何も書かない。
#[tokio::test(flavor = "multi_thread")]
async fn dvr_notifications_reject_bad_batches() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("dvr-bad").await;
    let empty = json!({ "items": [] });
    let too_many: Vec<Value> = (0..201)
        .map(|i| dvr_item("SN-1", &format!("{i}.vdf")))
        .collect();
    let traversal =
        json!({ "items": [dvr_item("SN-1", "a.vdf"), dvr_item("SN-1", "../../etc/passwd")] });
    for body in [empty, json!({ "items": too_many }), traversal] {
        let (status, text) = ctx.post_json(t, "/dvr/notifications", &body).await;
        assert_eq!((status, text.as_str()), (StatusCode::BAD_REQUEST, ""));
    }
    assert!(ctx.rows(t, DVR_COLS).await.is_empty());
    ctx.finish().await;
}

/// 通知のバッチ: 新しい行は作って `pending` に載せる。既に在る行は `skipped` に数え、まだ取りに行くべき行
/// (`pending` かつ試行回数が上限未満) だけ `pending` に再掲する。同じ自然キーでもテナントが違えば別の行。
#[tokio::test(flavor = "multi_thread")]
async fn dvr_notifications_ingest_and_repeat_pending() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("dvr").await;
    let other = ctx.tenant("dvr-other").await;

    let first = json!({ "items": [
        { "serial_no": "SN-1", "file_name": "a.vdf", "vehicle_cd": "1234", "vehicle_name": "TEST 100 あ 1234",
          "driver_name": "TEST-DRIVER", "event_type": "急ブレーキ", "dvr_datetime": "2026-09-03T01:23:45+09:00",
          "source_url": "/dvr/2026/09/03/SN-1/a.vdf" },
        dvr_item("SN-1", "b.vdf"),
    ]});
    let (status, body) = ctx.post_json(t, "/dvr/notifications", &first).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_of(&body);
    assert_eq!(
        (body["inserted"].clone(), body["skipped"].clone()),
        (json!(2), json!(0))
    );
    let pending = body["pending"].as_array().unwrap();
    let names: Vec<(&str, &str)> = pending
        .iter()
        .map(|p| {
            (
                p["serial_no"].as_str().unwrap(),
                p["file_name"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(names, vec![("SN-1", "a.vdf"), ("SN-1", "b.vdf")]);
    let rows = ctx.rows(t, DVR_COLS).await;
    assert_eq!(
        rows[0],
        json!({ "serial_no": "SN-1", "file_name": "a.vdf", "vehicle_cd": "1234", "vehicle_name": "TEST 100 あ 1234",
                "driver_name": "TEST-DRIVER", "event_type": "急ブレーキ", "dvr_datetime_utc": "2026-09-02T16:23:45",
                "source_url": "/dvr/2026/09/03/SN-1/a.vdf", "file_status": "pending", "attempts": 0,
                "r2_key": null, "size_bytes": null, "last_error": null })
    );
    assert_eq!(rows[1]["dvr_datetime_utc"], Value::Null);

    // 同じ 2 件 + 新しい 1 件: 既存の 2 件は skipped だが、まだ取りに行くべきなので pending に再掲する
    let ids = ctx
        .ingest(
            t,
            json!([
                dvr_item("SN-1", "a.vdf"),
                dvr_item("SN-1", "b.vdf"),
                dvr_item("SN-2", "c.vdf")
            ]),
        )
        .await;
    assert_eq!(ids.len(), 3);
    let a_id = pending[0]["id"].as_str().unwrap().parse::<Uuid>().unwrap();
    assert_eq!(ids[0], a_id, "再掲は同じ行の id");

    // 試行回数が上限に達した行・置き終えた行は再掲しない
    ctx.exec(t, "UPDATE alc_api.dvr_notifications SET attempts = 6 WHERE tenant_id = $1 AND file_name = 'a.vdf'")
        .await;
    ctx.exec(t, "UPDATE alc_api.dvr_notifications SET file_status = 'stored' WHERE tenant_id = $1 AND file_name = 'b.vdf'")
        .await;
    let (status, body) = ctx
        .post_json(
            t,
            "/dvr/notifications",
            &json!({ "items": [dvr_item("SN-1", "a.vdf"), dvr_item("SN-1", "b.vdf"), dvr_item("SN-2", "c.vdf")] }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let body = json_of(&body);
    assert_eq!(
        (body["inserted"].clone(), body["skipped"].clone()),
        (json!(0), json!(3))
    );
    let names: Vec<&str> = body["pending"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["file_name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["c.vdf"]);

    // 別のテナントは同じ自然キーでも新しい行
    let ids = ctx.ingest(other, json!([dvr_item("SN-1", "a.vdf")])).await;
    assert_eq!(ids.len(), 1);
    assert_ne!(ids[0], a_id);
    assert_eq!(ctx.rows(t, DVR_COLS).await.len(), 3);
    ctx.finish().await;
}

/// 実体: 生のバイト列を保存先に置き、行を `stored` にする。応答は id・`stored`・大きさ・key。
/// `{id}` は名乗ったテナントの行だけ (別テナントの行・無い id はどちらも 404 で、何も書かない)。
#[tokio::test(flavor = "multi_thread")]
async fn dvr_file_is_stored_and_marked() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("dvr-file").await;
    let other = ctx.tenant("dvr-file-other").await;
    let id = ctx.ingest(t, json!([dvr_item("SN-9", "x.vdf")])).await[0];

    let path = format!("/dvr/files/{id}");
    for (tenant_id, path) in [
        (other, path.clone()),
        (t, format!("/dvr/files/{}", Uuid::new_v4())),
    ] {
        let (status, body) = ctx.post(tenant_id, &path, Body::from(vec![1u8; 10])).await;
        assert_eq!((status, body.as_str()), (StatusCode::NOT_FOUND, ""));
    }
    assert!(ctx.store.objects().is_empty());

    let (status, body) = ctx.post(t, &path, Body::from(vec![7u8; 1024])).await;
    assert_eq!(status, StatusCode::OK);
    let key = format!("{t}/dvr/SN-9/x.vdf");
    let want = json!({ "id": id, "file_status": "stored", "size": 1024, "r2_key": key });
    assert_eq!(json_of(&body), want);
    let (bytes, content_type) = ctx.store.object(&key).unwrap();
    assert_eq!(
        (bytes, content_type.as_str()),
        (vec![7u8; 1024], "application/octet-stream")
    );
    let row = ctx.rows(t, DVR_COLS).await;
    assert_eq!(
        (
            &row[0]["file_status"],
            &row[0]["r2_key"],
            &row[0]["size_bytes"],
            &row[0]["attempts"],
            &row[0]["last_error"]
        ),
        (
            &json!("stored"),
            &json!(key),
            &json!(1024),
            &json!(0),
            &Value::Null
        )
    );
    ctx.finish().await;
}

/// 実体の失敗は試行回数を進めて理由 (固定の語) を残す: 大きすぎる = 413 で `failed`、body が壊れた = 400、
/// 保存先に置けない = 502 (どちらも `pending` のまま)。
#[tokio::test(flavor = "multi_thread")]
async fn dvr_file_failures_are_recorded() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("dvr-file-fail").await;
    let ids = ctx
        .ingest(
            t,
            json!([
                dvr_item("SN-1", "big.vdf"),
                dvr_item("SN-1", "broken.vdf"),
                dvr_item("SN-1", "nostore.vdf")
            ]),
        )
        .await;

    let big = Body::from(vec![0u8; MAX_FILE_BYTES + 1]);
    let (status, body) = ctx.post(t, &format!("/dvr/files/{}", ids[0]), big).await;
    assert_eq!((status, body.as_str()), (StatusCode::PAYLOAD_TOO_LARGE, ""));

    let chunks: Vec<Result<Vec<u8>, std::io::Error>> =
        vec![Ok(vec![1u8; 8]), Err(std::io::Error::other("cut"))];
    let broken = Body::from_stream(futures_util::stream::iter(chunks));
    let (status, body) = ctx.post(t, &format!("/dvr/files/{}", ids[1]), broken).await;
    assert_eq!((status, body.as_str()), (StatusCode::BAD_REQUEST, ""));

    ctx.store.fail_puts(&format!("{t}/dvr/SN-1/nostore.vdf"), 1);
    let (status, body) = ctx
        .post(
            t,
            &format!("/dvr/files/{}", ids[2]),
            Body::from(vec![2u8; 4]),
        )
        .await;
    assert_eq!((status, body.as_str()), (StatusCode::BAD_GATEWAY, ""));
    assert_eq!(
        ctx.last_log(),
        (
            LogLevel::Error,
            "dvr file failed: object store error (put)".to_owned()
        )
    );

    let got: Vec<Value> = ctx
        .rows(t, DVR_COLS)
        .await
        .into_iter()
        .map(|r| {
            json!([
                r["file_name"],
                r["file_status"],
                r["attempts"],
                r["last_error"],
                r["r2_key"]
            ])
        })
        .collect();
    assert_eq!(
        got,
        vec![
            json!(["big.vdf", "failed", 1, "file exceeds 33554432 bytes", null]),
            json!(["broken.vdf", "pending", 1, "body stream error", null]),
            json!(["nostore.vdf", "pending", 1, "R2 upload failed", null]),
        ]
    );
    assert!(ctx.store.objects().is_empty());
    ctx.finish().await;
}

/// dvr の DB の失敗: 起票・行の引き当て・置いた印は 500 (本文は空)。失敗の記録が書けなくても応答は変えず、ログに残す。
#[tokio::test(flavor = "multi_thread")]
async fn dvr_db_failures_are_500_with_fixed_logs() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("dvr-db").await;
    let ids = ctx
        .ingest(
            t,
            json!([dvr_item("SN-1", "a.vdf"), dvr_item("SN-1", "b.vdf")]),
        )
        .await;

    // UPDATE を奪う: 置いた印が書けない (500)・失敗の記録が書けない (413 のまま、ログに残す)
    let ctx = ctx
        .run_as_superuser("REVOKE UPDATE ON alc_api.dvr_notifications FROM alc_api_app")
        .await;
    let (status, body) = ctx
        .post(
            t,
            &format!("/dvr/files/{}", ids[0]),
            Body::from(vec![3u8; 16]),
        )
        .await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, "")
    );
    let (_, message) = ctx.last_log();
    assert!(message.starts_with("dvr stored failed: db ("), "{message}");
    let big = Body::from(vec![0u8; MAX_FILE_BYTES + 1]);
    let (status, _) = ctx.post(t, &format!("/dvr/files/{}", ids[1]), big).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let (_, message) = ctx.last_log();
    assert!(
        message.starts_with("dvr record failure failed: db ("),
        "{message}"
    );

    // INSERT を奪う: 起票が 500
    let ctx = ctx
        .run_as_superuser("REVOKE INSERT ON alc_api.dvr_notifications FROM alc_api_app")
        .await;
    let (status, body) = ctx
        .post_json(
            t,
            "/dvr/notifications",
            &json!({ "items": [dvr_item("SN-2", "c.vdf")] }),
        )
        .await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, "")
    );
    let (_, message) = ctx.last_log();
    assert!(message.starts_with("dvr ingest failed: db ("), "{message}");

    // SELECT を奪う: 行の引き当てが 500
    let ctx = ctx
        .run_as_superuser("REVOKE SELECT ON alc_api.dvr_notifications FROM alc_api_app")
        .await;
    let (status, body) = ctx
        .post(
            t,
            &format!("/dvr/files/{}", ids[0]),
            Body::from(vec![3u8; 16]),
        )
        .await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, "")
    );
    let (_, message) = ctx.last_log();
    assert!(message.starts_with("dvr lookup failed: db ("), "{message}");
    for (_, m) in ctx.logs.all() {
        assert!(
            !m.contains(&t.to_string()),
            "ログにテナント ID を出さない: {m}"
        );
    }
    ctx.finish().await;
}

fn item(serial_no: &str, file_name: &str) -> DvrNotificationCreate {
    serde_json::from_value(dvr_item(serial_no, file_name)).unwrap()
}

/// 通知の検査 (rust-alc-api と同じ規則): key の部品は英数字と `-` `_` `.` だけで `..` を含まない・長さの上限。
/// 表示用の欄は長さだけ、`source_url` は URL の形を問わず 2048 文字まで。
#[test]
fn dvr_validation_matches_backend_rules() {
    assert!(validate(&item("SN-0001", "20260903_012345.vdf")));
    for (sn, f) in [
        ("", "a.vdf"),
        ("sn", ""),
        (&*"x".repeat(65), "a.vdf"),
        ("sn", &*"x".repeat(129)),
    ] {
        assert!(!validate(&item(sn, f)), "{sn} {f}");
    }
    for (sn, f) in [
        ("sn", "a/b.vdf"),
        ("sn/..", "a.vdf"),
        ("sn", "a b.vdf"),
        ("sn", "a%00.vdf"),
    ] {
        assert!(!validate(&item(sn, f)), "{sn} {f}");
    }
    let mut ok = item("sn", "a.vdf");
    ok.vehicle_name = Some("あ".repeat(256));
    ok.source_url = Some("x".repeat(2048));
    assert!(validate(&ok));
    for field in 0..5 {
        let mut over = item("sn", "a.vdf");
        let long = Some("あ".repeat(257));
        match field {
            0 => over.vehicle_cd = long,
            1 => over.vehicle_name = long,
            2 => over.driver_name = long,
            3 => over.event_type = long,
            _ => over.source_url = Some("x".repeat(2049)),
        }
        assert!(!validate(&over), "field {field}");
    }
}
