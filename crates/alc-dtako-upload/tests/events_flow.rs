//! 勤怠向けの読み取りの口 (`GET /dtako/events`・`GET /dtako/events/etags`、W5b) を、口から DB・保存先まで通して確かめる (native)。
//!
//! DB はテストの process の中で起こす組み込みの PostgreSQL (`embedded/mod.rs`)、保存先・ログは偽物 (`fakes/mod.rs`)。
//! 期待値は rust-alc-api の同じ口 (`alc_dtako::dtako_events`) の応答。運行NO・乗務員CD・名前は作り物。

// 共用の土台・偽物のうち、このファイルが使わないものが在る
#[allow(dead_code)]
mod embedded;
#[allow(dead_code)]
mod fakes;

use std::sync::Arc;

use alc_core_wasm::TenantId;
use alc_dtako_upload::events::{derive_day_prefixes, kudgivt_key, parse_csv};
use alc_dtako_upload::routes::{tenant_router, DtakoState};
use alc_dtako_upload::split::LogLevel;
use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::{Extension, Router};
use embedded::{employee, exec, tenant, Embedded, Held, APP_ROLE};
use fakes::{fake_etag, FakeClock, FakeSleeper, FakeStore, Logs};
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

    /// 口の接続を閉じ切ってから DB を止める。
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

    /// 運行 1 行。`dep` は `+09` 付きの日時の文字列 (NULL は None)。値はテストの固定値を文に直に書く。
    #[allow(clippy::too_many_arguments)]
    async fn op(
        &self,
        t: Uuid,
        driver: Uuid,
        unko_no: &str,
        crew_role: i32,
        reading: &str,
        dep: Option<&str>,
        kudgivt: bool,
        prefix: Option<&str>,
    ) {
        let dep = dep.map_or("NULL".to_owned(), |d| format!("TIMESTAMPTZ '{d}'"));
        let ret = "NULL";
        let prefix = prefix.map_or("NULL".to_owned(), |p| format!("'{p}'"));
        let sql = format!(
            "INSERT INTO dtako_operations (tenant_id, unko_no, crew_role, reading_date, driver_id, \
             departure_at, return_at, has_kudgivt, r2_key_prefix) \
             VALUES ($1, '{unko_no}', {crew_role}, DATE '{reading}', '{driver}', {dep}, {ret}, {kudgivt}, {prefix})"
        );
        let mut c = self.pg.inner.lock().await;
        assert_eq!(exec(&mut c, t, &sql).await, 1);
    }

    fn seed(&self, key: &str, text: &str) {
        self.store
            .seed(key, text.as_bytes().to_vec(), "text/csv; charset=utf-8");
    }

    async fn get(&self, t: Uuid, path: &str) -> (StatusCode, String) {
        let req = Request::builder().uri(path).body(Body::empty()).unwrap();
        let res = self.app(t).oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await;
        (status, String::from_utf8(bytes.unwrap().to_vec()).unwrap())
    }

    async fn get_json(&self, t: Uuid, path: &str) -> Value {
        let (status, body) = self.get(t, path).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        serde_json::from_str(&body).unwrap()
    }

    fn last_log(&self) -> (LogLevel, String) {
        self.logs.all().pop().unwrap()
    }
}

const HEADER: &str = "運行NO,対象乗務員区分,開始日時,イベントCD,イベント名";

fn csv(unko_no: &str, rows: &[&str]) -> String {
    let mut out = format!("{HEADER}\r\n");
    for r in rows {
        out.push_str(&format!("{unko_no},{r}\r\n"));
    }
    out
}

fn key(t: Uuid, unko_no: &str) -> String {
    format!("{t}/unko/{unko_no}/KUDGIVT.csv")
}

/// 乗務員 1 人の版: 期間を前後 1 日広げて分割済みの運行を並べ (同じ運行NO は crew_role の小さい方)、KUDGIVT.csv を
/// 畳まずに返す。出発の早い順、`r2_key_prefix` が在ればその下の key、Shift_JIS は読み直す、読めない運行は warning。
#[tokio::test(flavor = "multi_thread")]
async fn single_driver_returns_raw_csv_per_operation() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("events-single").await;
    let d1 = ctx.driver(t, Some("D1"), "TEST-DRIVER ONE", false).await;
    let other = ctx.driver(t, Some("D9"), "TEST-DRIVER NINE", false).await;
    // 期間 03/01〜03/02 → 02/28〜03/03 の読取日を拾う
    ctx.op(
        t,
        d1,
        "260302080000",
        2,
        "2026-03-02",
        Some("2026-03-02 08:00+09"),
        true,
        None,
    )
    .await;
    ctx.op(
        t,
        d1,
        "260302080000",
        1,
        "2026-03-02",
        Some("2026-03-02 08:00+09"),
        true,
        None,
    )
    .await;
    ctx.op(
        t,
        d1,
        "260228230000",
        1,
        "2026-02-28",
        Some("2026-02-28 23:00+09"),
        true,
        None,
    )
    .await;
    ctx.op(
        t,
        d1,
        "260303100000",
        1,
        "2026-03-03",
        None,
        true,
        Some("custom/prefix/"),
    )
    .await;
    ctx.op(
        t,
        d1,
        "260227090000",
        1,
        "2026-02-27",
        Some("2026-02-27 09:00+09"),
        true,
        None,
    )
    .await; // 期間外
    ctx.op(
        t,
        d1,
        "260301120000",
        1,
        "2026-03-01",
        Some("2026-03-01 12:00+09"),
        false,
        None,
    )
    .await; // 未分割
    ctx.op(
        t,
        d1,
        "260301130000",
        1,
        "2026-03-01",
        Some("2026-03-01 13:00+09"),
        true,
        None,
    )
    .await; // 実体なし
    ctx.op(
        t,
        d1,
        "260301140000",
        1,
        "2026-03-01",
        Some("2026-03-01 14:00+09"),
        true,
        None,
    )
    .await; // 読めない
    ctx.op(
        t,
        other,
        "260301150000",
        1,
        "2026-03-01",
        Some("2026-03-01 15:00+09"),
        true,
        None,
    )
    .await; // 別の乗務員

    let mut text = csv("260302080000", &["1,2026/03/02 08:00:00,201,運転"]);
    text.push_str("\r\n   \r\n");
    ctx.seed(&key(t, "260302080000"), &text);
    let utf8 = csv("260228230000", &["1,2026/02/28 23:00:00,302,休息"]);
    let sjis = encoding_rs::SHIFT_JIS.encode(&utf8).0.into_owned();
    ctx.store.seed(&key(t, "260228230000"), sjis, "text/csv");
    ctx.seed(
        "custom/prefix/KUDGIVT.csv",
        &csv("260303100000", &["1, 2026/03/03 10:00:00 ,202,荷役"]),
    );
    ctx.seed(&key(t, "260301140000"), "x");
    ctx.store.fail_gets_containing("260301140000", 1);

    let body = ctx
        .get_json(
            t,
            "/dtako/events?driver_cd=D1&date_from=2026-03-01&date_to=2026-03-02",
        )
        .await;
    let ops = |unko: &str, crew: i32, dep: Value, row: Vec<&str>| {
        json!({ "unko_no": unko, "crew_role": crew, "departure_at": dep, "return_at": null,
                "headers": ["運行NO", "対象乗務員区分", "開始日時", "イベントCD", "イベント名"], "rows": [row] })
    };
    let want = json!({
        "driver": { "cd": "D1", "name": "TEST-DRIVER ONE" },
        "period": { "date_from": "2026-03-01", "date_to": "2026-03-02" },
        "operations": [
            ops("260228230000", 1, json!("2026-02-28T14:00:00Z"), vec!["260228230000", "1", "2026/02/28 23:00:00", "302", "休息"]),
            ops("260302080000", 1, json!("2026-03-01T23:00:00Z"), vec!["260302080000", "1", "2026/03/02 08:00:00", "201", "運転"]),
            ops("260303100000", 1, Value::Null, vec!["260303100000", "1", "2026/03/03 10:00:00", "202", "荷役"]),
        ],
        "warnings": [
            "260301130000: KUDGIVT 取得失敗 (not found)",
            "260301140000: KUDGIVT 取得失敗 (object store error (get))",
        ],
    });
    // 出発が NULL の運行は先頭 (Option の並びで None が先)
    let mut want = want;
    let ops_want = want["operations"].as_array_mut().unwrap();
    ops_want.rotate_right(1);
    assert_eq!(body, want);

    // 404・400 (本文は rust と同じ文字列)
    let cases = [
        (
            "/dtako/events?driver_cd=NOPE&date_from=2026-03-01&date_to=2026-03-02",
            StatusCode::NOT_FOUND,
            "driver_cd not found: NOPE",
        ),
        (
            "/dtako/events?driver_cd=D1&date_from=2026-03-02&date_to=2026-03-01",
            StatusCode::BAD_REQUEST,
            "date_from > date_to",
        ),
        (
            "/dtako/events?driver_cd=D1&date_from=2025-01-01&date_to=2026-01-01",
            StatusCode::BAD_REQUEST,
            "range too wide: 366 days (max 366)",
        ),
        (
            "/dtako/events?driver_cd=D1&date_from=2025-01-01&date_to=2026-01-02",
            StatusCode::BAD_REQUEST,
            "range too wide: 367 days (max 366)",
        ),
        (
            "/dtako/events?driver_cd=D1&date_from=2026-03-01&date_to=2026-03-02&page_size=5",
            StatusCode::BAD_REQUEST,
            "page_size / after_driver_cd are only valid without driver_cd",
        ),
        (
            "/dtako/events?driver_cd=D1&date_from=2026-03-01&date_to=2026-03-02&after_driver_cd=A",
            StatusCode::BAD_REQUEST,
            "page_size / after_driver_cd are only valid without driver_cd",
        ),
    ];
    for (path, status, text) in cases {
        let got = ctx.get(t, path).await;
        if text.starts_with("range too wide: 366") {
            assert_eq!(got.0, StatusCode::OK, "{path}: 366 日はちょうど上限");
            continue;
        }
        assert_eq!((got.0, got.1.as_str()), (status, text), "{path}");
    }
    let (status, _) = ctx.get(t, "/dtako/events?driver_cd=D1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "日付が無い");
    ctx.finish().await;
}

/// 全乗務員の版: 乗務員CD の順の keyset のページ (ちょうど満ちたときだけ次の `after_driver_cd`)。論理削除・乗務員CD 無し・
/// 分割済みの運行が無い乗務員は出ない。相乗りの運行の実体は 1 回だけ読み、同じ warning はまとめる。
#[tokio::test(flavor = "multi_thread")]
async fn all_drivers_page_by_driver_cd() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("events-all").await;
    let a = ctx.driver(t, Some("A1"), "TEST-A", false).await;
    let b = ctx.driver(t, Some("B2"), "TEST-B", false).await;
    let c = ctx.driver(t, Some("C3"), "TEST-C", false).await;
    let deleted = ctx.driver(t, Some("D4"), "TEST-D", true).await;
    let no_cd = ctx.driver(t, None, "TEST-E", false).await;
    let idle = ctx.driver(t, Some("F6"), "TEST-F", false).await;
    let day = "2026-03-02";
    let dep = Some("2026-03-02 08:00+09");
    // A と B が同じ運行に相乗り (運転手 = A、副運転手 = B)、その実体は無い
    ctx.op(t, a, "260302080000", 1, day, dep, true, None).await;
    ctx.op(t, b, "260302080000", 2, day, dep, true, None).await;
    ctx.op(
        t,
        b,
        "260302090000",
        1,
        day,
        Some("2026-03-02 09:00+09"),
        true,
        None,
    )
    .await;
    ctx.op(
        t,
        c,
        "260302100000",
        1,
        day,
        Some("2026-03-02 10:00+09"),
        true,
        None,
    )
    .await;
    ctx.op(t, deleted, "260302110000", 1, day, dep, true, None)
        .await;
    ctx.op(t, no_cd, "260302120000", 1, day, dep, true, None)
        .await;
    ctx.op(t, idle, "260302130000", 1, day, dep, false, None)
        .await;
    ctx.seed(
        &key(t, "260302090000"),
        &csv("260302090000", &["1,2026/03/02 09:00:00,201,運転"]),
    );
    ctx.seed(
        &key(t, "260302100000"),
        &csv("260302100000", &["1,2026/03/02 10:00:00,201,運転"]),
    );

    let page1 = ctx
        .get_json(
            t,
            "/dtako/events?date_from=2026-03-01&date_to=2026-03-31&page_size=2",
        )
        .await;
    let cds: Vec<&str> = page1["drivers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["driver"]["cd"].as_str().unwrap())
        .collect();
    assert_eq!(cds, vec!["A1", "B2"]);
    assert_eq!(page1["next_after_driver_cd"], "B2");
    assert_eq!(
        page1["period"],
        json!({ "date_from": "2026-03-01", "date_to": "2026-03-31" })
    );
    assert_eq!(page1["drivers"][0]["operations"], json!([]));
    let b_ops: Vec<&str> = page1["drivers"][1]["operations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["unko_no"].as_str().unwrap())
        .collect();
    assert_eq!(b_ops, vec!["260302090000"]);
    assert_eq!(
        page1["warnings"],
        json!(["260302080000: KUDGIVT 取得失敗 (not found)"]),
        "2 人分の同じ warning は 1 つ"
    );
    let calls = ctx.store.get_calls_containing("260302080000");
    assert_eq!(
        calls,
        vec![(key(t, "260302080000"), 1)],
        "相乗りの運行は 1 回だけ読む"
    );

    let page2 = ctx
        .get_json(
            t,
            "/dtako/events?date_from=2026-03-01&date_to=2026-03-31&page_size=2&after_driver_cd=B2",
        )
        .await;
    let cds: Vec<&str> = page2["drivers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["driver"]["cd"].as_str().unwrap())
        .collect();
    assert_eq!(cds, vec!["C3"]);
    assert_eq!(page2["next_after_driver_cd"], Value::Null);
    assert_eq!(page2["warnings"], json!([]));

    // 既定 25・下限 1・上限 50 に丸める
    let default = ctx
        .get_json(t, "/dtako/events?date_from=2026-03-01&date_to=2026-03-31")
        .await;
    assert_eq!(default["drivers"].as_array().unwrap().len(), 3);
    assert_eq!(default["next_after_driver_cd"], Value::Null);
    let one = ctx
        .get_json(
            t,
            "/dtako/events?date_from=2026-03-01&date_to=2026-03-31&page_size=0",
        )
        .await;
    assert_eq!(one["next_after_driver_cd"], "A1");
    let many = ctx
        .get_json(
            t,
            "/dtako/events?date_from=2026-03-01&date_to=2026-03-31&page_size=999",
        )
        .await;
    assert_eq!(many["drivers"].as_array().unwrap().len(), 3);

    let (status, body) = ctx
        .get(t, "/dtako/events?date_from=2026-03-01&date_to=2026-04-01")
        .await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::BAD_REQUEST, "range too wide: 32 days (max 31)")
    );
    ctx.finish().await;
}

/// etags: 運行が在る日だけ LIST し、`{tenant}/unko/{運行NO}/KUDGIVT.csv` の ETag を運行NO の順に並べる
/// (同じ日の別のファイルは無視、LIST に無い運行は etag null)。乗務員CD の集合、未分割の運行 (上限 500・総数) と warning。
#[tokio::test(flavor = "multi_thread")]
async fn etags_list_by_day_and_join_with_db() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("etags").await;
    let a = ctx.driver(t, Some("A1"), "TEST-A", false).await;
    let b = ctx.driver(t, Some("B2"), "TEST-B", false).await;
    ctx.op(t, a, "260302080000", 1, "2026-03-02", None, true, None)
        .await;
    ctx.op(t, b, "260302080000", 2, "2026-03-02", None, true, None)
        .await;
    ctx.op(t, b, "260305090000", 1, "2026-03-05", None, true, None)
        .await; // 実体なし
    ctx.op(t, a, "260304070000", 1, "2026-03-04", None, false, None)
        .await; // 未分割
    let text = csv("260302080000", &["1,2026/03/02 08:00:00,201,運転"]);
    ctx.seed(&key(t, "260302080000"), &text);
    ctx.seed(&format!("{t}/unko/260302080000/KUDGURI.csv"), "ignored");
    ctx.seed(&format!("{t}/unko/260306000000/KUDGIVT.csv"), "not in db");

    let body = ctx
        .get_json(
            t,
            "/dtako/events/etags?date_from=2026-03-01&date_to=2026-03-31",
        )
        .await;
    let want = json!({
        "period": { "date_from": "2026-03-01", "date_to": "2026-03-31" },
        "items": [
            { "unko_no": "260302080000", "etag": fake_etag(text.as_bytes()), "driver_cds": ["A1", "B2"] },
            { "unko_no": "260305090000", "etag": null, "driver_cds": ["B2"] },
        ],
        "unsplit": [ { "unko_no": "260304070000", "driver_cd": "A1", "reading_date": "2026-03-04" } ],
        "unsplit_total": 1,
        "warnings": ["未 split の運行 1 件 (has_kudgivt=FALSE、この期間のデータから欠けています)"],
    });
    assert_eq!(body, want);
    let mut lists = ctx.store.list_calls();
    lists.sort();
    assert_eq!(
        lists,
        vec![format!("{t}/unko/260302"), format!("{t}/unko/260305")]
    );

    // 運行が無い期間は LIST しない
    let empty = ctx
        .get_json(
            t,
            "/dtako/events/etags?date_from=2025-01-01&date_to=2025-01-31",
        )
        .await;
    assert_eq!(empty["items"], json!([]));
    assert_eq!(empty["unsplit_total"], 0);
    assert_eq!(empty["warnings"], json!([]));
    assert_eq!(ctx.store.list_calls().len(), 2);

    // 未分割は 500 件で切り、総数はそのまま
    let sql = "INSERT INTO dtako_operations (tenant_id, unko_no, crew_role, reading_date, driver_id, has_kudgivt) \
               SELECT $1, '2604' || lpad(g::text, 8, '0'), 1, DATE '2026-04-10', e.id, FALSE \
               FROM generate_series(1, 501) g, employees e WHERE e.tenant_id = $1 AND e.driver_cd = 'A1'";
    {
        let mut c = ctx.pg.inner.lock().await;
        assert_eq!(exec(&mut c, t, sql).await, 501);
    }
    let many = ctx
        .get_json(
            t,
            "/dtako/events/etags?date_from=2026-04-01&date_to=2026-04-30",
        )
        .await;
    assert_eq!(many["unsplit"].as_array().unwrap().len(), 500);
    assert_eq!(many["unsplit_total"], 501);
    assert_eq!(many["unsplit"][0]["unko_no"], "260400000001");

    let (status, body) = ctx
        .get(
            t,
            "/dtako/events/etags?date_from=2026-03-01&date_to=2026-04-10",
        )
        .await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::BAD_REQUEST, "range too wide: 41 days (max 40)")
    );
    ctx.finish().await;
}

/// etags: 運行NO の先頭 6 文字が数字でなければ日で絞れないのでテナント全体を 1 回 LIST する (警告のログ)。
/// LIST が 1 つでも失敗したら 500 (一部だけ返さない)。
#[tokio::test(flavor = "multi_thread")]
async fn etags_fall_back_to_whole_list_and_fail_closed() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("etags-fallback").await;
    let a = ctx.driver(t, Some("A1"), "TEST-A", false).await;
    ctx.op(t, a, "X60302080000", 1, "2026-03-02", None, true, None)
        .await;
    ctx.op(t, a, "260303080000", 1, "2026-03-03", None, true, None)
        .await;
    let text = csv("X60302080000", &["1,2026/03/02 08:00:00,201,運転"]);
    ctx.seed(&key(t, "X60302080000"), &text);

    let body = ctx
        .get_json(
            t,
            "/dtako/events/etags?date_from=2026-03-01&date_to=2026-03-31",
        )
        .await;
    assert_eq!(body["items"][1]["etag"], fake_etag(text.as_bytes()));
    assert_eq!(ctx.store.list_calls(), vec![format!("{t}/unko/")]);
    assert_eq!(ctx.last_log().0, LogLevel::Warn);

    ctx.store.fail_lists_containing("/unko/", 1);
    let (status, body) = ctx
        .get(
            t,
            "/dtako/events/etags?date_from=2026-03-01&date_to=2026-03-31",
        )
        .await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
    );
    assert_eq!(
        ctx.last_log(),
        (
            LogLevel::Error,
            "dtako-events-etags failed: object store error (list)".to_owned()
        )
    );
    ctx.finish().await;
}

/// DB の失敗は 500 で本文は `internal error`。ログは段の名前と kind だけ (テナント ID を出さない)。
#[tokio::test(flavor = "multi_thread")]
async fn db_failures_are_500_with_fixed_logs() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("events-db").await;
    let a = ctx.driver(t, Some("A1"), "TEST-A", false).await;
    ctx.op(t, a, "260302080000", 1, "2026-03-02", None, true, None)
        .await;
    let single = "/dtako/events?driver_cd=A1&date_from=2026-03-01&date_to=2026-03-02";
    let all = "/dtako/events?date_from=2026-03-01&date_to=2026-03-02";
    let etags = "/dtako/events/etags?date_from=2026-03-01&date_to=2026-03-02";

    // 運行の列 (運行NO など) だけ読めない: 乗務員は引けるが運行の文で落ちる
    let ctx = ctx
        .run_as_superuser(
            "REVOKE SELECT ON alc_api.dtako_operations FROM alc_api_app; \
             GRANT SELECT (tenant_id, driver_id, reading_date, operation_date, has_kudgivt) ON alc_api.dtako_operations TO alc_api_app",
        )
        .await;
    for (path, stage) in [
        (single, "dtako-events operations failed: db ("),
        (all, "dtako-events operations failed: db ("),
        (etags, "dtako-events-etags operations failed: db ("),
    ] {
        let (status, body) = ctx.get(t, path).await;
        assert_eq!(
            (status, body.as_str()),
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error"),
            "{path}"
        );
        let (level, message) = ctx.last_log();
        assert_eq!(level, LogLevel::Error);
        assert!(message.starts_with(stage), "{path}: {message}");
    }

    // 乗務員が読めない: 乗務員の文で落ちる
    let ctx = ctx
        .run_as_superuser("REVOKE SELECT ON alc_api.employees FROM alc_api_app")
        .await;
    for (path, stage) in [
        (single, "dtako-events driver failed: db ("),
        (all, "dtako-events drivers failed: db ("),
        (etags, "dtako-events-etags drivers failed: db ("),
    ] {
        let (status, _) = ctx.get(t, path).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{path}");
        assert!(ctx.last_log().1.starts_with(stage), "{path}");
    }
    for (_, m) in ctx.logs.all() {
        assert!(
            !m.contains(&t.to_string()),
            "ログにテナント ID を出さない: {m}"
        );
    }
    ctx.finish().await;
}

/// 純粋な部分: key の形 (prefix の末尾の `/` は 1 つにする)、CSV の split、日の prefix。
#[test]
fn pure_helpers_match_backend() {
    let t = Uuid::nil();
    assert_eq!(
        kudgivt_key(t, "U1", None),
        format!("{t}/unko/U1/KUDGIVT.csv")
    );
    assert_eq!(kudgivt_key(t, "U1", Some("a/b//")), "a/b/KUDGIVT.csv");
    assert_eq!(kudgivt_key(t, "U1", Some("a/b")), "a/b/KUDGIVT.csv");
    assert_eq!(parse_csv(""), (vec![String::new()], vec![]));
    assert_eq!(
        parse_csv("a, b\n1 ,2\n\n3,4"),
        (
            vec!["a".to_owned(), "b".to_owned()],
            vec![
                vec!["1".to_owned(), "2".to_owned()],
                vec!["3".to_owned(), "4".to_owned()]
            ]
        )
    );
    let nos = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    assert_eq!(
        derive_day_prefixes(&nos(&["260302A", "260301B", "260302C"])),
        Some(nos(&["260301", "260302"]))
    );
    assert_eq!(derive_day_prefixes(&nos(&["26030"])), None);
    assert_eq!(derive_day_prefixes(&nos(&["2603O1"])), None);
    assert_eq!(derive_day_prefixes(&[]), Some(vec![]));
}
