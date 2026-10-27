//! 車両のエラーの起票の口 (W5c-5) を、口から DB まで通して確かめる (native)。
//!
//! DB はテストの process の中で起こす組み込みの PostgreSQL (`embedded/mod.rs`)、保存先・ログは偽物 (`fakes/mod.rs`)。
//! 期待値は rust-alc-api の同じ口 (`alc_dtako::dtako_tickets`) の応答。車両名・メールは作り物。

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
use embedded::{tenant, Embedded, Held, APP_ROLE};
use fakes::{FakeClock, FakeSleeper, FakeStore, Logs};
use futures_util::lock::Mutex;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

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

    async fn json(
        &self,
        t: Uuid,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let (status, text) = self.call(t, method, path, body).await;
        let value = serde_json::from_str(&text).unwrap_or(Value::Null);
        (status, value)
    }

    /// 起票して、応答の本文を返す (201 であること)。
    async fn create(&self, t: Uuid, vehicle: &str, received_at: &str) -> Value {
        let body = json!({
            "source_email_subject": "TEST-SUBJECT",
            "source_email_from": "test@example.invalid",
            "source_email_received_at": received_at,
            "vehicle_name": vehicle,
            "error_kind": "sd_card_error",
        });
        let (status, v) = self
            .json(t, Method::POST, "/dtako/tickets", Some(body))
            .await;
        assert_eq!(status, StatusCode::CREATED, "{v}");
        v
    }
}

/// 起票と結果の反映: source の既定は email、close_token は 64 桁の hex、null の欄は変えずに `scraped` へ進める。
#[tokio::test(flavor = "multi_thread")]
async fn create_and_patch_scraped() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("tickets-create").await;
    let other = ctx.tenant("tickets-create-other").await;

    let v = ctx
        .create(t, "(16) TEST-TRUCK 16", "2026-10-01T09:00:00+09:00")
        .await;
    let id = v["id"].as_str().unwrap().to_owned();
    assert_eq!(v["tenant_id"], json!(t));
    assert_eq!(v["source"], "email");
    assert_eq!(v["status"], "open");
    assert_eq!(v["vehicle_name"], "(16) TEST-TRUCK 16");
    assert_eq!(v["vehicle_code"], Value::Null);
    assert_eq!(v["error_kind"], "sd_card_error");
    assert_eq!(v["source_email_received_at"], "2026-10-01T00:00:00Z");
    assert_eq!(v["scraped_payload"], Value::Null);
    let token = v["close_token"].as_str().unwrap();
    assert_eq!(token.len(), 64);
    assert!(token
        .bytes()
        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    // token は起票ごとに違う
    let v2 = ctx
        .create(t, "TEST-TRUCK 2", "2026-10-02T09:00:00+09:00")
        .await;
    assert_ne!(v2["close_token"], v["close_token"]);

    // source を指定できる
    let body = json!({
        "source": "manual",
        "source_email_received_at": "2026-10-03T00:00:00Z",
        "vehicle_name": "TEST-TRUCK 3",
        "vehicle_code": "V3",
        "error_kind": "other",
        "raw_email_text": "TEST-BODY",
    });
    let (status, v3) = ctx
        .json(t, Method::POST, "/dtako/tickets", Some(body))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(v3["source"], "manual");
    assert_eq!(v3["vehicle_code"], "V3");
    assert_eq!(v3["raw_email_text"], "TEST-BODY");

    // 車両名・エラーの種類が空白だけなら 400 (本文は空)
    for (vehicle, kind) in [("  ", "sd_card_error"), ("TEST-TRUCK", " ")] {
        let body = json!({
            "source_email_received_at": "2026-10-01T00:00:00Z",
            "vehicle_name": vehicle,
            "error_kind": kind,
        });
        let res = ctx
            .call(t, Method::POST, "/dtako/tickets", Some(body))
            .await;
        assert_eq!(res, (StatusCode::BAD_REQUEST, String::new()));
    }

    // 結果の反映 (1 回目は一部の欄だけ、2 回目で残りを足す。null の欄は前の値のまま)
    let path = format!("/dtako/tickets/{id}/scraped");
    let patch = json!({
        "comp_id": "C1",
        "unko_no": "2610010900",
        "operation_started_at": "2026-10-01T08:00:00+09:00",
        "scraped_payload": { "k": 1 },
    });
    let (status, p) = ctx.json(t, Method::PATCH, &path, Some(patch)).await;
    assert_eq!(status, StatusCode::OK, "{p}");
    assert_eq!(p["status"], "scraped");
    assert_eq!(p["comp_id"], "C1");
    assert_eq!(p["operation_started_at"], "2026-09-30T23:00:00Z");
    assert_eq!(p["operation_ended_at"], Value::Null);
    assert_eq!(p["scraped_payload"], json!({ "k": 1 }));
    let patch = json!({
        "operation_ended_at": "2026-10-01T18:00:00Z",
        "settings_zip_r2_key": "TEST/zip",
    });
    let (status, p) = ctx.json(t, Method::PATCH, &path, Some(patch)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(p["comp_id"], "C1");
    assert_eq!(p["unko_no"], "2610010900");
    assert_eq!(p["operation_ended_at"], "2026-10-01T18:00:00Z");
    assert_eq!(p["settings_zip_r2_key"], "TEST/zip");
    assert_eq!(p["scraped_payload"], json!({ "k": 1 }));
    assert_eq!(p["close_token"], v["close_token"]);

    // 無い id・ほかのテナントの id は 404 (本文は空)
    let missing = format!("/dtako/tickets/{}/scraped", Uuid::new_v4());
    let res = ctx.call(t, Method::PATCH, &missing, Some(json!({}))).await;
    assert_eq!(res, (StatusCode::NOT_FOUND, String::new()));
    let res = ctx.call(other, Method::PATCH, &path, Some(json!({}))).await;
    assert_eq!(res, (StatusCode::NOT_FOUND, String::new()));

    assert!(ctx.logs.all().is_empty());
    ctx.finish().await;
}

/// 一覧と 1 件: テナントで絞り、status と車両名の部分一致で絞り、受信の新しい順にページを切る。
#[tokio::test(flavor = "multi_thread")]
async fn list_and_get() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("tickets-list").await;
    let other = ctx.tenant("tickets-list-other").await;
    let a = ctx
        .create(t, "TEST-TRUCK Alpha", "2026-10-01T00:00:00Z")
        .await;
    let b = ctx
        .create(t, "TEST-TRUCK Beta", "2026-10-03T00:00:00Z")
        .await;
    let c = ctx
        .create(t, "TEST-VAN alpha", "2026-10-02T00:00:00Z")
        .await;
    ctx.create(other, "TEST-TRUCK Alpha", "2026-10-04T00:00:00Z")
        .await;
    let b_id = b["id"].as_str().unwrap();
    let path = format!("/dtako/tickets/{b_id}/scraped");
    let (status, b) = ctx.json(t, Method::PATCH, &path, Some(json!({}))).await;
    assert_eq!(status, StatusCode::OK);

    // 既定は 1 ページ目・50 件
    let (status, v) = ctx.json(t, Method::GET, "/dtako/tickets", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        v,
        json!({ "tickets": [b, c, a], "total": 3, "page": 1, "per_page": 50 })
    );

    // 絞り込み (status は一致、車両名は大小文字を問わない部分一致)
    let (_, v) = ctx
        .json(t, Method::GET, "/dtako/tickets?status=scraped", None)
        .await;
    assert_eq!(v["total"], 1);
    assert_eq!(v["tickets"], json!([b]));
    let (_, v) = ctx
        .json(t, Method::GET, "/dtako/tickets?vehicle_name=ALPHA", None)
        .await;
    assert_eq!(v["tickets"], json!([c, a]));
    let (_, v) = ctx
        .json(
            t,
            Method::GET,
            "/dtako/tickets?status=open&vehicle_name=truck",
            None,
        )
        .await;
    assert_eq!(v["tickets"], json!([a]));

    // ページ (件数は 1〜1000 に収め、ページは 1 未満を 1 にする)
    let (_, v) = ctx
        .json(t, Method::GET, "/dtako/tickets?page=2&per_page=2", None)
        .await;
    assert_eq!(
        v,
        json!({ "tickets": [a], "total": 3, "page": 2, "per_page": 2 })
    );
    let (_, v) = ctx
        .json(t, Method::GET, "/dtako/tickets?page=0&per_page=0", None)
        .await;
    assert_eq!(
        v,
        json!({ "tickets": [b], "total": 3, "page": 1, "per_page": 1 })
    );
    let (_, v) = ctx
        .json(t, Method::GET, "/dtako/tickets?per_page=5000", None)
        .await;
    assert_eq!(v["per_page"], 1000);

    // 1 件 (ほかのテナント・無い id は 404、本文は空)
    let (status, v) = ctx
        .json(t, Method::GET, &format!("/dtako/tickets/{b_id}"), None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v, b);
    let res = ctx
        .call(other, Method::GET, &format!("/dtako/tickets/{b_id}"), None)
        .await;
    assert_eq!(res, (StatusCode::NOT_FOUND, String::new()));
    let res = ctx
        .call(
            t,
            Method::GET,
            &format!("/dtako/tickets/{}", Uuid::new_v4()),
            None,
        )
        .await;
    assert_eq!(res, (StatusCode::NOT_FOUND, String::new()));

    assert!(ctx.logs.all().is_empty());
    ctx.finish().await;
}

/// close_token で閉じる: token の前後の空白は落とし、空の閉じた人は null、閉じ済み・無い token は 404。
/// token からテナントを逆引きするので、呼び手のテナントでは絞らない (rust-alc-api と同じ)。
#[tokio::test(flavor = "multi_thread")]
async fn close_by_token() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("tickets-close").await;
    let other = ctx.tenant("tickets-close-other").await;
    let a = ctx.create(t, "TEST-TRUCK A", "2026-10-01T00:00:00Z").await;
    let b = ctx.create(t, "TEST-TRUCK B", "2026-10-02T00:00:00Z").await;
    let a_id = a["id"].as_str().unwrap();

    // 空・長すぎる token は 400 (本文は空)
    let too_long = "a".repeat(129);
    for token in ["", "   ", too_long.as_str()] {
        let body = json!({ "close_token": token });
        let res = ctx
            .call(t, Method::POST, "/dtako/tickets/close", Some(body))
            .await;
        assert_eq!(res, (StatusCode::BAD_REQUEST, String::new()));
    }

    let token = format!(" {} ", a["close_token"].as_str().unwrap());
    let body = json!({ "close_token": token, "closed_by": "TEST-USER" });
    let (status, v) = ctx
        .json(t, Method::POST, "/dtako/tickets/close", Some(body.clone()))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v, json!({ "ticket_id": a_id }));
    let (_, v) = ctx
        .json(t, Method::GET, &format!("/dtako/tickets/{a_id}"), None)
        .await;
    assert_eq!(v["status"], "closed");
    assert_eq!(v["closed_by"], "TEST-USER");
    assert!(v["closed_at"].is_string());

    // 閉じ済み・無い token は 404
    let res = ctx
        .call(t, Method::POST, "/dtako/tickets/close", Some(body))
        .await;
    assert_eq!(res, (StatusCode::NOT_FOUND, String::new()));
    let body = json!({ "close_token": "f".repeat(64) });
    let res = ctx
        .call(t, Method::POST, "/dtako/tickets/close", Some(body))
        .await;
    assert_eq!(res, (StatusCode::NOT_FOUND, String::new()));

    // ほかのテナントから呼んでも、token の起票が閉じる。閉じた人が空なら null
    let body = json!({ "close_token": b["close_token"], "closed_by": "" });
    let (status, v) = ctx
        .json(other, Method::POST, "/dtako/tickets/close", Some(body))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["ticket_id"], b["id"]);
    let (_, v) = ctx
        .json(
            t,
            Method::GET,
            &format!("/dtako/tickets/{}", b["id"].as_str().unwrap()),
            None,
        )
        .await;
    assert_eq!(v["status"], "closed");
    assert_eq!(v["closed_by"], Value::Null);

    assert!(ctx.logs.all().is_empty());
    ctx.finish().await;
}

/// DB の失敗は 500 (本文は空)。原因は `log` に段の名前と kind だけ。
#[tokio::test(flavor = "multi_thread")]
async fn db_failures_are_500() {
    let ctx = Ctx::start().await;
    let t = ctx.tenant("tickets-db-fail").await;
    let ctx = ctx
        .run_as_superuser(
            "REVOKE ALL ON alc_api.dtako_tickets FROM alc_api_app; \
             DROP FUNCTION alc_api.close_dtako_ticket_by_token(TEXT, TEXT)",
        )
        .await;
    let id = Uuid::new_v4();
    let create = json!({
        "source_email_received_at": "2026-10-01T00:00:00Z",
        "vehicle_name": "TEST-TRUCK",
        "error_kind": "sd_card_error",
    });
    let cases: Vec<(Method, String, Option<Value>, &str)> = vec![
        (
            Method::POST,
            "/dtako/tickets".into(),
            Some(create),
            "create",
        ),
        (
            Method::PATCH,
            format!("/dtako/tickets/{id}/scraped"),
            Some(json!({})),
            "scraped",
        ),
        (Method::GET, "/dtako/tickets".into(), None, "list"),
        (Method::GET, format!("/dtako/tickets/{id}"), None, "get"),
        (
            Method::POST,
            "/dtako/tickets/close".into(),
            Some(json!({ "close_token": "TEST-TOKEN" })),
            "close",
        ),
    ];
    for (method, path, body, what) in cases {
        let res = ctx.call(t, method, &path, body).await;
        assert_eq!(
            res,
            (StatusCode::INTERNAL_SERVER_ERROR, String::new()),
            "{what}"
        );
        let logs = ctx.logs.all();
        let (level, message) = logs.last().unwrap();
        assert_eq!(*level, LogLevel::Error);
        assert!(
            message.starts_with(&format!("dtako tickets {what} failed: db (")),
            "{message}"
        );
    }
    assert_eq!(ctx.logs.all().len(), 5);
    ctx.finish().await;
}
