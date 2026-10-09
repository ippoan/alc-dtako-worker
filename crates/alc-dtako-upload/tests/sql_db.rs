//! `repo::sql` の定数を、テストの process の中で起こす組み込みの PostgreSQL で確かめる
//! (ippoan/alc-vein-worker の `tests/sql_db.rs` の形。Refs ippoan/rust-alc-api#725)。
//!
//! 流すのは worker が使うものと同じ実装 ([`alc_dtako_upload::pg`]) — 関数 1 回 = 1 トランザクション、
//! `BEGIN` → `alc_worker_db::SET_TENANT` → `repo::sql` の定数 (型付きの名前なしの文) → `COMMIT`。
//! 写しは持たない。接続だけ native の tokio-postgres でここが張る。
//!
//! DB は `pglite-oxide` (PostgreSQL 17.5) をテストごとに 1 つ起こす (`embedded/mod.rs`)。**docker も外の DB も
//! env も要らず**、`#[ignore]` でもない (`cargo test -p alc-dtako-upload` と `cargo llvm-cov` に入る)。回し方:
//!
//! ```bash
//! bash scripts/fetch-migrations.sh && cargo test -p alc-dtako-upload --test sql_db
//! ```
//!
//! - **migration が未取得 (`.alc-migrations` が無い) なら失敗する** (skip して緑にしない)
//! - ロールは RLS が効く `alc_api_app` (superuser / BYPASSRLS / 表の所有者なら、準備の `Embedded::start` が落とす)
//! - **同時に張れる接続は 1 本。** 別の接続が要るときは、閉じ切ってから次を張る
//! - 直結で流す (PgBouncer を挟まない)
//! - テナント ID はテストごとの乱数

// 共用の土台のうち、このファイルが使わないものが在る
#[allow(dead_code)]
mod embedded;

use std::collections::HashMap;
use std::sync::Arc;

use alc_compare::upload_daily::{compute_daily_hours, DailyHours, DailySegment};
use alc_compare::DayKey;
use alc_csv_parser::kudgivt::KudgivtRow;
use alc_csv_parser::kudguri::KudguriRow;
use alc_csv_parser::operation_changes::OperationMinutes;
use alc_csv_parser::work_segments::EventClass;
use alc_dtako_upload::pg::{
    self, ApplyUploadError, CreateUploadError, OperationInput, PreparedRow,
};
use alc_worker_db::PgClient;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use embedded::{
    employee, exec, kudgivt_flags, operation, operations, rows_json, tenant, upload, Embedded,
    APP_ROLE, TABLES,
};
use serde_json::{json, Value};
use uuid::Uuid;

/// ZIP の key は、テナントを設定した接続で、テナントでも絞って引く。
#[tokio::test(flavor = "multi_thread")]
async fn upload_zip_key_is_scoped_to_tenant() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let a = tenant(c, "Dtako Key Tenant A").await;
    let b = tenant(c, "Dtako Key Tenant B").await;
    let a_up = upload(c, a, "a.zip", "completed", Some("a/key.zip"), 0.0).await;
    let b_up = upload(c, b, "b.zip", "completed", Some("b/key.zip"), 0.0).await;
    let a_no_key = upload(c, a, "pending.zip", "processing", None, 0.0).await;

    // 自テナントの id
    let got = pg::upload_zip_key(c, a, a_up).await.unwrap();
    assert_eq!(got.as_deref(), Some("a/key.zip"));
    let got = pg::upload_zip_key(c, b, b_up).await.unwrap();
    assert_eq!(got.as_deref(), Some("b/key.zip"));
    // 別テナントの id は引けない (どちら向きも)
    assert_eq!(pg::upload_zip_key(c, a, b_up).await.unwrap(), None);
    assert_eq!(pg::upload_zip_key(c, b, a_up).await.unwrap(), None);
    // r2_zip_key が NULL の行・存在しない id
    assert_eq!(pg::upload_zip_key(c, a, a_no_key).await.unwrap(), None);
    assert_eq!(
        pg::upload_zip_key(c, a, Uuid::new_v4()).await.unwrap(),
        None
    );
    held.close().await;
    db.shutdown();
}

/// 渡した運行NO の行だけに印が付き、付けた行の運行NO が (行の数だけ) 返る。別テナントの行は変わらない。
#[tokio::test(flavor = "multi_thread")]
async fn mark_has_kudgivt_marks_only_given_unko_nos_of_the_tenant() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let a = tenant(c, "Dtako Mark Tenant A").await;
    let b = tenant(c, "Dtako Mark Tenant B").await;
    // テナント A: 1001 は乗務員 2 人 (2 行)、1002・1003 は 1 行ずつ
    operation(c, a, "1001", 0, false).await;
    operation(c, a, "1001", 1, false).await;
    operation(c, a, "1002", 0, false).await;
    operation(c, a, "1003", 0, false).await;
    // テナント B にも同じ運行NO
    operation(c, b, "1001", 0, false).await;

    // 空の入力は何もしない
    assert_eq!(
        pg::mark_has_kudgivt(c, a, vec![]).await.unwrap(),
        Vec::<String>::new()
    );
    assert!(kudgivt_flags(c, a).await.iter().all(|(_, _, f)| !f));

    // 1001 (2 行) と 1002、それに行の無い 9999
    let input = vec!["1001".to_owned(), "1002".to_owned(), "9999".to_owned()];
    let mut marked = pg::mark_has_kudgivt(c, a, input.clone()).await.unwrap();
    marked.sort();
    assert_eq!(marked, ["1001", "1001", "1002"]);
    assert_eq!(
        kudgivt_flags(c, a).await,
        [
            ("1001".to_owned(), 0, true),
            ("1001".to_owned(), 1, true),
            ("1002".to_owned(), 0, true),
            ("1003".to_owned(), 0, false),
        ]
    );
    // 別テナントの同じ運行NO は変わらない
    assert_eq!(kudgivt_flags(c, b).await, [("1001".to_owned(), 0, false)]);

    // 印が付いた後にもう一度流しても同じ行が返る (条件に has_kudgivt は無い)
    let mut again = pg::mark_has_kudgivt(c, a, input).await.unwrap();
    again.sort();
    assert_eq!(again, ["1001", "1001", "1002"]);
    held.close().await;
    db.shutdown();
}

/// 101 件以上の運行NO を 1 回 (1 文) で渡せる。
#[tokio::test(flavor = "multi_thread")]
async fn mark_has_kudgivt_takes_more_than_100_in_one_call() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let t = tenant(c, "Dtako Bulk Tenant").await;
    let unko_nos = operations(c, t, "B", 150).await;
    operation(c, t, "untouched", 0, false).await;

    let mut marked = pg::mark_has_kudgivt(c, t, unko_nos.clone()).await.unwrap();
    marked.sort();
    let mut want = unko_nos;
    want.sort();
    assert_eq!(marked, want);
    let flags = kudgivt_flags(c, t).await;
    assert_eq!(flags.len(), 151);
    assert_eq!(flags.iter().filter(|(_, _, f)| *f).count(), 150);
    assert!(flags.contains(&("untouched".to_owned(), 0, false)));
    held.close().await;
    db.shutdown();
}

/// 未分割の運行が在れば、completed で key の在る履歴が新しい順に返る。無ければ空。別テナントのぶんは出ない。
#[tokio::test(flavor = "multi_thread")]
async fn uploads_needing_split_lists_completed_uploads_with_key_newest_first() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let a = tenant(c, "Dtako Split Tenant A").await;
    let b = tenant(c, "Dtako Split Tenant B").await;
    let old = upload(c, a, "old.zip", "completed", Some("a/old.zip"), 300.0).await;
    let new = upload(c, a, "new.zip", "completed", Some("a/new.zip"), 10.0).await;
    let mid = upload(c, a, "mid.zip", "completed", Some("a/mid.zip"), 100.0).await;
    // 出ないもの: completed でない / key が NULL / 別テナント
    upload(
        c,
        a,
        "processing.zip",
        "processing",
        Some("a/processing.zip"),
        5.0,
    )
    .await;
    upload(c, a, "failed.zip", "failed", Some("a/failed.zip"), 6.0).await;
    upload(c, a, "nokey.zip", "completed", None, 7.0).await;
    let b_up = upload(c, b, "b.zip", "completed", Some("b/b.zip"), 1.0).await;

    // 運行が 1 行も無い (= 未分割が無い) → 空
    assert_eq!(pg::uploads_needing_split(c, a).await.unwrap(), []);

    // 未分割の運行が 2 行在っても、履歴は 1 回ずつ (DISTINCT)
    operation(c, a, "2001", 0, false).await;
    operation(c, a, "2002", 0, false).await;
    operation(c, b, "2001", 0, false).await;
    assert_eq!(
        pg::uploads_needing_split(c, a).await.unwrap(),
        [
            (new, "new.zip".to_owned()),
            (mid, "mid.zip".to_owned()),
            (old, "old.zip".to_owned()),
        ]
    );
    assert_eq!(
        pg::uploads_needing_split(c, b).await.unwrap(),
        [(b_up, "b.zip".to_owned())]
    );

    // テナント A の運行を全部分割済みにすると A は空。B は変わらない
    let all = vec!["2001".to_owned(), "2002".to_owned()];
    assert_eq!(pg::mark_has_kudgivt(c, a, all).await.unwrap().len(), 2);
    assert_eq!(pg::uploads_needing_split(c, a).await.unwrap(), []);
    assert_eq!(
        pg::uploads_needing_split(c, b).await.unwrap(),
        [(b_up, "b.zip".to_owned())]
    );
    held.close().await;
    db.shutdown();
}

/// テナントを設定しない素の接続では、この crate が触る表 (`TABLES`) の行は読めない (エラーか 0 行)。
#[tokio::test(flavor = "multi_thread")]
async fn connection_without_tenant_reads_no_rows() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let t = tenant(c, "Dtako No Tenant").await;
    upload(c, t, "a.zip", "completed", Some("a/key.zip"), 0.0).await;
    operation(c, t, "3001", 0, false).await;
    // 残りの表にも 1 行ずつ入れておく (空の表を読んで 0 行、にしない)
    employee(c, t, None, Some("3001"), "TEST-DRIVER", false).await;
    for insert in [
        "INSERT INTO dtako_offices (tenant_id, office_cd, office_name) VALUES ($1, 'O1', 'TEST')",
        "INSERT INTO dtako_vehicles (tenant_id, vehicle_cd, vehicle_name) VALUES ($1, 'V1', 'TEST')",
        "INSERT INTO dtako_event_classifications (tenant_id, event_cd, event_name, classification) VALUES ($1, '201', 'TEST', 'drive')",
        "INSERT INTO dtako_operation_changes (tenant_id, unko_no, crew_role, reason) VALUES ($1, '3001', 0, 'reupload')",
        "INSERT INTO dtako_daily_work_hours (tenant_id, driver_id, work_date) SELECT $1, id, DATE '2026-03-02' FROM employees WHERE tenant_id = $1",
        "INSERT INTO dtako_daily_work_segments (tenant_id, driver_id, work_date, unko_no, start_at, end_at, work_minutes) SELECT $1, id, DATE '2026-03-02', '3001', now(), now(), 0 FROM employees WHERE tenant_id = $1",
        "INSERT INTO dtako_daily_recalc_pending (tenant_id, driver_id, month) SELECT $1, id, DATE '2026-03-01' FROM employees WHERE tenant_id = $1",
    ] {
        assert_eq!(exec(c, t, insert).await, 1);
    }
    for table in TABLES {
        let count = format!("SELECT COUNT(*) AS n FROM {table} WHERE tenant_id = $1");
        assert_eq!(
            rows_json(c, t, &count).await,
            [json!({ "n": 1 })],
            "{table}"
        );
    }
    held.close().await;

    let raw = db.raw(APP_ROLE).await;
    for table in TABLES {
        let sql = format!("SELECT COUNT(*) FROM alc_api.{table}");
        match raw.inner.query_one(&sql, &[]).await {
            Ok(row) => assert_eq!(row.get::<_, i64>(0), 0, "{table}"),
            Err(e) => assert!(e.as_db_error().is_some(), "{table}: {e}"),
        }
    }
    raw.close().await;
    db.shutdown();
}

/// DB のエラーはそのまま `Err` で返り、失敗した transaction は残らない (同じ接続で次の関数が通る)。
#[tokio::test(flavor = "multi_thread")]
async fn db_error_is_returned_and_leaves_no_transaction() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let t = tenant(c, "Dtako DbError Tenant").await;
    operation(c, t, "4001", 0, false).await;

    // NUL バイト入りの文字列は DB が弾く (22021 character_not_in_repertoire)
    let err = pg::mark_has_kudgivt(c, t, vec!["a\0b".to_owned()])
        .await
        .unwrap_err();
    assert_eq!(err.code().map(|s| s.code()), Some("22021"), "{err}");
    // 同じ接続で次が通る (ROLLBACK 済み)
    let marked = pg::mark_has_kudgivt(c, t, vec!["4001".to_owned()]).await;
    assert_eq!(marked.unwrap(), ["4001"]);
    held.close().await;

    // 表の権限を持たないロールで繋ぐと、3 関数とも 42501
    let mut held = db.client("anon").await;
    let c = &mut held.inner;
    let denied = |e: tokio_postgres::Error| e.code().map(|s| s.code().to_owned());
    let err = pg::upload_zip_key(c, t, Uuid::new_v4()).await.unwrap_err();
    assert_eq!(denied(err).as_deref(), Some("42501"));
    let err = pg::mark_has_kudgivt(c, t, vec!["4001".to_owned()])
        .await
        .unwrap_err();
    assert_eq!(denied(err).as_deref(), Some("42501"));
    let err = pg::uploads_needing_split(c, t).await.unwrap_err();
    assert_eq!(denied(err).as_deref(), Some("42501"));
    held.close().await;
    db.shutdown();
}

/// 接続が切れているときは `Err` (DB のエラーではない)。空の入力の `mark_has_kudgivt` は接続に触らない。
#[tokio::test(flavor = "multi_thread")]
async fn closed_connection_is_an_error() {
    let db = Embedded::start().await;
    let mut c = db.client(APP_ROLE).await.sever().await;
    let t = Uuid::new_v4();
    let err = pg::upload_zip_key(&mut c, t, t).await.unwrap_err();
    assert!(err.is_closed(), "{err}");
    let err = pg::mark_has_kudgivt(&mut c, t, vec!["x".to_owned()])
        .await
        .unwrap_err();
    assert!(err.is_closed(), "{err}");
    assert!(pg::uploads_needing_split(&mut c, t)
        .await
        .unwrap_err()
        .is_closed());
    // transaction を開かないので、切れた接続でも通る
    assert_eq!(
        pg::mark_has_kudgivt(&mut c, t, vec![]).await.unwrap(),
        Vec::<String>::new()
    );
    drop(c);
    db.shutdown();
}

// ---- アップロードの取り込みの DB の層 ----

/// KUDGURI の 1 行 (運行日 2026-03-02、出発 `dep_hour`:15:30・帰着 `ret_hour`:15:30)。営業所・車輌は無し。値は作り物。
fn kudguri(
    unko_no: &str,
    crew_role: i32,
    driver_cd: &str,
    dep_hour: u32,
    ret_hour: u32,
) -> KudguriRow {
    let at = |hour| {
        NaiveDate::from_ymd_opt(2026, 3, 2)
            .unwrap()
            .and_hms_opt(hour, 15, 30)
    };
    KudguriRow {
        unko_no: unko_no.into(),
        reading_date: NaiveDate::from_ymd_opt(2026, 3, 3).unwrap(),
        operation_date: NaiveDate::from_ymd_opt(2026, 3, 2),
        office_cd: String::new(),
        office_name: String::new(),
        vehicle_cd: String::new(),
        vehicle_name: String::new(),
        driver_cd: driver_cd.into(),
        driver_name: format!("TEST-DRIVER {driver_cd}"),
        crew_role,
        departure_at: at(dep_hour),
        return_at: at(ret_hour),
        garage_out_at: None,
        garage_in_at: None,
        meter_start: None,
        meter_end: None,
        total_distance: None,
        drive_time_general: None,
        drive_time_highway: None,
        drive_time_bypass: None,
        safety_score: None,
        economy_score: None,
        total_score: None,
        raw_data: json!({}),
    }
}

fn minutes(break_minutes: i32) -> OperationMinutes {
    OperationMinutes {
        drive_minutes: 300,
        cargo_minutes: 20,
        break_minutes,
        rest_minutes: 480,
    }
}

/// KUDGIVT の 1 行 (分類の読み込みが見るのは event_cd と event_name だけ)。
fn kudgivt(event_cd: &str, event_name: &str) -> KudgivtRow {
    let date = NaiveDate::from_ymd_opt(2026, 3, 2).unwrap();
    KudgivtRow {
        unko_no: "T".into(),
        reading_date: date,
        driver_cd: "T".into(),
        driver_name: String::new(),
        crew_role: 1,
        start_at: date.and_hms_opt(8, 0, 0).unwrap(),
        end_at: None,
        event_cd: event_cd.into(),
        event_name: event_name.into(),
        duration_minutes: None,
        section_distance: None,
    }
}

/// 準備 (`prepare_upload`) → 取り込みの本体 (`apply_upload`。日エントリは無し) を、取り込みと同じ順で 1 回流す。
/// `before` は「既に在る」行にだけ渡す前回の分数 (保存先の旧 KUDGIVT が取れなかった場合は `None`)。
async fn import(
    c: &mut PgClient,
    tenant_id: Uuid,
    upload_id: Uuid,
    rows: Vec<KudguriRow>,
    before: Option<OperationMinutes>,
    after: OperationMinutes,
) -> (Vec<PreparedRow>, i32) {
    import_with(c, tenant_id, upload_id, rows, before, after, false).await
}

/// [`import`] の、全部の行の [`OperationInput::recalc`] を指定する形。
async fn import_with(
    c: &mut PgClient,
    tenant_id: Uuid,
    upload_id: Uuid,
    rows: Vec<KudguriRow>,
    before: Option<OperationMinutes>,
    after: OperationMinutes,
    recalc: bool,
) -> (Vec<PreparedRow>, i32) {
    let rows = Arc::new(rows);
    let prepared = pg::prepare_upload(c, tenant_id, rows.clone(), Arc::new(vec![]));
    let prepared = prepared.await.unwrap().rows;
    let inputs: Vec<OperationInput> = prepared
        .iter()
        .map(|p| OperationInput {
            office_id: p.office_id,
            vehicle_id: p.vehicle_id,
            driver_id: p.driver_id,
            before_minutes: before.filter(|_| p.exists),
            after_minutes: after,
            recalc,
        })
        .collect();
    let count = pg::apply_upload(c, tenant_id, upload_id, rows, inputs);
    (prepared, count.await.unwrap())
}

/// 変更記録を、記録した順に読む (`upload_id` は文字列)。
async fn changes(c: &mut PgClient, tenant_id: Uuid) -> Vec<Value> {
    let query = "SELECT unko_no, crew_role, driver_cd, upload_id, reason, before, after \
                 FROM dtako_operation_changes WHERE tenant_id = $1 ORDER BY recorded_at, unko_no, crew_role";
    rows_json(c, tenant_id, query).await
}

/// 乗務員 1 人を、取り込みと同じ解決の手順で引く。
async fn resolve_driver(c: &mut PgClient, tenant_id: Uuid, driver_cd: &str) -> Option<Uuid> {
    let driver_cd = driver_cd.to_owned();
    c.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move { pg::upsert_driver(tx, tenant_id, &driver_cd, "TEST-NEW").await })
    })
    .await
    .unwrap()
}

/// 生存している乗務員の `(code, driver_cd, name)` (名前の順)。
async fn live_employees(c: &mut PgClient, tenant_id: Uuid) -> Vec<Value> {
    let query = "SELECT id, code, driver_cd, name FROM employees \
                 WHERE tenant_id = $1 AND deleted_at IS NULL ORDER BY name";
    rows_json(c, tenant_id, query).await
}

/// 乗務員の解決: `code` の行 → `driver_cd` を埋める → `driver_cd` の行 → 新規 → 一意の制約に当たったら引き直す。
#[tokio::test(flavor = "multi_thread")]
async fn upsert_driver_resolves_in_order() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;

    // (1) code の行が在り driver_cd が NULL → その行を使い、driver_cd を埋める (行は増えない)。2 回目は埋まった行がそのまま当たる
    let t = tenant(c, "Dtako Driver Code Row").await;
    let canonical = employee(c, t, Some("7001"), None, "TEST-CANONICAL", false).await;
    assert_eq!(resolve_driver(c, t, "7001").await, Some(canonical));
    let want =
        json!({ "id": canonical, "code": "7001", "driver_cd": "7001", "name": "TEST-CANONICAL" });
    assert_eq!(live_employees(c, t).await, std::slice::from_ref(&want));
    assert_eq!(resolve_driver(c, t, "7001").await, Some(canonical));
    assert_eq!(live_employees(c, t).await, [want]);

    // (2) code では当たらず、driver_cd の行が在る → その行
    let t = tenant(c, "Dtako Driver Cd Row").await;
    let existing = employee(c, t, None, Some("7002"), "TEST-EXISTING", false).await;
    assert_eq!(resolve_driver(c, t, "7002").await, Some(existing));
    assert_eq!(live_employees(c, t).await.len(), 1);

    // (3) どちらでも当たらない → 新しい行 (code は入れない。名前は渡したもの)
    let t = tenant(c, "Dtako Driver New Row").await;
    let created = resolve_driver(c, t, "7003").await.unwrap();
    let want = json!({ "id": created, "code": null, "driver_cd": "7003", "name": "TEST-NEW" });
    assert_eq!(live_employees(c, t).await, [want]);

    // (4) code の行 (driver_cd NULL) と、同じ driver_cd を持つ別の生存行が同居 → 埋めずに driver_cd の行へ落ちる
    let t = tenant(c, "Dtako Driver No Backfill").await;
    let canonical = employee(c, t, Some("7004"), None, "TEST-A-CANONICAL", false).await;
    let holder = employee(c, t, None, Some("7004"), "TEST-B-HOLDER", false).await;
    assert_eq!(resolve_driver(c, t, "7004").await, Some(holder));
    let want = [
        json!({ "id": canonical, "code": "7004", "driver_cd": null, "name": "TEST-A-CANONICAL" }),
        json!({ "id": holder, "code": null, "driver_cd": "7004", "name": "TEST-B-HOLDER" }),
    ];
    assert_eq!(live_employees(c, t).await, want);

    // (5) 論理削除済みの行は解決の対象にしない → 新しい行が 1 つ
    let t = tenant(c, "Dtako Driver Soft Deleted").await;
    employee(c, t, Some("7005"), None, "TEST-GONE-CODE", true).await;
    employee(c, t, None, Some("7005"), "TEST-GONE-CD", true).await;
    let created = resolve_driver(c, t, "7005").await.unwrap();
    let want = json!({ "id": created, "code": null, "driver_cd": "7005", "name": "TEST-NEW" });
    assert_eq!(live_employees(c, t).await, [want]);

    // (6) INSERT が一意の制約に当たる → エラーにせず引き直す。生存行が無ければ None (行は増えない)。
    //     論理削除済みの行が driver_cd を握っていて、削除済みも含めて一意にする index が在る場合 (このテナントだけの代用の index)
    let t = tenant(c, "Dtako Driver Insert Conflict").await;
    employee(c, t, None, Some("7006"), "TEST-GONE", true).await;
    held.close().await;
    let su = db.superuser().await;
    let index = format!(
        "CREATE UNIQUE INDEX idx_test_driver_cd ON alc_api.employees (tenant_id, driver_cd) \
         WHERE driver_cd IS NOT NULL AND tenant_id = '{t}'::UUID"
    );
    su.inner.batch_execute(&index).await.unwrap();
    su.close().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    assert_eq!(resolve_driver(c, t, "7006").await, None);
    assert_eq!(live_employees(c, t).await, Vec::<Value>::new());

    held.close().await;
    db.shutdown();
}

/// 上げ直しと変更記録: 初回・同じ値・値が変わった・2 人乗務・前回の分数が取れない・同じ zip の中の重複行。
#[tokio::test(flavor = "multi_thread")]
async fn reupload_records_a_change_only_when_the_snapshot_differs() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let t = tenant(c, "Dtako Reupload Tenant").await;
    let first = pg::create_upload(c, t, "first.zip".into()).await.unwrap();
    let second = pg::create_upload(c, t, "second.zip".into()).await.unwrap();
    let not_exists = |p: &[PreparedRow]| p.iter().map(|r| r.exists).collect::<Vec<bool>>();

    // 初回の取り込みは記録しない
    let (prepared, count) = import(
        c,
        t,
        first,
        vec![kudguri("U1", 1, "D-ONE", 3, 22)],
        None,
        minutes(0),
    )
    .await;
    assert_eq!((not_exists(&prepared), count), (vec![false], 1));
    assert_eq!(changes(c, t).await, Vec::<Value>::new());

    // 同じ値の上げ直しは記録しない (運行は「既に在る」)
    let (prepared, count) = import(
        c,
        t,
        second,
        vec![kudguri("U1", 1, "D-ONE", 3, 22)],
        Some(minutes(0)),
        minutes(0),
    )
    .await;
    assert_eq!((not_exists(&prepared), count), (vec![true], 1));
    assert_eq!(changes(c, t).await, Vec::<Value>::new());

    // 休憩の分数が変わった上げ直し → 記録 1 件。before / after は snapshot + 分数
    import(
        c,
        t,
        second,
        vec![kudguri("U1", 1, "D-ONE", 3, 22)],
        Some(minutes(0)),
        minutes(178),
    )
    .await;
    let snapshot = |driver: &str, break_minutes: i32| {
        json!({
            "driver_cd": driver,
            "departure_at": "2026-03-02T03:15:30Z",
            "return_at": "2026-03-02T22:15:30Z",
            "drive_minutes": 300,
            "cargo_minutes": 20,
            "break_minutes": break_minutes,
            "rest_minutes": 480,
        })
    };
    let first_change = json!({
        "unko_no": "U1",
        "crew_role": 1,
        "driver_cd": "D-ONE",
        "upload_id": second,
        "reason": "reupload",
        "before": snapshot("D-ONE", 0),
        "after": snapshot("D-ONE", 178),
    });
    assert_eq!(changes(c, t).await, std::slice::from_ref(&first_change));

    // 2 人乗務: crew_role ごとに比べる。助手 (crew_role 2) の乗務員だけが変わった → 記録は crew_role 2 の 1 件
    let two_crew = vec![
        kudguri("U2", 1, "D-ONE", 3, 22),
        kudguri("U2", 2, "D-ONE", 3, 22),
    ];
    let (prepared, count) = import(c, t, first, two_crew, None, minutes(0)).await;
    assert_eq!((not_exists(&prepared), count), (vec![false, false], 2));
    let swapped = vec![
        kudguri("U2", 1, "D-ONE", 3, 22),
        kudguri("U2", 2, "D-TWO", 3, 22),
    ];
    let (prepared, count) = import(c, t, second, swapped, Some(minutes(0)), minutes(0)).await;
    assert_eq!((not_exists(&prepared), count), (vec![true, true], 2));
    let second_change = json!({
        "unko_no": "U2",
        "crew_role": 2,
        "driver_cd": "D-TWO",
        "upload_id": second,
        "reason": "reupload",
        "before": snapshot("D-ONE", 0),
        "after": snapshot("D-TWO", 0),
    });
    assert_eq!(
        changes(c, t).await,
        [first_change.clone(), second_change.clone()]
    );

    // 前回の分数が取れない (before_minutes = None) → before に印を残し、分数は比べない。乗務員が変わったので記録する
    import(
        c,
        t,
        second,
        vec![kudguri("U1", 1, "D-TWO", 3, 22)],
        None,
        minutes(178),
    )
    .await;
    let third_change = json!({
        "unko_no": "U1",
        "crew_role": 1,
        "driver_cd": "D-TWO",
        "upload_id": second,
        "reason": "reupload",
        "before": {
            "driver_cd": "D-ONE",
            "departure_at": "2026-03-02T03:15:30Z",
            "return_at": "2026-03-02T22:15:30Z",
            "before_kudgivt": "unavailable",
        },
        "after": snapshot("D-TWO", 178),
    });
    let so_far = [first_change, second_change, third_change];
    assert_eq!(changes(c, t).await, so_far);
    // 分数だけが違っても、前回の分数が取れなければ「変わった」としない
    import(
        c,
        t,
        second,
        vec![kudguri("U1", 1, "D-TWO", 3, 22)],
        None,
        minutes(5),
    )
    .await;
    assert_eq!(changes(c, t).await, so_far);

    // 同じ zip の中の重複行: 行の数だけ流し (2)、後の行が残る。後の行から見ると運行は「既に在る」
    let duplicated = vec![
        kudguri("U3", 1, "D-ONE", 3, 22),
        kudguri("U3", 1, "D-ONE", 5, 22),
    ];
    let (prepared, count) = import(c, t, first, duplicated, None, minutes(0)).await;
    assert_eq!((not_exists(&prepared), count), (vec![false, true], 2));
    let departures = "SELECT to_char(departure_at AT TIME ZONE 'UTC', 'HH24:MI:SS') AS dep \
                      FROM dtako_operations WHERE tenant_id = $1 AND unko_no = 'U3'";
    assert_eq!(
        rows_json(c, t, departures).await,
        [json!({ "dep": "05:15:30" })]
    );
    // 後の行は「上げ直し」と同じ扱いになり、出発が違うので記録が 1 件増える
    let mut after = snapshot("D-ONE", 0);
    after["departure_at"] = json!("2026-03-02T05:15:30Z");
    let fourth_change = json!({
        "unko_no": "U3",
        "crew_role": 1,
        "driver_cd": "D-ONE",
        "upload_id": first,
        "reason": "reupload",
        "before": {
            "driver_cd": "D-ONE",
            "departure_at": "2026-03-02T03:15:30Z",
            "return_at": "2026-03-02T22:15:30Z",
            "before_kudgivt": "unavailable",
        },
        "after": after,
    });
    let [first_change, second_change, third_change] = so_far;
    let all = [first_change, second_change, third_change, fourth_change];
    assert_eq!(changes(c, t).await, all);

    held.close().await;
    db.shutdown();
}

/// 運行の 23 列: `KudguriRow` の全 field に別々の値を入れ、列ごとに読み戻して比べる (引数の順の取り違えを捕まえる)。
#[tokio::test(flavor = "multi_thread")]
async fn operation_columns_hold_the_row_values() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let t = tenant(c, "Dtako Columns Tenant").await;
    let upload_id = pg::create_upload(c, t, "columns.zip".into()).await.unwrap();
    let day = |d| NaiveDate::from_ymd_opt(2026, 3, d).unwrap();
    let full = KudguriRow {
        unko_no: "FULL-1".into(),
        reading_date: day(24),
        operation_date: Some(day(23)),
        office_cd: "OF1".into(),
        office_name: "TEST-OFFICE".into(),
        vehicle_cd: "VH1".into(),
        vehicle_name: "TEST-VEHICLE".into(),
        driver_cd: "8001".into(),
        driver_name: "TEST-DRIVER".into(),
        crew_role: 2,
        departure_at: day(23).and_hms_opt(8, 1, 2),
        return_at: day(23).and_hms_opt(17, 3, 4),
        garage_out_at: day(23).and_hms_opt(7, 5, 6),
        garage_in_at: day(23).and_hms_opt(18, 7, 8),
        meter_start: Some(1000.5),
        meter_end: Some(1234.25),
        total_distance: Some(233.75),
        drive_time_general: Some(301),
        drive_time_highway: Some(62),
        drive_time_bypass: Some(23),
        safety_score: Some(91.5),
        economy_score: Some(82.25),
        total_score: Some(73.125),
        raw_data: json!({ "列A": "値A", "n": 1 }),
    };
    // 省ける field が全部空の行 (営業所・車輌・乗務員の cd も空 → DB を引かず、id は NULL)
    let mut empty = kudguri("EMPTY-1", 0, "", 3, 22);
    empty.operation_date = None;
    empty.departure_at = None;
    empty.return_at = None;

    let (prepared, count) = import(c, t, upload_id, vec![full, empty], None, minutes(0)).await;
    assert_eq!(count, 2);
    let none = PreparedRow {
        office_id: None,
        vehicle_id: None,
        driver_id: None,
        exists: false,
    };
    assert_eq!(prepared[1], none);
    assert!(prepared[0].office_id.is_some() && prepared[0].vehicle_id.is_some());

    let utc = |column: &str| {
        format!("to_char(o.{column} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS {column}")
    };
    let query = format!(
        "SELECT o.tenant_id, o.unko_no, o.crew_role, o.reading_date, o.operation_date, \
                o.office_id, o.vehicle_id, o.driver_id, f.office_cd, f.office_name, v.vehicle_cd, v.vehicle_name, \
                e.driver_cd, e.name AS driver_name, e.code AS driver_code, \
                {}, {}, {}, {}, \
                o.meter_start, o.meter_end, o.total_distance, \
                o.drive_time_general, o.drive_time_highway, o.drive_time_bypass, \
                o.safety_score, o.economy_score, o.total_score, o.raw_data, o.r2_key_prefix, o.has_kudgivt \
           FROM dtako_operations o \
           LEFT JOIN dtako_offices f ON f.id = o.office_id \
           LEFT JOIN dtako_vehicles v ON v.id = o.vehicle_id \
           LEFT JOIN employees e ON e.id = o.driver_id \
          WHERE o.tenant_id = $1 ORDER BY o.unko_no DESC",
        utc("departure_at"),
        utc("return_at"),
        utc("garage_out_at"),
        utc("garage_in_at"),
    );
    let stored = rows_json(c, t, &query).await;
    let want_full = json!({
        "tenant_id": t,
        "unko_no": "FULL-1",
        "crew_role": 2,
        "reading_date": "2026-03-24",
        "operation_date": "2026-03-23",
        "office_id": prepared[0].office_id,
        "vehicle_id": prepared[0].vehicle_id,
        "driver_id": prepared[0].driver_id,
        "office_cd": "OF1",
        "office_name": "TEST-OFFICE",
        "vehicle_cd": "VH1",
        "vehicle_name": "TEST-VEHICLE",
        "driver_cd": "8001",
        "driver_name": "TEST-DRIVER",
        "driver_code": null,
        "departure_at": "2026-03-23 08:01:02",
        "return_at": "2026-03-23 17:03:04",
        "garage_out_at": "2026-03-23 07:05:06",
        "garage_in_at": "2026-03-23 18:07:08",
        "meter_start": 1000.5,
        "meter_end": 1234.25,
        "total_distance": 233.75,
        "drive_time_general": 301,
        "drive_time_highway": 62,
        "drive_time_bypass": 23,
        "safety_score": 91.5,
        "economy_score": 82.25,
        "total_score": 73.125,
        "raw_data": { "列A": "値A", "n": 1 },
        "r2_key_prefix": format!("{t}/unko/FULL-1"),
        "has_kudgivt": false,
    });
    let want_empty = json!({
        "tenant_id": t,
        "unko_no": "EMPTY-1",
        "crew_role": 0,
        "reading_date": "2026-03-03",
        "operation_date": null,
        "office_id": null,
        "vehicle_id": null,
        "driver_id": null,
        "office_cd": null,
        "office_name": null,
        "vehicle_cd": null,
        "vehicle_name": null,
        "driver_cd": null,
        "driver_name": null,
        "driver_code": null,
        "departure_at": null,
        "return_at": null,
        "garage_out_at": null,
        "garage_in_at": null,
        "meter_start": null,
        "meter_end": null,
        "total_distance": null,
        "drive_time_general": null,
        "drive_time_highway": null,
        "drive_time_bypass": null,
        "safety_score": null,
        "economy_score": null,
        "total_score": null,
        "raw_data": {},
        "r2_key_prefix": format!("{t}/unko/EMPTY-1"),
        "has_kudgivt": false,
    });
    assert_eq!(stored, [want_full, want_empty]);

    // 同じ cd で名前が変わった営業所・車輌は、同じ行の名前を更新する (id は変わらない)
    let mut renamed = kudguri("FULL-2", 1, "8001", 3, 22);
    renamed.office_cd = "OF1".into();
    renamed.office_name = "TEST-OFFICE-2".into();
    renamed.vehicle_cd = "VH1".into();
    renamed.vehicle_name = "TEST-VEHICLE-2".into();
    let (again, _) = import(c, t, upload_id, vec![renamed], None, minutes(0)).await;
    assert_eq!(again[0].office_id, prepared[0].office_id);
    assert_eq!(again[0].vehicle_id, prepared[0].vehicle_id);
    assert_eq!(again[0].driver_id, prepared[0].driver_id);
    let names = "SELECT (SELECT office_name FROM dtako_offices WHERE tenant_id = $1) AS office, \
                        (SELECT vehicle_name FROM dtako_vehicles WHERE tenant_id = $1) AS vehicle";
    let want = json!({ "office": "TEST-OFFICE-2", "vehicle": "TEST-VEHICLE-2" });
    assert_eq!(rows_json(c, t, names).await, [want]);

    held.close().await;
    db.shutdown();
}

/// 履歴の行 (`id` を除く主な列)。
async fn history(c: &mut PgClient, tenant_id: Uuid) -> Vec<Value> {
    let query =
        "SELECT filename, status, r2_zip_key, error_message, operations_count, uploaded_by \
                 FROM dtako_upload_history WHERE tenant_id = $1 ORDER BY filename";
    rows_json(c, tenant_id, query).await
}

/// テナントの分離・履歴・分類: 同じ cd・同じ運行NO でもテナントごとに別の行。別テナントの履歴には触れない。
#[tokio::test(flavor = "multi_thread")]
async fn upload_stages_stay_inside_the_tenant() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let a = tenant(c, "Dtako Stage Tenant A").await;
    let b = tenant(c, "Dtako Stage Tenant B").await;

    // 履歴: 作る → key を記録する (別テナントの id には 0 行)
    let a_up = pg::create_upload(c, a, "a.zip".into()).await.unwrap();
    let b_up = pg::create_upload(c, b, "b.zip".into()).await.unwrap();
    let fresh = |filename: &str| {
        json!({
            "filename": filename, "status": "processing", "r2_zip_key": null,
            "error_message": null, "operations_count": 0, "uploaded_by": null,
        })
    };
    assert_eq!(history(c, a).await, [fresh("a.zip")]);
    assert_eq!(
        pg::set_upload_zip_key(c, a, b_up, "x/other.zip".into())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        pg::mark_upload_failed(c, a, b_up, "test_label".into())
            .await
            .unwrap(),
        0
    );
    assert_eq!(history(c, b).await, [fresh("b.zip")]);
    assert_eq!(
        pg::set_upload_zip_key(c, a, a_up, "x/a.zip".into())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        pg::upload_zip_key(c, a, a_up).await.unwrap().as_deref(),
        Some("x/a.zip")
    );
    assert_eq!(
        pg::mark_upload_failed(c, a, a_up, "test_label".into())
            .await
            .unwrap(),
        1
    );
    let failed = json!({
        "filename": "a.zip", "status": "failed", "r2_zip_key": "x/a.zip",
        "error_message": "test_label", "operations_count": 0, "uploaded_by": null,
    });
    assert_eq!(history(c, a).await, [failed]);
    assert_eq!(history(c, b).await, [fresh("b.zip")]);

    // 存在しないテナントでは履歴を作れない (ほかの失敗と区別できる)
    let missing = pg::create_upload(c, Uuid::new_v4(), "none.zip".into()).await;
    assert!(
        matches!(missing, Err(CreateUploadError::TenantNotFound)),
        "{missing:?}"
    );

    // 同じ営業所・車輌・乗務員の cd、同じ運行NO を 2 つのテナントで取り込む → 行は別々
    let mut row = kudguri("SAME-1", 1, "9001", 3, 22);
    row.office_cd = "OF1".into();
    row.office_name = "TEST-OFFICE".into();
    row.vehicle_cd = "VH1".into();
    row.vehicle_name = "TEST-VEHICLE".into();
    let (in_a, _) = import(c, a, a_up, vec![row.clone()], None, minutes(0)).await;
    let (in_b, _) = import(c, b, b_up, vec![row.clone()], None, minutes(0)).await;
    assert!(!in_a[0].exists && !in_b[0].exists);
    assert_ne!(in_a[0].office_id, in_b[0].office_id);
    assert_ne!(in_a[0].vehicle_id, in_b[0].vehicle_id);
    assert_ne!(in_a[0].driver_id, in_b[0].driver_id);
    let owned =
        "SELECT (SELECT COUNT(*) FROM dtako_operations WHERE tenant_id = $1) AS operations, \
                        (SELECT COUNT(*) FROM dtako_offices WHERE tenant_id = $1) AS offices, \
                        (SELECT COUNT(*) FROM dtako_vehicles WHERE tenant_id = $1) AS vehicles, \
                        (SELECT COUNT(*) FROM employees WHERE tenant_id = $1) AS employees";
    let one_each = json!({ "operations": 1, "offices": 1, "vehicles": 1, "employees": 1 });
    assert_eq!(
        rows_json(c, a, owned).await,
        std::slice::from_ref(&one_each)
    );
    assert_eq!(rows_json(c, b, owned).await, [one_each]);
    // テナント A の上げ直しは、テナント B の運行を変えず、記録も A にだけ残る
    row.driver_cd = "9002".into();
    import(c, a, a_up, vec![row], Some(minutes(0)), minutes(0)).await;
    assert_eq!(changes(c, a).await.len(), 1);
    assert_eq!(changes(c, b).await, Vec::<Value>::new());
    let driver =
        "SELECT e.driver_cd FROM dtako_operations o JOIN employees e ON e.id = o.driver_id \
                  WHERE o.tenant_id = $1";
    assert_eq!(
        rows_json(c, a, driver).await,
        [json!({ "driver_cd": "9002" })]
    );
    assert_eq!(
        rows_json(c, b, driver).await,
        [json!({ "driver_cd": "9001" })]
    );

    // 分類: 在るものはそのまま読み、KUDGIVT に出てくる未登録のコードを既定の分類で足す (出てきた順に 1 回ずつ)
    for insert in [
        "INSERT INTO dtako_event_classifications (tenant_id, event_cd, event_name, classification) VALUES ($1, '110', 'TEST', 'drive')",
        "INSERT INTO dtako_event_classifications (tenant_id, event_cd, event_name, classification) VALUES ($1, '301', 'TEST', 'work')",
    ] {
        assert_eq!(exec(c, a, insert).await, 1);
    }
    let events = Arc::new(vec![
        kudgivt("202", "TEST-CARGO"),
        kudgivt("999", "TEST-UNKNOWN"),
        kudgivt("110", "TEST-IGNORED-NAME"),
        kudgivt("202", "TEST-CARGO-AGAIN"),
    ]);
    let prepared = pg::prepare_upload(c, a, Arc::new(vec![]), events.clone())
        .await
        .unwrap();
    assert_eq!(prepared.rows, vec![]);
    let mut listed = prepared.classifications.clone();
    listed.sort();
    let pair = |cd: &str, cls: &str| (cd.to_string(), cls.to_string());
    let want = [
        pair("110", "drive"),
        pair("202", "cargo"),
        pair("301", "work"),
        pair("999", "ignore"),
    ];
    assert_eq!(listed, want);
    let map = prepared.classification_map();
    assert_eq!(map.len(), 4);
    assert_eq!(
        (&map["110"], &map["202"]),
        (&EventClass::Drive, &EventClass::Cargo)
    );
    assert_eq!(
        (&map["301"], &map["999"]),
        (&EventClass::Drive, &EventClass::Ignore)
    );
    let stored = "SELECT event_cd, event_name, classification FROM dtako_event_classifications \
                  WHERE tenant_id = $1 ORDER BY event_cd";
    let want_stored = [
        json!({ "event_cd": "110", "event_name": "TEST", "classification": "drive" }),
        json!({ "event_cd": "202", "event_name": "TEST-CARGO", "classification": "cargo" }),
        json!({ "event_cd": "301", "event_name": "TEST", "classification": "work" }),
        json!({ "event_cd": "999", "event_name": "TEST-UNKNOWN", "classification": "ignore" }),
    ];
    assert_eq!(rows_json(c, a, stored).await, want_stored);
    // 2 回目は増えず、同じ一覧が返る。別テナントには 1 行も無い
    let again = pg::prepare_upload(c, a, Arc::new(vec![]), events)
        .await
        .unwrap();
    assert_eq!(again.clone(), prepared);
    assert_eq!(rows_json(c, a, stored).await, want_stored);
    assert_eq!(rows_json(c, b, stored).await, Vec::<Value>::new());

    held.close().await;
    db.shutdown();
}

/// 接続が切れているとき、取り込みの各段は DB の失敗を返す (テナントが無い、とは区別される)。
#[tokio::test(flavor = "multi_thread")]
async fn upload_stages_fail_on_a_closed_connection() {
    let db = Embedded::start().await;
    let mut c = db.client(APP_ROLE).await.sever().await;
    let t = Uuid::new_v4();
    let rows = Arc::new(vec![kudguri("X", 1, "1", 3, 22)]);

    let created = pg::create_upload(&mut c, t, "x.zip".into()).await;
    assert!(
        matches!(&created, Err(CreateUploadError::Db(e)) if e.is_closed()),
        "{created:?}"
    );
    assert!(format!("{created:?}").starts_with("Err(Db("));
    assert!(pg::set_upload_zip_key(&mut c, t, t, "k".into())
        .await
        .unwrap_err()
        .is_closed());
    assert!(pg::mark_upload_failed(&mut c, t, t, "label".into())
        .await
        .unwrap_err()
        .is_closed());
    let prepared = pg::prepare_upload(&mut c, t, rows.clone(), Arc::new(vec![])).await;
    assert!(prepared.unwrap_err().is_closed());
    let input = OperationInput {
        office_id: None,
        vehicle_id: None,
        driver_id: None,
        before_minutes: None,
        after_minutes: minutes(0),
        recalc: false,
    };
    assert_eq!(
        format!("{:?}", input.clone()).len(),
        format!("{input:?}").len()
    );
    // 行と入力の数が違えば、DB に触れる前に失敗する (切れた接続でも DB の失敗にならない)
    let applied = pg::apply_upload(&mut c, t, t, rows.clone(), vec![]).await;
    assert!(
        matches!(applied, Err(ApplyUploadError::LengthMismatch)),
        "{applied:?}"
    );
    assert_eq!(applied.unwrap_err().kind(), "length_mismatch");
    let applied = pg::apply_upload(&mut c, t, t, rows, vec![input]).await;
    assert!(
        matches!(&applied, Err(ApplyUploadError::Db(e)) if e.is_closed()),
        "{applied:?}"
    );
    assert_eq!(applied.unwrap_err().kind(), "closed");
    // 印の読み取り・DB の時刻・保存 (と印を消す) の段も、切れた接続では `Err`
    assert!(pg::recalc_pending_marks(&mut c, t)
        .await
        .unwrap_err()
        .is_closed());
    assert!(pg::db_now(&mut c, t).await.unwrap_err().is_closed());
    drop(c);
    db.shutdown();
}

// ---- 日別の労働時間とセグメントの保存・完了の印 ----

/// 2026-03-`day` の `hour`:15:00 (作り物の日時)。
fn at(day: u32, hour: u32) -> NaiveDateTime {
    let date = NaiveDate::from_ymd_opt(2026, 3, day).unwrap();
    date.and_hms_opt(hour, 15, 0).unwrap()
}

/// 出発・帰着を指定した KUDGURI の 1 行 (crew_role 1)。
fn trip(
    unko_no: &str,
    driver_cd: &str,
    departure: NaiveDateTime,
    ret: NaiveDateTime,
) -> KudguriRow {
    let mut row = kudguri(unko_no, 1, driver_cd, 0, 0);
    row.operation_date = Some(departure.date());
    row.departure_at = Some(departure);
    row.return_at = Some(ret);
    row
}

/// 運行・乗務員・開始・長さ (分) を指定した KUDGIVT の 1 行 (201 運転 / 202 荷役 / 302 休息)。
fn event(
    unko_no: &str,
    driver_cd: &str,
    start: NaiveDateTime,
    event_cd: &str,
    minutes: i32,
) -> KudgivtRow {
    let mut row = kudgivt(event_cd, "TEST-EVENT");
    row.unko_no = unko_no.into();
    row.driver_cd = driver_cd.into();
    row.reading_date = start.date();
    row.start_at = start;
    row.end_at = Some(start + chrono::Duration::minutes(minutes.into()));
    row.duration_minutes = Some(minutes);
    row
}

/// 取り込みと再計算の順で 1 回流す: 準備 → `apply_upload` (運行と印) → `compute_daily_hours` (backend と同じ関数) →
/// `save_daily_hours_in_tx` (消す対象は日エントリの運行NO。印は消さない)。返すのは流した行数と保存の結果。
/// `extra` は、計算の出力に足す日エントリ (計算からは出てこない形を保存に通すため)。
async fn import_daily(
    c: &mut PgClient,
    tenant_id: Uuid,
    upload_id: Uuid,
    rows: Vec<KudguriRow>,
    events: Vec<KudgivtRow>,
    extra: Vec<(DayKey, DailyHours)>,
) -> (i32, Result<(), tokio_postgres::Error>) {
    let (rows, events) = (Arc::new(rows), Arc::new(events));
    let prepared = pg::prepare_upload(c, tenant_id, rows.clone(), events.clone());
    let prepared = prepared.await.unwrap();
    let classifications = prepared.classification_map();
    let mut daily = compute_daily_hours(&rows, &events, &classifications, &HashMap::new());
    daily.extend(extra);
    let inputs: Vec<OperationInput> = prepared
        .rows
        .iter()
        .map(|p| OperationInput {
            office_id: p.office_id,
            vehicle_id: p.vehicle_id,
            driver_id: p.driver_id,
            before_minutes: None,
            after_minutes: minutes(0),
            recalc: false,
        })
        .collect();
    let count = pg::apply_upload(c, tenant_id, upload_id, rows, inputs);
    let count = count.await.unwrap();
    let all_unko_nos = Arc::new(pg::daily_unko_nos(&daily));
    let saved = pg::save_daily_hours_in_tx(c, tenant_id, daily, all_unko_nos, None);
    (count, saved.await)
}

/// 日別の行 (乗務員は名前で。乗務員・日・開始時刻の順)。
async fn daily_rows(c: &mut PgClient, tenant_id: Uuid) -> Vec<Value> {
    let query = "SELECT e.name AS driver, h.work_date, h.start_time, h.total_work_minutes, h.total_drive_minutes, \
                 h.total_rest_minutes, h.late_night_minutes, h.drive_minutes, h.cargo_minutes, h.total_distance, \
                 h.operation_count, h.unko_nos, h.overlap_drive_minutes, h.overlap_cargo_minutes, \
                 h.overlap_break_minutes, h.overlap_restraint_minutes, h.ot_late_night_minutes \
                 FROM dtako_daily_work_hours h JOIN employees e ON e.id = h.driver_id \
                 WHERE h.tenant_id = $1 ORDER BY e.name, h.work_date, h.start_time";
    rows_json(c, tenant_id, query).await
}

/// セグメントの行 (乗務員は名前で、日時は UTC の文字列で。乗務員・日・開始・番号の順)。
async fn segment_rows(c: &mut PgClient, tenant_id: Uuid) -> Vec<Value> {
    let query = "SELECT e.name AS driver, s.work_date, s.unko_no, s.segment_index, \
                 to_char(s.start_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS start_at, \
                 to_char(s.end_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS end_at, \
                 s.work_minutes, s.labor_minutes, s.late_night_minutes, s.drive_minutes, s.cargo_minutes \
                 FROM dtako_daily_work_segments s JOIN employees e ON e.id = s.driver_id \
                 WHERE s.tenant_id = $1 ORDER BY e.name, s.work_date, s.start_at, s.segment_index";
    rows_json(c, tenant_id, query).await
}

/// 運行の `(unko_no, 乗務員の名前, 出発)` (運行NO の順)。
async fn operation_rows(c: &mut PgClient, tenant_id: Uuid) -> Vec<Value> {
    let query = "SELECT o.unko_no, e.name AS driver, to_char(o.departure_at AT TIME ZONE 'UTC', 'DD HH24:MI') AS departure \
                 FROM dtako_operations o LEFT JOIN employees e ON e.id = o.driver_id \
                 WHERE o.tenant_id = $1 ORDER BY o.unko_no";
    rows_json(c, tenant_id, query).await
}

/// 履歴の `(filename, status, operations_count)` (filename の順)。
async fn completion(c: &mut PgClient, tenant_id: Uuid) -> Vec<Value> {
    let query = "SELECT filename, status, operations_count FROM dtako_upload_history \
                 WHERE tenant_id = $1 ORDER BY filename";
    rows_json(c, tenant_id, query).await
}

/// 日別の要再計算の印の `(乗務員の名前, 月)` (月・名前の順)。
async fn marks(c: &mut PgClient, tenant_id: Uuid) -> Vec<Value> {
    let query = "SELECT e.name AS driver, p.month FROM dtako_daily_recalc_pending p \
                 JOIN employees e ON e.id = p.driver_id WHERE p.tenant_id = $1 ORDER BY p.month, e.name";
    rows_json(c, tenant_id, query).await
}

/// 2 日にまたがる 1 運行 (運転 420 分・荷役 60 分 → 休息 600 分 → 運転 720 分)。日エントリは 2 つ (03-02 と 03-03)。
fn two_day_trip(unko_no: &str, driver_cd: &str) -> (Vec<KudguriRow>, Vec<KudgivtRow>) {
    let mut row = trip(unko_no, driver_cd, at(2, 6), at(3, 12));
    row.total_distance = Some(123.5);
    let events = vec![
        event(unko_no, driver_cd, at(2, 6), "201", 420),
        event(unko_no, driver_cd, at(2, 13), "202", 60),
        event(unko_no, driver_cd, at(2, 14), "302", 600),
        event(unko_no, driver_cd, at(3, 0), "201", 720),
    ];
    (vec![row], events)
}

/// 1 日に収まる 1 運行 (運転 480 分)。日エントリは 1 つ (03-02 の 09:15)。
fn one_day_trip(unko_no: &str, driver_cd: &str) -> (Vec<KudguriRow>, Vec<KudgivtRow>) {
    let mut row = trip(unko_no, driver_cd, at(2, 9), at(2, 17));
    row.total_distance = Some(80.25);
    let events = vec![event(unko_no, driver_cd, at(2, 9), "201", 480)];
    (vec![row], events)
}

/// 全部の field に別々の値を入れた日エントリ (引数の順の取り違えを捕まえる。計算からは出てこない値)。
fn distinct_hours() -> DailyHours {
    let segment = |unko_no: &str, index: i32, hour: u32, base: i32| DailySegment {
        unko_no: unko_no.into(),
        segment_index: index,
        start_at: at(5, hour),
        end_at: at(5, hour + 2),
        work_minutes: base + 1,
        labor_minutes: base + 2,
        late_night_minutes: base + 3,
        drive_minutes: base + 4,
        cargo_minutes: base + 5,
    };
    DailyHours {
        total_work_minutes: 101,
        total_labor_minutes: 102,
        late_night_minutes: 103,
        drive_minutes: 104,
        cargo_minutes: 105,
        total_distance: 106.5,
        operation_count: 107,
        unko_nos: vec!["HAND-1".into(), "HAND-2".into()],
        segments: vec![segment("HAND-1", 0, 4, 200), segment("HAND-2", 1, 7, 210)],
        rest_event_minutes: 108,
        overlap_drive_minutes: 109,
        overlap_cargo_minutes: 110,
        overlap_break_minutes: 111,
        overlap_restraint_minutes: 112,
        ot_late_night_minutes: 7,
    }
}

/// 日別の保存: 18 列・12 列を読み戻す。上げ直しで、その運行の古い日別とセグメントが消えて入れ替わる。履歴に完了の印が付く。
#[tokio::test(flavor = "multi_thread")]
async fn daily_hours_are_saved_and_replaced_on_reupload() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let t = tenant(c, "Dtako Daily Tenant").await;
    employee(c, t, None, Some("D-ONE"), "TEST-ONE", false).await;
    employee(c, t, None, Some("D-TWO"), "TEST-HAND", false).await;
    let first = pg::create_upload(c, t, "first.zip".into()).await.unwrap();
    let second = pg::create_upload(c, t, "second.zip".into()).await.unwrap();

    // 前から在る行: (a) 同じ (乗務員, 日, 開始時刻) の日別と、その日のセグメント (b) 同じ運行NO を持つ別の日の日別とセグメント
    // (c) どれにも当たらない日別とセグメント。(a) (b) は消え、(c) は残る
    for seed in [
        "INSERT INTO dtako_daily_work_hours (tenant_id, driver_id, work_date, start_time, total_work_minutes, unko_nos) \
         SELECT $1, id, d::date, s::time, 1, u::text[] FROM employees, (VALUES \
         ('2026-03-02', '06:15:00', '{OLD-9}'), ('2026-03-09', '00:00:00', '{OLD-9,DAY-1}'), ('2026-03-10', '00:00:00', '{OLD-9}') \
         ) v(d, s, u) WHERE tenant_id = $1 AND driver_cd = 'D-ONE'",
        "INSERT INTO dtako_daily_work_segments (tenant_id, driver_id, work_date, unko_no, start_at, end_at, work_minutes) \
         SELECT $1, id, d::date, u, (d || ' 01:00:00+00')::timestamptz, (d || ' 02:00:00+00')::timestamptz, 1 FROM employees, (VALUES \
         ('2026-03-02', 'OLD-9'), ('2026-03-09', 'DAY-1'), ('2026-03-10', 'OLD-9') \
         ) v(d, u) WHERE tenant_id = $1 AND driver_cd = 'D-ONE'",
    ] {
        assert_eq!(exec(c, t, seed).await, 3);
    }
    let kept_day = json!({
        "driver": "TEST-ONE", "work_date": "2026-03-10", "start_time": "00:00:00", "total_work_minutes": 1,
        "total_drive_minutes": null, "total_rest_minutes": null, "late_night_minutes": 0, "drive_minutes": 0,
        "cargo_minutes": 0, "total_distance": null, "operation_count": 0, "unko_nos": ["OLD-9"],
        "overlap_drive_minutes": 0, "overlap_cargo_minutes": 0, "overlap_break_minutes": 0,
        "overlap_restraint_minutes": 0, "ot_late_night_minutes": 0,
    });
    let kept_segment = json!({
        "driver": "TEST-ONE", "work_date": "2026-03-10", "unko_no": "OLD-9", "segment_index": 0,
        "start_at": "2026-03-10 01:00:00", "end_at": "2026-03-10 02:00:00", "work_minutes": 1,
        "labor_minutes": 0, "late_night_minutes": 0, "drive_minutes": 0, "cargo_minutes": 0,
    });

    // 1 回目: 計算の出力 (2 日ぶん) と、全部の field が別々の値の日エントリ (別の乗務員)
    let (rows, events) = two_day_trip("DAY-1", "D-ONE");
    let hand_key = (
        "D-TWO".to_string(),
        at(5, 4).date(),
        NaiveTime::from_hms_opt(4, 5, 6).unwrap(),
    );
    let extra = vec![(hand_key, distinct_hours())];
    let count = import_daily(c, t, first, rows, events, extra).await;
    assert_eq!((count.0, count.1.unwrap()), (1, ()));
    // 保存する 2 つの値は method の値: total_drive_minutes = 労働の合計 (102)、late_night_minutes = 103 - 7
    let hand_day = json!({
        "driver": "TEST-HAND", "work_date": "2026-03-05", "start_time": "04:05:06", "total_work_minutes": 101,
        "total_drive_minutes": 102, "total_rest_minutes": 108, "late_night_minutes": 96, "drive_minutes": 104,
        "cargo_minutes": 105, "total_distance": 106.5, "operation_count": 107, "unko_nos": ["HAND-1", "HAND-2"],
        "overlap_drive_minutes": 109, "overlap_cargo_minutes": 110, "overlap_break_minutes": 111,
        "overlap_restraint_minutes": 112, "ot_late_night_minutes": 7,
    });
    let hand_segments = [
        json!({
            "driver": "TEST-HAND", "work_date": "2026-03-05", "unko_no": "HAND-1", "segment_index": 0,
            "start_at": "2026-03-05 04:15:00", "end_at": "2026-03-05 06:15:00", "work_minutes": 201,
            "labor_minutes": 202, "late_night_minutes": 203, "drive_minutes": 204, "cargo_minutes": 205,
        }),
        json!({
            "driver": "TEST-HAND", "work_date": "2026-03-05", "unko_no": "HAND-2", "segment_index": 1,
            "start_at": "2026-03-05 07:15:00", "end_at": "2026-03-05 09:15:00", "work_minutes": 211,
            "labor_minutes": 212, "late_night_minutes": 213, "drive_minutes": 214, "cargo_minutes": 215,
        }),
    ];
    let first_day = json!({
        "driver": "TEST-ONE", "work_date": "2026-03-02", "start_time": "06:15:00", "total_work_minutes": 480,
        "total_drive_minutes": 480, "total_rest_minutes": 600, "late_night_minutes": 0, "drive_minutes": 420,
        "cargo_minutes": 60, "total_distance": 123.5, "operation_count": 1, "unko_nos": ["DAY-1"],
        "overlap_drive_minutes": 360, "overlap_cargo_minutes": 0, "overlap_break_minutes": 0,
        "overlap_restraint_minutes": 360, "ot_late_night_minutes": 0,
    });
    let second_day = json!({
        "driver": "TEST-ONE", "work_date": "2026-03-03", "start_time": "00:15:00", "total_work_minutes": 720,
        "total_drive_minutes": 720, "total_rest_minutes": 0, "late_night_minutes": 285, "drive_minutes": 720,
        "cargo_minutes": 0, "total_distance": 0, "operation_count": 1, "unko_nos": ["DAY-1"],
        "overlap_drive_minutes": 0, "overlap_cargo_minutes": 0, "overlap_break_minutes": 0,
        "overlap_restraint_minutes": 0, "ot_late_night_minutes": 0,
    });
    let want_days = [&hand_day, &first_day, &second_day, &kept_day];
    assert_eq!(daily_rows(c, t).await.iter().collect::<Vec<_>>(), want_days);
    let first_segment = json!({
        "driver": "TEST-ONE", "work_date": "2026-03-02", "unko_no": "DAY-1", "segment_index": 0,
        "start_at": "2026-03-02 06:15:00", "end_at": "2026-03-02 14:15:00", "work_minutes": 480,
        "labor_minutes": 480, "late_night_minutes": 0, "drive_minutes": 420, "cargo_minutes": 60,
    });
    let second_segment = json!({
        "driver": "TEST-ONE", "work_date": "2026-03-03", "unko_no": "DAY-1", "segment_index": 0,
        "start_at": "2026-03-03 00:15:00", "end_at": "2026-03-03 12:15:00", "work_minutes": 720,
        "labor_minutes": 720, "late_night_minutes": 285, "drive_minutes": 720, "cargo_minutes": 0,
    });
    let [hand_a, hand_b] = &hand_segments;
    let want_segments = [
        hand_a,
        hand_b,
        &first_segment,
        &second_segment,
        &kept_segment,
    ];
    assert_eq!(
        segment_rows(c, t).await.iter().collect::<Vec<_>>(),
        want_segments
    );
    let done = |filename: &str, status: &str, n: i32| json!({ "filename": filename, "status": status, "operations_count": n });
    let after_first = [
        done("first.zip", "completed", 1),
        done("second.zip", "processing", 0),
    ];
    assert_eq!(completion(c, t).await, after_first);

    // 上げ直し: 同じ運行が 1 日に収まる形に変わった → その運行の古い 2 日ぶんが消えて、新しい 1 日ぶんに入れ替わる。
    // この zip に出てこない乗務員 (TEST-HAND) の行と、当たらない行は、そのまま
    let (rows, events) = one_day_trip("DAY-1", "D-ONE");
    let count = import_daily(c, t, second, rows, events, vec![]).await;
    assert_eq!((count.0, count.1.unwrap()), (1, ()));
    let new_day = json!({
        "driver": "TEST-ONE", "work_date": "2026-03-02", "start_time": "09:15:00", "total_work_minutes": 480,
        "total_drive_minutes": 480, "total_rest_minutes": 0, "late_night_minutes": 0, "drive_minutes": 480,
        "cargo_minutes": 0, "total_distance": 80.25, "operation_count": 1, "unko_nos": ["DAY-1"],
        "overlap_drive_minutes": 0, "overlap_cargo_minutes": 0, "overlap_break_minutes": 0,
        "overlap_restraint_minutes": 0, "ot_late_night_minutes": 0,
    });
    let new_segment = json!({
        "driver": "TEST-ONE", "work_date": "2026-03-02", "unko_no": "DAY-1", "segment_index": 0,
        "start_at": "2026-03-02 09:15:00", "end_at": "2026-03-02 17:15:00", "work_minutes": 480,
        "labor_minutes": 480, "late_night_minutes": 0, "drive_minutes": 480, "cargo_minutes": 0,
    });
    let want_days = [&hand_day, &new_day, &kept_day];
    assert_eq!(daily_rows(c, t).await.iter().collect::<Vec<_>>(), want_days);
    let want_segments = [hand_a, hand_b, &new_segment, &kept_segment];
    assert_eq!(
        segment_rows(c, t).await.iter().collect::<Vec<_>>(),
        want_segments
    );
    let after_second = [
        done("first.zip", "completed", 1),
        done("second.zip", "completed", 1),
    ];
    assert_eq!(completion(c, t).await, after_second);

    held.close().await;
    db.shutdown();
}

/// 同じ乗務員・同じ日に日エントリが 2 つ在っても、両方のセグメントが残る (何度流しても同じ)。乗務員 id が引けない CD と
/// 空の CD の日エントリは保存しない。運行の乗務員 (`upsert_driver`) と日別の乗務員 (`get_employee_id_by_driver_cd`) は別に引く。
#[tokio::test(flavor = "multi_thread")]
async fn daily_hours_keep_both_entries_of_a_day_and_skip_unresolved_drivers() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let t = tenant(c, "Dtako Daily Order Tenant").await;
    // 乗務員CD が、ある行の `code` と、別の行の `driver_cd` の両方に在る
    employee(c, t, Some("D-SPLIT"), None, "TEST-BY-CODE", false).await;
    employee(c, t, None, Some("D-SPLIT"), "TEST-BY-DRIVER-CD", false).await;
    let upload_id = pg::create_upload(c, t, "order.zip".into()).await.unwrap();

    // 1 運行の中に休息が在り、同じ日 (03-04) に日エントリが 2 つ出る (01:15 と 15:15)。もう 1 運行は乗務員CD が空
    let rows = vec![
        trip("TWICE-1", "D-SPLIT", at(4, 1), at(4, 23)),
        trip("EMPTY-1", "", at(5, 8), at(5, 12)),
    ];
    let events = vec![
        event("TWICE-1", "D-SPLIT", at(4, 1), "201", 240),
        event("TWICE-1", "D-SPLIT", at(4, 5), "302", 600),
        event("TWICE-1", "D-SPLIT", at(4, 15), "201", 480),
        event("EMPTY-1", "", at(5, 8), "201", 240),
    ];
    let ghost_key = ("D-GHOST".to_string(), at(6, 0).date(), NaiveTime::MIN);
    let extra = vec![(ghost_key, distinct_hours())];
    let days = "SELECT e.name AS driver, h.work_date, h.start_time, h.total_work_minutes \
                FROM dtako_daily_work_hours h JOIN employees e ON e.id = h.driver_id \
                WHERE h.tenant_id = $1 ORDER BY h.work_date, h.start_time";
    let segments = "SELECT e.name AS driver, s.work_date, s.unko_no, s.segment_index, s.work_minutes, to_char(s.start_at AT TIME ZONE 'UTC', 'HH24:MI') AS start_at \
                    FROM dtako_daily_work_segments s JOIN employees e ON e.id = s.driver_id \
                    WHERE s.tenant_id = $1 ORDER BY s.start_at";
    let want_days = [
        json!({ "driver": "TEST-BY-CODE", "work_date": "2026-03-04", "start_time": "01:15:00", "total_work_minutes": 240 }),
        json!({ "driver": "TEST-BY-CODE", "work_date": "2026-03-04", "start_time": "15:15:00", "total_work_minutes": 480 }),
    ];
    let want_segments = [
        json!({ "driver": "TEST-BY-CODE", "work_date": "2026-03-04", "unko_no": "TWICE-1", "segment_index": 0, "work_minutes": 240, "start_at": "01:15" }),
        json!({ "driver": "TEST-BY-CODE", "work_date": "2026-03-04", "unko_no": "TWICE-1", "segment_index": 0, "work_minutes": 480, "start_at": "15:15" }),
    ];
    let want_operations = [
        json!({ "unko_no": "EMPTY-1", "driver": null, "departure": "05 08:15" }),
        json!({ "unko_no": "TWICE-1", "driver": "TEST-BY-DRIVER-CD", "departure": "04 01:15" }),
    ];
    // 2 回流す (2 回目は上げ直し)。どちらの後も同じ
    for _ in 0..2 {
        let count =
            import_daily(c, t, upload_id, rows.clone(), events.clone(), extra.clone()).await;
        assert_eq!((count.0, count.1.unwrap()), (2, ()));
        // 日別は `code` の行に、運行は `driver_cd` の行に付く。空の CD と、id が引けない CD (D-GHOST) の日エントリは無い
        assert_eq!(rows_json(c, t, days).await, want_days);
        // 同じ日の 2 つめの日エントリを保存しても、1 つめのセグメントは消えない
        assert_eq!(rows_json(c, t, segments).await, want_segments);
        assert_eq!(operation_rows(c, t).await, want_operations);
        // 日別の保存は乗務員を作らない・`driver_cd` を埋めない
        assert_eq!(live_employees(c, t).await.len(), 2);
    }
    let completed =
        json!({ "filename": "order.zip", "status": "completed", "operations_count": 2 });
    assert_eq!(completion(c, t).await, [completed]);

    held.close().await;
    db.shutdown();
}

/// 別テナントの日別・セグメント・履歴・印には触れない。取り込みの段の途中で失敗したら、運行の入れ替えも印も履歴も元のまま、
/// 保存の段の途中で失敗したら、日別も印も元のまま (どちらも transaction は 1 つ)。
#[tokio::test(flavor = "multi_thread")]
async fn daily_hours_stay_inside_the_tenant_and_roll_back_with_the_operations() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let a = tenant(c, "Dtako Daily Tenant A").await;
    let b = tenant(c, "Dtako Daily Tenant B").await;
    let a_up = pg::create_upload(c, a, "a1.zip".into()).await.unwrap();
    let b_up = pg::create_upload(c, b, "b1.zip".into()).await.unwrap();
    let b_pending = pg::create_upload(c, b, "b2.zip".into()).await.unwrap();

    // 同じ運行NO・同じ乗務員CD を 2 つのテナントで取り込む
    for (tenant_id, upload_id) in [(a, a_up), (b, b_up)] {
        let (rows, events) = one_day_trip("SAME-1", "D-ONE");
        let count = import_daily(c, tenant_id, upload_id, rows, events, vec![]).await;
        assert_eq!((count.0, count.1.unwrap()), (1, ()));
    }
    let b_days = daily_rows(c, b).await;
    let b_segments = segment_rows(c, b).await;
    let b_history = completion(c, b).await;
    assert_eq!((b_days.len(), b_segments.len()), (1, 1));
    let done = |filename: &str, status: &str, n: i32| json!({ "filename": filename, "status": status, "operations_count": n });
    assert_eq!(
        b_history,
        [
            done("b1.zip", "completed", 1),
            done("b2.zip", "processing", 0)
        ]
    );

    // テナント A が上げ直す (2 日ぶんに変わる)。upload_id にテナント B の履歴の id を渡しても、B の履歴は completed にならない
    let (rows, events) = two_day_trip("SAME-1", "D-ONE");
    let count = import_daily(c, a, b_pending, rows, events, vec![]).await;
    assert_eq!((count.0, count.1.unwrap()), (1, ()));
    assert_eq!(
        (daily_rows(c, a).await.len(), segment_rows(c, a).await.len()),
        (2, 2)
    );
    assert_eq!(daily_rows(c, b).await, b_days);
    assert_eq!(segment_rows(c, b).await, b_segments);
    assert_eq!(completion(c, b).await, b_history);
    assert_eq!(completion(c, a).await, [done("a1.zip", "completed", 1)]);

    // 印: どちらのテナントにも、取り込んだ乗務員 × 月 に 1 つずつ (上げ直しは同じ印に当たって増えない)
    let one_mark = || json!([{ "driver": "TEST-DRIVER D-ONE", "month": "2026-03-01" }]);
    assert_eq!(json!(marks(c, a).await), one_mark());
    assert_eq!(json!(marks(c, b).await), one_mark());

    // 保存の段の途中の失敗: 日別を入れた後の、セグメントの INSERT が落ちる (文字列に NUL) → 日別・セグメントも、
    // 同じ transaction で消すはずだった印も元のまま
    let a_failing = pg::create_upload(c, a, "a2.zip".into()).await.unwrap();
    let a_days = daily_rows(c, a).await;
    let a_segments = segment_rows(c, a).await;
    let mut broken = distinct_hours();
    broken.segments[1].unko_no = "BAD\0".into();
    let broken_key = ("D-ONE".to_string(), at(8, 0).date(), NaiveTime::MIN);
    let daily = HashMap::from([(broken_key, broken)]);
    let clear = pg::PendingClear {
        month: NaiveDate::from_ymd_opt(2026, 3, 1).unwrap(),
        driver_id: None,
        driver_cd: Some("D-ONE".into()),
        before: Utc::now() + Duration::days(1),
    };
    let unko_nos = Arc::new(vec!["SAME-1".to_owned()]);
    let failed =
        pg::save_daily_hours_in_tx(c, a, daily, unko_nos.clone(), Some(clear.clone())).await;
    assert!(!failed.unwrap_err().is_closed());
    assert_eq!(daily_rows(c, a).await, a_days);
    assert_eq!(segment_rows(c, a).await, a_segments);
    assert_eq!(json!(marks(c, a).await), one_mark());
    // 落ちなければ、乗務員CD で指した印が消える (テナント B の同じ乗務員CD の印は残る)
    let saved = pg::save_daily_hours_in_tx(c, a, HashMap::new(), unko_nos, Some(clear)).await;
    saved.unwrap();
    assert_eq!(marks(c, a).await, Vec::<Value>::new());
    assert_eq!(json!(marks(c, b).await), one_mark());

    // 取り込みの段の途中の失敗: 1 行目を入れ替えた後、2 行目の INSERT が落ちる (raw_data に JSONB が受けない NUL) →
    // 運行・変更記録・印・履歴は元のまま (1 行目の変化の印も、新しい 2 行目の印も付かない)
    let a_operations = operation_rows(c, a).await;
    let a_changes = changes(c, a).await;
    assert_eq!(a_changes.len(), 1);
    let departed =
        json!({ "unko_no": "SAME-1", "driver": "TEST-DRIVER D-ONE", "departure": "02 06:15" });
    assert_eq!(a_operations, [departed]);
    let (mut rows, _) = one_day_trip("SAME-1", "D-ONE");
    let mut bad = kudguri("NEW-2", 1, "D-ONE", 3, 22);
    bad.raw_data = json!({ "x": "\u{0}" });
    rows.push(bad);
    let driver_id = resolve_driver(c, a, "D-ONE").await;
    let input = |_| OperationInput {
        office_id: None,
        vehicle_id: None,
        driver_id,
        before_minutes: None,
        after_minutes: minutes(0),
        recalc: false,
    };
    let inputs = (0..2).map(input).collect();
    let failed = pg::apply_upload(c, a, a_failing, Arc::new(rows), inputs).await;
    assert!(
        matches!(&failed, Err(ApplyUploadError::Db(e)) if !e.is_closed()),
        "{failed:?}"
    );
    assert_eq!(operation_rows(c, a).await, a_operations);
    assert_eq!(changes(c, a).await, a_changes);
    assert_eq!(marks(c, a).await, Vec::<Value>::new());
    assert_eq!(
        completion(c, a).await,
        [
            done("a1.zip", "completed", 1),
            done("a2.zip", "processing", 0)
        ]
    );

    // 行と入力の数が違えば、DB に触れる前に失敗する (何も変わらない)
    let (rows, _) = one_day_trip("SAME-1", "D-ONE");
    let mismatched = pg::apply_upload(c, a, a_failing, Arc::new(rows), vec![]).await;
    assert!(
        matches!(mismatched, Err(ApplyUploadError::LengthMismatch)),
        "{mismatched:?}"
    );
    assert_eq!(operation_rows(c, a).await, a_operations);
    assert_eq!(
        completion(c, a).await,
        [
            done("a1.zip", "completed", 1),
            done("a2.zip", "processing", 0)
        ]
    );

    held.close().await;
    db.shutdown();
}

// ---- 日別の要再計算の印 ----

/// 印: 取り込み (`apply_upload`) が付ける対象 (新しい運行・snapshot の変化・`recalc`・乗務員と日付の移動。変化なしは付けない)・
/// ON CONFLICT・一覧の並びと読んだ時刻・消す条件 (月・乗務員の id か乗務員CD・created_at の上限)・テナントの分離。
#[tokio::test(flavor = "multi_thread")]
async fn recalc_pending_marks_follow_the_changes_and_clear_inside_the_tenant() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let a = tenant(c, "Dtako Pending Tenant A").await;
    let b = tenant(c, "Dtako Pending Tenant B").await;
    let up = pg::create_upload(c, a, "p.zip".into()).await.unwrap();
    let unmark = "DELETE FROM dtako_daily_recalc_pending WHERE tenant_id = $1";
    let mark = |driver: &str, month: &str| json!({ "driver": format!("TEST-DRIVER {driver}"), "month": month });
    let same = Some(minutes(0));

    // 新しい運行 → 乗務員 × 読取日と運行日の月 (月をまたぐ運行は 2 つ)。乗務員CD が空の運行には付けない
    let mut p1 = kudguri("P-1", 1, "D-ONE", 8, 17);
    p1.reading_date = NaiveDate::from_ymd_opt(2026, 4, 1).unwrap();
    let rows = vec![p1.clone(), kudguri("P-2", 1, "", 8, 17)];
    let (_, n) = import(c, a, up, rows.clone(), None, minutes(0)).await;
    assert_eq!(n, 2);
    let both_months = [mark("D-ONE", "2026-03-01"), mark("D-ONE", "2026-04-01")];
    assert_eq!(marks(c, a).await, both_months);
    // 印が残っているところへもう一度付けても (ON CONFLICT) 増えない
    import_with(c, a, up, rows.clone(), same, minutes(0), true).await;
    assert_eq!(marks(c, a).await, both_months);

    // 何も変わらない上げ直しは印を付けない。`recalc` (KUDGIVT の変化・やり直し) なら付ける
    exec(c, a, unmark).await;
    import(c, a, up, rows.clone(), same, minutes(0)).await;
    assert_eq!(marks(c, a).await, Vec::<Value>::new());
    import_with(c, a, up, rows.clone(), same, minutes(0), true).await;
    assert_eq!(marks(c, a).await, both_months);
    // snapshot (出発の時刻) が変わった
    exec(c, a, unmark).await;
    let mut later = p1.clone();
    later.departure_at = later.departure_at.map(|d| d + Duration::hours(1));
    import(c, a, up, vec![later], same, minutes(0)).await;
    assert_eq!(marks(c, a).await, both_months);

    // 乗務員が変わった → 今の乗務員 × 月と、前の乗務員 × 前の月
    exec(c, a, unmark).await;
    let mut moved = p1.clone();
    moved.driver_cd = "D-TWO".into();
    moved.driver_name = "TEST-DRIVER D-TWO".into();
    import(c, a, up, vec![moved.clone()], same, minutes(0)).await;
    let four = [
        mark("D-ONE", "2026-03-01"),
        mark("D-TWO", "2026-03-01"),
        mark("D-ONE", "2026-04-01"),
        mark("D-TWO", "2026-04-01"),
    ];
    assert_eq!(marks(c, a).await, four);
    // 読取日だけが変わった (snapshot は同じ) → 今の月と前の月
    exec(c, a, unmark).await;
    moved.reading_date = NaiveDate::from_ymd_opt(2026, 3, 3).unwrap();
    import(c, a, up, vec![moved], same, minutes(0)).await;
    let two_months = [mark("D-TWO", "2026-03-01"), mark("D-TWO", "2026-04-01")];
    assert_eq!(marks(c, a).await, two_months);

    // テナント B に同じ運行を取り込んでも、A の印は変わらない
    let b_up = pg::create_upload(c, b, "p.zip".into()).await.unwrap();
    import(c, b, b_up, vec![p1], None, minutes(0)).await;
    assert_eq!(marks(c, b).await, both_months);
    assert_eq!(marks(c, a).await, two_months);

    // 一覧: 月・乗務員の順。読んだ時刻はどの行も同じで、印の時刻より後。DB の時刻はさらに後
    let listed = pg::recalc_pending_marks(c, a).await.unwrap();
    let d_two = resolve_driver(c, a, "D-TWO").await.unwrap();
    let months: Vec<_> = listed.iter().map(|m| (m.driver_id, m.month)).collect();
    let march = NaiveDate::from_ymd_opt(2026, 3, 1).unwrap();
    let april = NaiveDate::from_ymd_opt(2026, 4, 1).unwrap();
    assert_eq!(months, [(d_two, march), (d_two, april)]);
    let read_at = listed[0].read_at;
    assert!(listed
        .iter()
        .all(|m| m.read_at == read_at && m.created_at <= read_at));
    assert_eq!(listed[0].clone(), listed[0]);
    assert!(pg::db_now(c, a).await.unwrap() >= read_at);

    // 消す: 時刻の上限より後の印は消さない → 上限を読んだ時刻にすれば、その乗務員 × 月だけ消える
    let clear = |before| pg::PendingClear {
        month: march,
        driver_id: Some(d_two),
        driver_cd: None,
        before,
    };
    let older = listed[0].created_at - Duration::microseconds(1);
    let none = HashMap::new;
    let no_unko_nos = || Arc::new(Vec::new());
    let saved = pg::save_daily_hours_in_tx(c, a, none(), no_unko_nos(), Some(clear(older)));
    saved.await.unwrap();
    assert_eq!(marks(c, a).await, two_months);
    let saved = pg::save_daily_hours_in_tx(c, a, none(), no_unko_nos(), Some(clear(read_at)));
    saved.await.unwrap();
    assert_eq!(marks(c, a).await, [mark("D-TWO", "2026-04-01")]);
    // 乗務員CD で指す (月の全員の再計算)。テナント B から A の乗務員CD を指しても、A の印は消えない
    let by_cd = pg::PendingClear {
        month: april,
        driver_id: None,
        driver_cd: Some("D-TWO".into()),
        before: read_at,
    };
    let saved = pg::save_daily_hours_in_tx(c, b, none(), no_unko_nos(), Some(by_cd.clone()));
    saved.await.unwrap();
    assert_eq!(marks(c, a).await, [mark("D-TWO", "2026-04-01")]);
    let saved = pg::save_daily_hours_in_tx(c, a, none(), no_unko_nos(), Some(by_cd));
    saved.await.unwrap();
    assert_eq!(marks(c, a).await, Vec::<Value>::new());
    assert_eq!(marks(c, b).await, both_months);
    assert_eq!(pg::recalc_pending_marks(c, a).await.unwrap(), []);
    assert_eq!(
        pg::month_of(NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()).to_string(),
        "2026-02-01"
    );

    held.close().await;
    db.shutdown();
}

/// 再計算の口が印を読んだ後・保存する前に、同じ 乗務員 × 月 へ取り込みが印を付け直すと、印は消してから入れ直されて
/// 時刻が新しくなる (読んだ時刻より後)。なので、読んだ時刻までの印を消す保存では消えず、計算に入らなかった変化の印が残る。
/// 付け直した後に読んだ時刻なら消える。
#[tokio::test(flavor = "multi_thread")]
async fn recalc_pending_mark_put_again_after_reading_survives_the_clear() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let t = tenant(c, "Dtako Pending Again Tenant").await;
    let up = pg::create_upload(c, t, "q.zip".into()).await.unwrap();
    let rows = vec![kudguri("Q-1", 1, "D-ONE", 8, 17)];
    import(c, t, up, rows.clone(), None, minutes(0)).await;
    let read = pg::recalc_pending_marks(c, t).await.unwrap();
    assert_eq!(read.len(), 1);
    let (read_at, first_created) = (read[0].read_at, read[0].created_at);
    assert!(first_created <= read_at);

    // 読んだ後に、同じ 乗務員 × 月 へ取り込み (`recalc`) が印を付け直す → 1 つのまま、時刻は読んだ時刻より後
    import_with(c, t, up, rows, Some(minutes(0)), minutes(0), true).await;
    let again = pg::recalc_pending_marks(c, t).await.unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(
        (again[0].driver_id, again[0].month),
        (read[0].driver_id, read[0].month)
    );
    assert!(again[0].created_at > read_at, "{again:?} / {read_at}");

    // 再計算の口と同じ消し方 (最初に読んだ時刻まで) では消えない
    let clear = |before| pg::PendingClear {
        month: read[0].month,
        driver_id: Some(read[0].driver_id),
        driver_cd: None,
        before,
    };
    let saved =
        pg::save_daily_hours_in_tx(c, t, HashMap::new(), Arc::new(vec![]), Some(clear(read_at)));
    saved.await.unwrap();
    let one = json!([{ "driver": "TEST-DRIVER D-ONE", "month": "2026-03-01" }]);
    assert_eq!(json!(marks(c, t).await), one);
    // 付け直した後に読んだ時刻なら消える
    let saved = pg::save_daily_hours_in_tx(
        c,
        t,
        HashMap::new(),
        Arc::new(vec![]),
        Some(clear(again[0].read_at)),
    );
    saved.await.unwrap();
    assert_eq!(marks(c, t).await, Vec::<Value>::new());

    held.close().await;
    db.shutdown();
}

// ---- 履歴の読み取り ----

/// 履歴の一覧 2 つ (新しい順・同じ時刻は id の降順・50 件まで・テナントごと・NULL の列) と、ダウンロード用の行。
#[tokio::test(flavor = "multi_thread")]
async fn upload_lists_are_newest_first_capped_and_scoped_to_the_tenant() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let a = tenant(c, "Dtako List Tenant A").await;
    let b = tenant(c, "Dtako List Tenant B").await;

    // テナント A: 1 分おきの 49 行 (f-1 が最も古い) と、それより新しい同じ時刻の 2 行 = 51 行。全部 failed
    let series = "INSERT INTO dtako_upload_history (tenant_id, filename, status, error_message, r2_zip_key, created_at)                   SELECT $1, 'f-' || g || '.zip', 'failed', 'invalid_zip', 'k/' || g,                   TIMESTAMPTZ '2026-03-01 00:00:00+00' + g * interval '1 minute' FROM generate_series(1, 49) g";
    assert_eq!(exec(c, a, series).await, 49);
    let tie = "INSERT INTO dtako_upload_history (tenant_id, filename, status, created_at)                SELECT $1, 'tie.zip', 'failed', TIMESTAMPTZ '2026-03-02 00:00:00+00' FROM generate_series(1, 2)";
    assert_eq!(exec(c, a, tie).await, 2);
    // テナント B: 状態の違う 4 行 (時刻は小数つき)。key と error_message が NULL の行を含む
    let mixed = "INSERT INTO dtako_upload_history (tenant_id, filename, status, error_message, r2_zip_key, created_at) VALUES                  ($1, 'done.zip', 'completed', NULL, 'k/done', TIMESTAMPTZ '2026-03-02 01:02:04+00'),                  ($1, 'failed.zip', 'failed', 'kudguri_invalid', NULL, TIMESTAMPTZ '2026-03-02 01:02:03.123456+00'),                  ($1, 'retry.zip', 'pending_retry', NULL, 'k/retry', TIMESTAMPTZ '2026-03-02 01:02:02+00'),                  ($1, 'running.zip', 'processing', NULL, NULL, TIMESTAMPTZ '2026-03-02 01:02:01+00')";
    assert_eq!(exec(c, b, mixed).await, 4);

    // A: 50 件で切れる (最も古い f-1 が落ちる)。先頭は同じ時刻の 2 行で、id の降順
    let listed = pg::list_uploads(c, a).await.unwrap();
    let names: Vec<&str> = listed.iter().map(|r| r.filename.as_str()).collect();
    let mut want: Vec<String> = vec!["tie.zip".into(), "tie.zip".into()];
    want.extend((2..=49).rev().map(|g| format!("f-{g}.zip")));
    assert_eq!(names, want);
    assert!(listed[0].id > listed[1].id);
    let pending = pg::list_pending_uploads(c, a).await.unwrap();
    let pending_names: Vec<&str> = pending.iter().map(|r| r.filename.as_str()).collect();
    assert_eq!(pending_names, want);
    assert!(pending[0].id > pending[1].id);
    assert!(pending.iter().all(|r| r.tenant_id == a));
    // 列の中身 (f-49 の行)
    let at = |h, m, s| Utc.with_ymd_and_hms(2026, 3, 2, h, m, s).unwrap();
    let newest = &listed[2];
    let got = (
        newest.status.as_str(),
        newest.error_message.as_deref(),
        newest.r2_zip_key.as_deref(),
        newest.created_at,
    );
    let created = Utc.with_ymd_and_hms(2026, 3, 1, 0, 49, 0).unwrap();
    assert_eq!(got, ("failed", Some("invalid_zip"), Some("k/49"), created));

    // B: 別テナントの行は混ざらない。NULL の列は None。一覧は 4 行とも、pending は failed と pending_retry だけ
    let listed = pg::list_uploads(c, b).await.unwrap();
    let rows: Vec<_> = listed
        .iter()
        .map(|r| {
            (
                r.filename.as_str(),
                r.status.as_str(),
                r.error_message.as_deref(),
                r.r2_zip_key.as_deref(),
                r.created_at,
            )
        })
        .collect();
    let want_rows = [
        ("done.zip", "completed", None, Some("k/done"), at(1, 2, 4)),
        (
            "failed.zip",
            "failed",
            Some("kudguri_invalid"),
            None,
            at(1, 2, 3) + Duration::microseconds(123_456),
        ),
        (
            "retry.zip",
            "pending_retry",
            None,
            Some("k/retry"),
            at(1, 2, 2),
        ),
        ("running.zip", "processing", None, None, at(1, 2, 1)),
    ];
    assert_eq!(rows, want_rows);
    let pending = pg::list_pending_uploads(c, b).await.unwrap();
    let rows: Vec<_> = pending
        .iter()
        .map(|r| {
            (
                r.tenant_id,
                r.filename.as_str(),
                r.status.as_str(),
                r.error_message.as_deref(),
                r.created_at,
            )
        })
        .collect();
    let want_rows = [
        (
            b,
            "failed.zip",
            "failed",
            Some("kudguri_invalid"),
            at(1, 2, 3) + Duration::microseconds(123_456),
        ),
        (b, "retry.zip", "pending_retry", None, at(1, 2, 2)),
    ];
    assert_eq!(rows, want_rows);
    assert_eq!(pending[0].id, listed[1].id);
    assert_eq!(
        format!("{:?}", pending[0].clone()),
        format!("{:?}", pending[0])
    );
    assert_eq!(
        format!("{:?}", listed[0].clone()),
        format!("{:?}", listed[0])
    );

    // ダウンロード用の行: key と filename。key が NULL の行は (None, filename)。行が無い・別テナントの id は None
    let (done, failed) = (listed[0].id, listed[1].id);
    let row = pg::upload_download(c, b, done).await.unwrap();
    assert_eq!(
        row,
        Some((Some("k/done".to_owned()), "done.zip".to_owned()))
    );
    let row = pg::upload_download(c, b, failed).await.unwrap();
    assert_eq!(row, Some((None, "failed.zip".to_owned())));
    assert_eq!(
        pg::upload_download(c, b, Uuid::new_v4()).await.unwrap(),
        None
    );
    assert_eq!(pg::upload_download(c, a, done).await.unwrap(), None);

    held.close().await;

    // 切れた接続では 3 つとも DB の失敗
    let mut c = db.client(APP_ROLE).await.sever().await;
    assert!(pg::list_uploads(&mut c, a).await.unwrap_err().is_closed());
    assert!(pg::list_pending_uploads(&mut c, a)
        .await
        .unwrap_err()
        .is_closed());
    assert!(pg::upload_download(&mut c, a, done)
        .await
        .unwrap_err()
        .is_closed());
    drop(c);
    db.shutdown();
}

// ---- 再計算 ----

/// 月の再計算の対象の運行: 運行日か読取日が範囲 (月初〜月末の翌日) に入る行・テナントごと・2 人乗務は乗務員ごとに 1 行
/// (同じ乗務員CD なら 1 行)・読取日と運行NO の順。
#[tokio::test(flavor = "multi_thread")]
async fn operations_for_recalc_pick_the_month_by_operation_or_reading_date() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let a = tenant(c, "Dtako Recalc Tenant A").await;
    let b = tenant(c, "Dtako Recalc Tenant B").await;
    employee(c, a, None, Some("D-ONE"), "TEST-ONE", false).await;
    employee(c, a, None, Some("D-TWO"), "TEST-TWO", false).await;
    employee(c, b, None, Some("D-ONE"), "TEST-OTHER", false).await;
    let ops = "INSERT INTO dtako_operations (tenant_id, unko_no, crew_role, reading_date, operation_date, departure_at, return_at, \
               driver_id, total_distance, drive_time_general, drive_time_highway, drive_time_bypass) \
               SELECT $1, v.unko_no, v.crew_role, v.reading_date::date, v.operation_date::date, \
               TIMESTAMPTZ '2026-03-02 08:15:00+00', TIMESTAMPTZ '2026-03-02 17:15:00+00', \
               (SELECT id FROM employees WHERE tenant_id = $1 AND driver_cd = v.driver_cd), 12.5, 100, 20, 3 FROM (VALUES \
               ('R-IN-OP', 1, '2026-04-05', '2026-03-15', 'D-ONE'), \
               ('R-IN-READ', 1, '2026-03-01', '2026-02-27', 'D-ONE'), \
               ('R-EDGE-END', 1, '2026-04-01', NULL, 'D-ONE'), \
               ('R-OUT', 1, '2026-02-28', '2026-02-27', 'D-ONE'), \
               ('R-OUT-LATE', 1, '2026-04-02', NULL, 'D-ONE'), \
               ('R-TWO', 1, '2026-03-10', '2026-03-09', 'D-ONE'), \
               ('R-TWO', 2, '2026-03-10', '2026-03-09', 'D-TWO'), \
               ('R-SAME', 1, '2026-03-11', '2026-03-11', NULL), \
               ('R-SAME', 2, '2026-03-11', '2026-03-11', NULL) \
               ) v(unko_no, crew_role, reading_date, operation_date, driver_cd)";
    assert_eq!(exec(c, a, ops).await, 9);
    let other = "INSERT INTO dtako_operations (tenant_id, unko_no, reading_date, driver_id) \
                 SELECT $1, 'R-OTHER', DATE '2026-03-10', id FROM employees WHERE tenant_id = $1";
    assert_eq!(exec(c, b, other).await, 1);

    let (start, end) = (
        NaiveDate::from_ymd_opt(2026, 3, 1).unwrap(),
        NaiveDate::from_ymd_opt(2026, 4, 1).unwrap(),
    );
    let rows = pg::operations_for_recalc(c, a, start, end).await.unwrap();
    let got: Vec<(&str, NaiveDate, Option<&str>)> = rows
        .iter()
        .map(|r| (r.unko_no.as_str(), r.reading_date, r.driver_cd.as_deref()))
        .collect();
    let d = |m, day| NaiveDate::from_ymd_opt(2026, m, day).unwrap();
    let want = [
        ("R-IN-READ", d(3, 1), Some("D-ONE")),
        ("R-TWO", d(3, 10), Some("D-ONE")),
        ("R-TWO", d(3, 10), Some("D-TWO")),
        ("R-SAME", d(3, 11), None),
        ("R-EDGE-END", d(4, 1), Some("D-ONE")),
        ("R-IN-OP", d(4, 5), Some("D-ONE")),
    ];
    assert_eq!(got, want);
    // 列の中身 (日時は UTC の値のまま)
    let first = rows[0].clone();
    let at = |h| Utc.with_ymd_and_hms(2026, 3, 2, h, 15, 0).unwrap();
    assert_eq!(
        (first.operation_date, first.departure_at, first.return_at),
        (Some(d(2, 27)), Some(at(8)), Some(at(17)))
    );
    let numbers = (
        first.total_distance,
        first.drive_time_general,
        first.drive_time_highway,
        first.drive_time_bypass,
    );
    assert_eq!(numbers, (Some(12.5), Some(100), Some(20), Some(3)));
    assert_eq!(format!("{first:?}").len(), format!("{:?}", rows[0]).len());
    // 別テナントには A の行が出ない
    let other_rows = pg::operations_for_recalc(c, b, start, end).await.unwrap();
    let names: Vec<&str> = other_rows.iter().map(|r| r.unko_no.as_str()).collect();
    assert_eq!(names, ["R-OTHER"]);
    held.close().await;

    let mut c = db.client(APP_ROLE).await.sever().await;
    assert!(pg::operations_for_recalc(&mut c, a, start, end)
        .await
        .unwrap_err()
        .is_closed());
    let saved = pg::save_daily_hours_in_tx(&mut c, a, HashMap::new(), Arc::new(Vec::new()), None);
    let saved = saved.await;
    assert!(saved.unwrap_err().is_closed());
    drop(c);
    db.shutdown();
}

/// 乗務員ごとの再計算の文: 乗務員CD (テナントで絞る・NULL は無いのと同じ) / 乗務員 1 人の運行 (運行日か読取日が範囲に入る行・
/// その乗務員だけ・同じ運行NO は 1 行・読取日と運行NO の順)。
#[tokio::test(flavor = "multi_thread")]
async fn driver_recalc_reads_driver_cd_and_the_drivers_operations() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let a = tenant(c, "Dtako Driver Recalc Tenant A").await;
    let b = tenant(c, "Dtako Driver Recalc Tenant B").await;
    let d = |m, day| NaiveDate::from_ymd_opt(2026, m, day).unwrap();

    let one = employee(c, a, None, Some("D-ONE"), "TEST-ONE", false).await;
    employee(c, a, None, Some("D-TWO"), "TEST-TWO", false).await;
    let no_cd = employee(c, a, Some("E-NOCD"), None, "TEST-NOCD", false).await;
    let other_one = employee(c, b, None, Some("D-ONE"), "TEST-OTHER", false).await;
    let ops = "INSERT INTO dtako_operations (tenant_id, unko_no, crew_role, reading_date, operation_date, departure_at, return_at, \
               driver_id, total_distance, drive_time_general, drive_time_highway, drive_time_bypass) \
               SELECT $1, v.unko_no, v.crew_role, v.reading_date::date, v.operation_date::date, \
               TIMESTAMPTZ '2026-03-02 08:15:00+00', TIMESTAMPTZ '2026-03-02 17:15:00+00', \
               (SELECT id FROM employees WHERE tenant_id = $1 AND driver_cd = v.driver_cd), 12.5, 100, 20, 3 FROM (VALUES \
               ('R-IN-OP', 1, '2026-04-05', '2026-03-15', 'D-ONE'), \
               ('R-IN-READ', 1, '2026-03-01', '2026-02-27', 'D-ONE'), \
               ('R-EDGE-END', 1, '2026-04-01', NULL, 'D-ONE'), \
               ('R-OUT', 1, '2026-02-28', '2026-02-27', 'D-ONE'), \
               ('R-OUT-LATE', 1, '2026-04-02', NULL, 'D-ONE'), \
               ('R-TWO', 1, '2026-03-10', '2026-03-09', 'D-ONE'), \
               ('R-TWO', 2, '2026-03-10', '2026-03-09', 'D-TWO'), \
               ('R-DUP', 1, '2026-03-11', '2026-03-11', 'D-ONE'), \
               ('R-DUP', 2, '2026-03-11', '2026-03-11', 'D-ONE') \
               ) v(unko_no, crew_role, reading_date, operation_date, driver_cd)";
    assert_eq!(exec(c, a, ops).await, 9);
    let other_op = "INSERT INTO dtako_operations (tenant_id, unko_no, reading_date, driver_id) \
                    SELECT $1, 'R-OTHER', DATE '2026-03-10', id FROM employees WHERE tenant_id = $1";
    assert_eq!(exec(c, b, other_op).await, 1);

    let (start, end) = (d(3, 1), d(4, 1));
    let (driver_cd, rows) = pg::driver_operations_for_recalc(c, a, one, start, end)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(driver_cd, "D-ONE");
    let got: Vec<(&str, NaiveDate, Option<&str>)> = rows
        .iter()
        .map(|r| (r.unko_no.as_str(), r.reading_date, r.driver_cd.as_deref()))
        .collect();
    let want = [
        ("R-IN-READ", d(3, 1), Some("D-ONE")),
        ("R-TWO", d(3, 10), Some("D-ONE")),
        ("R-DUP", d(3, 11), Some("D-ONE")),
        ("R-EDGE-END", d(4, 1), Some("D-ONE")),
        ("R-IN-OP", d(4, 5), Some("D-ONE")),
    ];
    assert_eq!(got, want);
    // 列の中身 (日時は UTC の値のまま)
    let first = &rows[0];
    let at = |h| Utc.with_ymd_and_hms(2026, 3, 2, h, 15, 0).unwrap();
    assert_eq!(
        (first.operation_date, first.departure_at, first.return_at),
        (Some(d(2, 27)), Some(at(8)), Some(at(17)))
    );
    let numbers = (
        first.total_distance,
        first.drive_time_general,
        first.drive_time_highway,
        first.drive_time_bypass,
    );
    assert_eq!(numbers, (Some(12.5), Some(100), Some(20), Some(3)));
    // 乗務員CD が NULL・別テナントの乗務員・居ない id は None
    for id in [no_cd, other_one, Uuid::new_v4()] {
        let none = pg::driver_operations_for_recalc(c, a, id, start, end).await;
        assert_eq!(none.unwrap().map(|(cd, _)| cd), None);
    }
    // 別テナントには A の行が出ない
    let (_, other_rows) = pg::driver_operations_for_recalc(c, b, other_one, start, end)
        .await
        .unwrap()
        .unwrap();
    let names: Vec<&str> = other_rows.iter().map(|r| r.unko_no.as_str()).collect();
    assert_eq!(names, ["R-OTHER"]);
    held.close().await;

    let mut c = db.client(APP_ROLE).await.sever().await;
    let driver = pg::driver_operations_for_recalc(&mut c, a, one, start, end).await;
    assert!(driver.unwrap_err().is_closed());
    drop(c);
    db.shutdown();
}
