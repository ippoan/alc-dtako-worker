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

use std::sync::Arc;

use alc_csv_parser::kudgivt::KudgivtRow;
use alc_csv_parser::kudguri::KudguriRow;
use alc_csv_parser::operation_changes::OperationMinutes;
use alc_csv_parser::work_segments::EventClass;
use alc_dtako_upload::pg::{self, CreateUploadError, OperationInput, PreparedRow};
use alc_worker_db::PgClient;
use chrono::NaiveDate;
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

/// 準備 (`prepare_upload`) → 入れ替え (`replace_operations`) を、取り込みと同じ順で 1 回流す。
/// `before` は「既に在る」行にだけ渡す前回の分数 (保存先の旧 KUDGIVT が取れなかった場合は `None`)。
async fn import(
    c: &mut PgClient,
    tenant_id: Uuid,
    upload_id: Uuid,
    rows: Vec<KudguriRow>,
    before: Option<OperationMinutes>,
    after: OperationMinutes,
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
        })
        .collect();
    let count = pg::replace_operations(c, tenant_id, upload_id, rows, inputs);
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
    let all = changes(c, t).await;
    assert_eq!(all.len(), 4);
    assert_eq!(all[3]["unko_no"], "U3");
    assert_eq!(all[3]["before"]["departure_at"], "2026-03-02T03:15:30Z");
    assert_eq!(all[3]["after"]["departure_at"], "2026-03-02T05:15:30Z");
    assert_eq!(all[3]["before"]["before_kudgivt"], "unavailable");

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
    };
    assert_eq!(
        format!("{:?}", input.clone()).len(),
        format!("{input:?}").len()
    );
    let replaced = pg::replace_operations(&mut c, t, t, rows, vec![input]).await;
    assert!(replaced.unwrap_err().is_closed());
    drop(c);
    db.shutdown();
}
