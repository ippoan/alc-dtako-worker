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

mod embedded;

use alc_dtako_upload::pg;
use embedded::{kudgivt_flags, operation, operations, tenant, upload, Embedded, APP_ROLE};
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

/// テナントを設定しない素の接続では、`dtako_upload_history`・`dtako_operations` の行は読めない (エラーか 0 行)。
#[tokio::test(flavor = "multi_thread")]
async fn connection_without_tenant_reads_no_rows() {
    let db = Embedded::start().await;
    let mut held = db.client(APP_ROLE).await;
    let c = &mut held.inner;
    let t = tenant(c, "Dtako No Tenant").await;
    upload(c, t, "a.zip", "completed", Some("a/key.zip"), 0.0).await;
    operation(c, t, "3001", 0, false).await;
    held.close().await;

    let raw = db.raw(APP_ROLE).await;
    for table in ["dtako_upload_history", "dtako_operations"] {
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
