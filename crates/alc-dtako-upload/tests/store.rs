//! `store` (保存先の層・PUT のやり直し・まとめて読む) を、偽の保存先で確かめる (native。DB も R2 も要らない)。
//! 偽物は `fakes/mod.rs` (`tests/split_flow.rs` と共用)。

// 共用の偽物のうち、このファイルが使わないもの (ログの差し込み口など) が在る
#[allow(dead_code)]
mod fakes;

use alc_dtako_upload::store::{
    get_all, put_all_with_retry, ObjectStore, PutItem, PutOutcome, StoreError, GET_CONCURRENCY,
    PUT_CONCURRENCY, PUT_RETRY_ATTEMPTS, PUT_RETRY_DELAYS_MS,
};
use fakes::{FakeSleeper, FakeStore};

fn item(key: &str) -> PutItem<String> {
    PutItem {
        key: key.to_owned(),
        bytes: format!("body of {key}").into_bytes(),
        content_type: "text/csv",
        tag: format!("tag:{key}"),
    }
}

fn items(keys: &[&str]) -> Vec<PutItem<String>> {
    keys.iter().map(|k| item(k)).collect()
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

async fn run(store: &FakeStore, keys: &[&str]) -> (PutOutcome<String>, Vec<u64>) {
    let sleeper = FakeSleeper::default();
    let out = put_all_with_retry(store, &sleeper, items(keys)).await;
    (out, sleeper.slept())
}

#[test]
fn constants_match_the_backend() {
    assert_eq!(PUT_RETRY_ATTEMPTS, 3);
    assert_eq!(PUT_RETRY_DELAYS_MS, [300, 800]);
    assert_eq!(PUT_CONCURRENCY, 6);
}

/// 全部 1 回で成功 → 待たない・失敗なし。bytes と content_type がそのまま渡る。
#[tokio::test]
async fn all_succeed_on_the_first_attempt() {
    let store = FakeStore::default();
    let (out, slept) = run(&store, &["a", "b", "c"]).await;
    assert_eq!(sorted(out.succeeded), ["tag:a", "tag:b", "tag:c"]);
    assert_eq!(out.failed, Vec::<String>::new());
    assert_eq!(slept, Vec::<u64>::new());
    assert_eq!(store.total_put_calls(), 3);
    assert_eq!(
        store.object("b"),
        Some((b"body of b".to_vec(), "text/csv".to_owned()))
    );
}

/// 一部が 1 回失敗 → 2 回目で成功。待ちは 300 だけ。成功済みは 2 回目に送らない。
#[tokio::test]
async fn failed_once_is_retried_without_resending_the_succeeded() {
    let store = FakeStore::failing(&[("b", 1)]);
    let (out, slept) = run(&store, &["a", "b", "c"]).await;
    assert_eq!(sorted(out.succeeded), ["tag:a", "tag:b", "tag:c"]);
    assert_eq!(out.failed, Vec::<String>::new());
    assert_eq!(slept, [300]);
    assert_eq!(
        (
            store.put_calls("a"),
            store.put_calls("b"),
            store.put_calls("c")
        ),
        (1, 2, 1)
    );
    // やり直しでも同じ bytes が書かれる
    assert_eq!(
        store.object("b").map(|(bytes, _)| bytes),
        Some(b"body of b".to_vec())
    );
}

/// 一部が 2 回失敗 → 3 回目で成功。待ちは 300・800。
#[tokio::test]
async fn failed_twice_succeeds_on_the_third_attempt() {
    let store = FakeStore::failing(&[("b", 2), ("c", 1)]);
    let (out, slept) = run(&store, &["a", "b", "c"]).await;
    assert_eq!(sorted(out.succeeded), ["tag:a", "tag:b", "tag:c"]);
    assert_eq!(out.failed, Vec::<String>::new());
    assert_eq!(slept, [300, 800]);
    assert_eq!(
        (
            store.put_calls("a"),
            store.put_calls("b"),
            store.put_calls("c")
        ),
        (1, 3, 2)
    );
}

/// 3 回とも失敗 → `failed` に入る。待ちは 300・800 (3 回目の後は待たない)。4 回目は送らない。
#[tokio::test]
async fn failed_three_times_ends_in_failed() {
    let store = FakeStore::failing(&[("b", 9), ("d", 9)]);
    let (out, slept) = run(&store, &["a", "b", "c", "d"]).await;
    assert_eq!(sorted(out.succeeded), ["tag:a", "tag:c"]);
    assert_eq!(sorted(out.failed), ["tag:b", "tag:d"]);
    assert_eq!(slept, [300, 800]);
    assert_eq!((store.put_calls("b"), store.put_calls("d")), (3, 3));
    assert_eq!((store.put_calls("a"), store.put_calls("c")), (1, 1));
    assert_eq!(store.object("b"), None);
}

/// 同時に走る PUT は 6 本まで (item 20 個)。
#[tokio::test]
async fn at_most_six_puts_run_at_once() {
    let keys: Vec<String> = (0..20).map(|i| format!("k{i:02}")).collect();
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    let store = FakeStore::default();
    let (out, slept) = run(&store, &keys).await;
    assert_eq!(out.succeeded.len(), 20);
    assert_eq!(out.failed, Vec::<String>::new());
    assert_eq!(slept, Vec::<u64>::new());
    assert_eq!(store.max_running(), PUT_CONCURRENCY);
    assert_eq!(store.running(), 0);
}

/// 空の入力 → 保存先にも待ちにも触らない。
#[tokio::test]
async fn empty_input_touches_nothing() {
    let store = FakeStore::default();
    let (out, slept) = run(&store, &[]).await;
    assert_eq!(
        out,
        PutOutcome {
            succeeded: vec![],
            failed: vec![]
        }
    );
    assert_eq!(slept, Vec::<u64>::new());
    assert_eq!(store.total_put_calls(), 0);
    assert_eq!(store.max_running(), 0);
}

/// `get`: 無い = `Ok(None)` / 在る = `Ok(Some)` / 失敗 = `Err`。
#[tokio::test]
async fn get_returns_none_some_or_error() {
    let store = FakeStore::default();
    assert_eq!(store.get("missing").await, Ok(None));
    let put = store.put("k", b"zip".to_vec(), "application/zip");
    put.await.unwrap();
    assert_eq!(store.get("k").await, Ok(Some(b"zip".to_vec())));
    store.break_get();
    assert_eq!(store.get("k").await, Err(StoreError::new("get")));
}

/// `StoreError` が持つのは段の名前だけ (`Display` も `Debug` も、固定の語と段の名前で全部)。
#[tokio::test]
async fn store_error_display_has_only_the_stage() {
    let key = "some-prefix/unko/123/KUDGIVT.csv";
    let store = FakeStore::failing(&[(key, 1)]);
    let err = store.put(key, vec![], "text/csv").await.unwrap_err();
    assert_eq!(err.stage(), "put");
    assert_eq!(err.to_string(), "object store error (put)");
    assert_eq!(format!("{err:?}"), "StoreError { stage: \"put\" }");
    let source: &dyn std::error::Error = &err;
    assert!(source.source().is_none());
}

/// まとめて読む: 同時は 6 本まで。結果は tag に結び付き、無い・読めないはどちらも `None` (やり直さない)。空の入力は何も呼ばない。
#[tokio::test(flavor = "multi_thread")]
async fn get_all_reads_at_most_six_at_once_and_binds_results_to_tags() {
    let store = FakeStore::default();
    assert_eq!(get_all(&store, Vec::<(String, usize)>::new()).await, vec![]);
    assert_eq!(store.max_get_running(), 0);

    // 20 本のうち、偶数番だけ置いてある。3 番は置いてあるが 1 回だけ読めない
    for n in (0..20).step_by(2) {
        store.seed(
            &format!("k/{n}"),
            format!("body {n}").into_bytes(),
            "text/csv",
        );
    }
    store.seed("k/3", b"body 3".to_vec(), "text/csv");
    store.fail_gets_containing("k/3", 1);
    let wanted: Vec<(String, usize)> = (0..20).map(|n| (format!("k/{n}"), n)).collect();
    let mut got = get_all(&store, wanted).await;
    got.sort();
    let want: Vec<(usize, Option<Vec<u8>>)> = (0..20)
        .map(|n| (n, (n % 2 == 0).then(|| format!("body {n}").into_bytes())))
        .collect();
    assert_eq!(got, want);
    assert_eq!(store.max_get_running(), GET_CONCURRENCY);
    assert_eq!((GET_CONCURRENCY, PUT_CONCURRENCY), (6, 6));
    // やり直していない (失敗の指定は 1 回ぶんで、次に読めば読める)
    assert_eq!(store.get("k/3").await, Ok(Some(b"body 3".to_vec())));
}
