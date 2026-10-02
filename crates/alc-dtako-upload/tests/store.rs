//! `store` (保存先の層と PUT のやり直し) を、偽の保存先で確かめる (native。DB も R2 も要らない)。
//!
//! 偽の保存先は key ごとに「あと何回失敗するか」を持ち、key ごとの呼ばれた回数と、同時に走っている PUT の最大を記録する。
//! 偽の待ちは待たずに、渡された値を記録する。

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use alc_dtako_upload::store::{
    put_all_with_retry, ObjectStore, PutItem, PutOutcome, Sleeper, StoreError, PUT_CONCURRENCY,
    PUT_RETRY_ATTEMPTS, PUT_RETRY_DELAYS_MS,
};
use futures_util::future::LocalBoxFuture;

#[derive(Default)]
struct FakeStore {
    /// key → あと何回 PUT を失敗させるか
    fail_left: RefCell<HashMap<String, u32>>,
    /// key → PUT が呼ばれた回数
    put_calls: RefCell<HashMap<String, u32>>,
    /// key → (bytes, content_type)
    objects: RefCell<HashMap<String, (Vec<u8>, String)>>,
    running: Cell<usize>,
    max_running: Cell<usize>,
    /// GET を失敗させる
    get_broken: Cell<bool>,
}

impl FakeStore {
    fn failing(plan: &[(&str, u32)]) -> Self {
        let this = Self::default();
        for (key, n) in plan {
            this.fail_left.borrow_mut().insert((*key).to_owned(), *n);
        }
        this
    }

    fn put_calls(&self, key: &str) -> u32 {
        self.put_calls.borrow().get(key).copied().unwrap_or(0)
    }

    fn total_put_calls(&self) -> u32 {
        self.put_calls.borrow().values().sum()
    }
}

impl ObjectStore for FakeStore {
    fn get<'a>(&'a self, key: &'a str) -> LocalBoxFuture<'a, Result<Option<Vec<u8>>, StoreError>> {
        Box::pin(async move {
            if self.get_broken.get() {
                return Err(StoreError::new("get"));
            }
            Ok(self
                .objects
                .borrow()
                .get(key)
                .map(|(bytes, _)| bytes.clone()))
        })
    }

    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
    ) -> LocalBoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.running.set(self.running.get() + 1);
            self.max_running
                .set(self.max_running.get().max(self.running.get()));
            // ここで他の PUT に順番を譲る (同時に走っている状態を作る)
            tokio::task::yield_now().await;
            self.running.set(self.running.get() - 1);
            *self
                .put_calls
                .borrow_mut()
                .entry(key.to_owned())
                .or_insert(0) += 1;
            let mut fail_left = self.fail_left.borrow_mut();
            let left = fail_left.entry(key.to_owned()).or_insert(0);
            if *left > 0 {
                *left -= 1;
                return Err(StoreError::new("put"));
            }
            self.objects
                .borrow_mut()
                .insert(key.to_owned(), (bytes, content_type.to_owned()));
            Ok(())
        })
    }
}

#[derive(Default)]
struct FakeSleeper {
    slept: RefCell<Vec<u64>>,
}

impl Sleeper for FakeSleeper {
    fn sleep_ms(&self, ms: u64) -> LocalBoxFuture<'_, ()> {
        Box::pin(async move { self.slept.borrow_mut().push(ms) })
    }
}

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
    (out, sleeper.slept.into_inner())
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
    let objects = store.objects.borrow();
    assert_eq!(
        objects.get("b"),
        Some(&(b"body of b".to_vec(), "text/csv".to_owned()))
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
    let objects = store.objects.borrow();
    assert_eq!(
        objects.get("b").map(|(b, _)| b.as_slice()),
        Some(&b"body of b"[..])
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
    assert!(!store.objects.borrow().contains_key("b"));
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
    assert_eq!(store.max_running.get(), PUT_CONCURRENCY);
    assert_eq!(store.running.get(), 0);
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
    assert_eq!(store.max_running.get(), 0);
}

/// `get`: 無い = `Ok(None)` / 在る = `Ok(Some)` / 失敗 = `Err`。
#[tokio::test]
async fn get_returns_none_some_or_error() {
    let store = FakeStore::default();
    assert_eq!(store.get("missing").await, Ok(None));
    store
        .put("k", b"zip".to_vec(), "application/zip")
        .await
        .unwrap();
    assert_eq!(store.get("k").await, Ok(Some(b"zip".to_vec())));
    store.get_broken.set(true);
    assert_eq!(store.get("k").await, Err(StoreError::new("get")));
}

/// `StoreError` の文は固定の語と段の名前だけ (key を含まない)。
#[tokio::test]
async fn store_error_display_has_only_the_stage() {
    let key = "tenant-x/unko-123/KUDGIVT.csv";
    let store = FakeStore::failing(&[(key, 1)]);
    let err = store.put(key, vec![], "text/csv").await.unwrap_err();
    assert_eq!(err.stage(), "put");
    assert_eq!(err.to_string(), "object store error (put)");
    assert!(!format!("{err} {err:?}").contains("unko-123"));
    let source: &dyn std::error::Error = &err;
    assert!(source.source().is_none());
}
