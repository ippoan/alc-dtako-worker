//! 偽の保存先・偽の待ち・ログを溜める差し込み口 (`tests/store.rs` と `tests/split_flow.rs` が使う)。
//!
//! 偽の保存先は key ごとに「あと何回 PUT を失敗させるか」を持ち、key ごとの呼ばれた回数と、
//! 同時に走っている PUT の最大を記録する。偽の待ちは待たずに、渡された値を記録する。

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use alc_dtako_upload::split::{LogLevel, LogSink};
use alc_dtako_upload::store::{ObjectStore, Sleeper, StoreError};
use futures_util::future::BoxFuture;

#[derive(Default)]
pub struct FakeStore {
    /// key → あと何回 PUT を失敗させるか
    fail_left: Mutex<HashMap<String, u32>>,
    /// key → PUT が呼ばれた回数
    put_calls: Mutex<HashMap<String, u32>>,
    /// key → (bytes, content_type)
    objects: Mutex<BTreeMap<String, (Vec<u8>, String)>>,
    running: AtomicUsize,
    max_running: AtomicUsize,
    /// GET を失敗させる
    get_broken: AtomicBool,
}

impl FakeStore {
    pub fn failing(plan: &[(&str, u32)]) -> Self {
        let this = Self::default();
        for (key, n) in plan {
            this.fail_puts(key, *n);
        }
        this
    }

    /// `key` への PUT を、あと `n` 回失敗させる。
    pub fn fail_puts(&self, key: &str, n: u32) {
        self.fail_left.lock().unwrap().insert(key.to_owned(), n);
    }

    pub fn break_get(&self) {
        self.get_broken.store(true, Ordering::SeqCst);
    }

    /// PUT を通さずに object を置く (テストの準備用。呼ばれた回数には数えない)。
    pub fn seed(&self, key: &str, bytes: Vec<u8>, content_type: &str) {
        let mut objects = self.objects.lock().unwrap();
        objects.insert(key.to_owned(), (bytes, content_type.to_owned()));
    }

    pub fn put_calls(&self, key: &str) -> u32 {
        self.put_calls
            .lock()
            .unwrap()
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    pub fn total_put_calls(&self) -> u32 {
        self.put_calls.lock().unwrap().values().sum()
    }

    pub fn object(&self, key: &str) -> Option<(Vec<u8>, String)> {
        self.objects.lock().unwrap().get(key).cloned()
    }

    /// 置かれている全部の object (key → (bytes, content_type))。
    pub fn objects(&self) -> BTreeMap<String, (Vec<u8>, String)> {
        self.objects.lock().unwrap().clone()
    }

    pub fn running(&self) -> usize {
        self.running.load(Ordering::SeqCst)
    }

    pub fn max_running(&self) -> usize {
        self.max_running.load(Ordering::SeqCst)
    }
}

impl ObjectStore for FakeStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, StoreError>> {
        Box::pin(async move {
            if self.get_broken.load(Ordering::SeqCst) {
                return Err(StoreError::new("get"));
            }
            Ok(self.object(key).map(|(bytes, _)| bytes))
        })
    }

    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_running.fetch_max(running, Ordering::SeqCst);
            // ここで他の PUT に順番を譲る (同時に走っている状態を作る)
            tokio::task::yield_now().await;
            self.running.fetch_sub(1, Ordering::SeqCst);
            *self
                .put_calls
                .lock()
                .unwrap()
                .entry(key.to_owned())
                .or_insert(0) += 1;
            {
                let mut fail_left = self.fail_left.lock().unwrap();
                let left = fail_left.entry(key.to_owned()).or_insert(0);
                if *left > 0 {
                    *left -= 1;
                    return Err(StoreError::new("put"));
                }
            }
            self.seed(key, bytes, content_type);
            Ok(())
        })
    }
}

#[derive(Default)]
pub struct FakeSleeper {
    slept: Mutex<Vec<u64>>,
}

impl FakeSleeper {
    pub fn slept(&self) -> Vec<u64> {
        self.slept.lock().unwrap().clone()
    }
}

impl Sleeper for FakeSleeper {
    fn sleep_ms(&self, ms: u64) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.slept.lock().unwrap().push(ms) })
    }
}

/// ログを溜める差し込み口。
#[derive(Clone, Default)]
pub struct Logs(Arc<Mutex<Vec<(LogLevel, String)>>>);

impl Logs {
    pub fn sink(&self) -> LogSink {
        let logs = self.0.clone();
        Arc::new(move |level, message| logs.lock().unwrap().push((level, message.to_owned())))
    }

    pub fn all(&self) -> Vec<(LogLevel, String)> {
        self.0.lock().unwrap().clone()
    }
}
