//! 偽の保存先・偽の待ち・ログを溜める差し込み口 (`tests/store.rs`・`tests/split_flow.rs`・`tests/upload_flow.rs` が使う)。
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
    /// (key の一部, あと何回 GET を失敗させるか)。key が実行時に決まるもの (履歴の id を含む key) 用
    get_fail_patterns: Mutex<Vec<(String, u32)>>,
    /// (key の一部, あと何回 PUT を失敗させるか)
    put_fail_patterns: Mutex<Vec<(String, u32)>>,
}

/// `key` が当たる pattern のうち、回数が残っている最初のものを 1 回ぶん減らす。減らしたら true (= 今回は失敗させる)。
fn take_failure(patterns: &Mutex<Vec<(String, u32)>>, key: &str) -> bool {
    let mut patterns = patterns.lock().unwrap();
    let hit = patterns
        .iter_mut()
        .find(|(needle, left)| *left > 0 && key.contains(needle.as_str()));
    hit.map(|(_, left)| *left -= 1).is_some()
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

    /// key に `needle` を含む GET を、あと `n` 回失敗させる。
    pub fn fail_gets_containing(&self, needle: &str, n: u32) {
        let mut patterns = self.get_fail_patterns.lock().unwrap();
        patterns.push((needle.to_owned(), n));
    }

    /// key に `needle` を含む PUT を、あと `n` 回失敗させる。
    pub fn fail_puts_containing(&self, needle: &str, n: u32) {
        let mut patterns = self.put_fail_patterns.lock().unwrap();
        patterns.push((needle.to_owned(), n));
    }

    /// object を消す (テストの準備用)。
    pub fn remove(&self, key: &str) {
        self.objects.lock().unwrap().remove(key);
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
            let broken = self.get_broken.load(Ordering::SeqCst);
            if broken || take_failure(&self.get_fail_patterns, key) {
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
            if take_failure(&self.put_fail_patterns, key) {
                return Err(StoreError::new("put"));
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

/// zip の先頭のエントリの「非圧縮サイズ」(central directory に書かれた値) を書き換える (中身は変えない)。
/// 書かれた大きさと中身が合わない zip を作るため。
pub fn declare_uncompressed_size(zip: &mut [u8], size: u32) {
    let at = first_central_header(zip);
    zip[at + 24..at + 28].copy_from_slice(&size.to_le_bytes());
}

/// zip の先頭のエントリに「大きさは中身の後ろに書いてある」の印 (central directory の flag の bit 3) を立てる (中身は変えない)。
/// 非圧縮サイズの合計が分からない扱いの zip を作るため。
pub fn flag_data_descriptor(zip: &mut [u8]) {
    let at = first_central_header(zip);
    zip[at + 8] |= 1 << 3;
}

/// central directory の先頭のエントリの位置。
fn first_central_header(zip: &[u8]) -> usize {
    // end of central directory (末尾 22 バイト。comment は無い前提) の +16 が central directory の位置
    let eocd = zip.len() - 22;
    assert_eq!(&zip[eocd..eocd + 4], b"PK\x05\x06");
    let at = u32::from_le_bytes(zip[eocd + 16..eocd + 20].try_into().unwrap()) as usize;
    assert_eq!(&zip[at..at + 4], b"PK\x01\x02");
    at
}
