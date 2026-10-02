//! 保存先 (R2) への読み書きの層と、失敗した PUT だけをやり直す [`put_all_with_retry`]
//! (Refs ippoan/rust-alc-api#725)。
//!
//! 口のコードから保存先を切り離すための抽象。R2 の binding (`worker::Bucket`) は wasm32 でしか動かないので、
//! 口の流れを native のテストで通すときは偽の保存先を差す。**R2 を包む実装はここには無い** (載せる側 = 直下の worker が持つ)。
//!
//! trait は `Send` を要求しない (Workers の R2 の future は `Send` でない)。待ちも [`Sleeper`] 越しで、
//! この crate は `tokio::time` を使わない (wasm32 で動かないため)。

use std::fmt;

use futures_util::future::LocalBoxFuture;
use futures_util::stream::{self, StreamExt};

/// PUT の回 (attempt) の上限 (初回 + やり直し)。backend の `SPLIT_RETRY_ATTEMPTS` と同じ。
pub const PUT_RETRY_ATTEMPTS: usize = 3;
/// 回と回の間の待ち (ms)。1 回目の後・2 回目の後。backend の `SPLIT_RETRY_DELAYS_MS` と同じ。
pub const PUT_RETRY_DELAYS_MS: [u64; 2] = [300, 800];
/// 1 つの回の中で同時に走らせる PUT の数 (Workers の同時接続の上限に合わせる)。
pub const PUT_CONCURRENCY: usize = 6;

/// 保存先のエラー。**識別子を持たない** — 持つのは段の名前 (コードに書いた固定の語) だけで、
/// key・bucket 名・ランタイムが返した生の文は載せない (詳細を残したいときは、作る側が自分のログに出す)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreError {
    stage: &'static str,
}

impl StoreError {
    /// `stage` = 失敗した段の名前 (`"get"`・`"put"`・`"read body"` など、固定の語)。
    pub const fn new(stage: &'static str) -> Self {
        Self { stage }
    }

    pub const fn stage(&self) -> &'static str {
        self.stage
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "object store error ({})", self.stage)
    }
}

impl std::error::Error for StoreError {}

/// 保存先 (key → bytes)。
pub trait ObjectStore {
    /// object を読む。無ければ `Ok(None)`。
    fn get<'a>(&'a self, key: &'a str) -> LocalBoxFuture<'a, Result<Option<Vec<u8>>, StoreError>>;

    /// object を書く (同じ key は上書き)。
    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
    ) -> LocalBoxFuture<'a, Result<(), StoreError>>;
}

/// 待ち (worker では `worker::Delay`、テストでは待たずに値を記録する偽物)。
pub trait Sleeper {
    fn sleep_ms(&self, ms: u64) -> LocalBoxFuture<'_, ()>;
}

/// PUT 1 件。`tag` は呼び手のもの (結果の仕分けに使う値。この層は中身を見ない)。
#[derive(Debug, Clone)]
pub struct PutItem<T> {
    pub key: String,
    pub bytes: Vec<u8>,
    pub content_type: &'static str,
    pub tag: T,
}

/// [`put_all_with_retry`] の結果。`tag` だけを返す (順は入力の順とは限らない)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOutcome<T> {
    pub succeeded: Vec<T>,
    /// 回を使い切っても書けなかったもの
    pub failed: Vec<T>,
}

/// `items` を全部 PUT する。失敗したものだけを、最大 [`PUT_RETRY_ATTEMPTS`] 回までやり直す
/// (成功済みは再送しない)。1 つの回の中は同時 [`PUT_CONCURRENCY`] 本。
///
/// 回の後に失敗が残り、次の回が在るときだけ [`PUT_RETRY_DELAYS_MS`] の該当の値だけ待つ
/// (最後の回の後は待たない)。`items` が空なら `store` にも `sleeper` にも触らない。
pub async fn put_all_with_retry<S, P, T>(
    store: &S,
    sleeper: &P,
    items: Vec<PutItem<T>>,
) -> PutOutcome<T>
where
    S: ObjectStore + ?Sized,
    P: Sleeper + ?Sized,
{
    let mut succeeded = Vec::new();
    let mut pending = items;
    for attempt in 0..PUT_RETRY_ATTEMPTS {
        if pending.is_empty() {
            break;
        }
        let results: Vec<(PutItem<T>, bool)> = stream::iter(pending)
            .map(|item| async move {
                // やり直しに備えて bytes は手元に残す (渡すのは写し)
                let put = store.put(&item.key, item.bytes.clone(), item.content_type);
                let ok = put.await.is_ok();
                (item, ok)
            })
            .buffer_unordered(PUT_CONCURRENCY)
            .collect()
            .await;
        pending = Vec::new();
        for (item, ok) in results {
            if ok {
                succeeded.push(item.tag);
            } else {
                pending.push(item);
            }
        }
        if !pending.is_empty() {
            if let Some(ms) = PUT_RETRY_DELAYS_MS.get(attempt) {
                sleeper.sleep_ms(*ms).await;
            }
        }
    }
    PutOutcome {
        succeeded,
        failed: pending.into_iter().map(|item| item.tag).collect(),
    }
}
