//! 保存先の層 (`alc_dtako_upload::store`) の Workers での実装: R2 の binding と、待ち。
//!
//! `ObjectStore`・`Sleeper` は `Send + Sync` で `Send` な future を返す (axum の state と handler が要求する)。
//! R2 の値と future は JS の値で `Send` でないので、`worker::send` の `SendWrapper`・`SendFuture` で包む
//! (Workers は単一 thread)。
//!
//! エラーは段の名前だけを持つ `StoreError` に落とす。**key とランタイムの生のエラー文はログに出さない。**
//!
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging の実物)。

use std::time::Duration;

use alc_dtako_upload::store::{ObjectStore, Sleeper, StoreError};
use futures_util::future::BoxFuture;
use worker::send::{SendFuture, SendWrapper};
use worker::{Bucket, Delay, HttpMetadata};

/// R2 の bucket (binding `DTAKO_R2`)。
pub struct R2Store(SendWrapper<Bucket>);

impl R2Store {
    pub fn new(bucket: Bucket) -> Self {
        Self(SendWrapper::new(bucket))
    }
}

impl ObjectStore for R2Store {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, StoreError>> {
        Box::pin(SendFuture::new(async move {
            let object = self.0.get(key).execute().await;
            let Some(object) = object.map_err(|_| StoreError::new("get"))? else {
                return Ok(None);
            };
            let body = object.body().ok_or(StoreError::new("get body"))?;
            let bytes = body
                .bytes()
                .await
                .map_err(|_| StoreError::new("read body"))?;
            Ok(Some(bytes))
        }))
    }

    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(SendFuture::new(async move {
            let metadata = HttpMetadata {
                content_type: Some(content_type.to_owned()),
                ..Default::default()
            };
            let put = self
                .0
                .put(key, bytes)
                .http_metadata(metadata)
                .execute()
                .await;
            put.map(|_| ()).map_err(|_| StoreError::new("put"))
        }))
    }
}

/// `worker::Delay` での待ち。
pub struct WorkerSleeper;

impl Sleeper for WorkerSleeper {
    fn sleep_ms(&self, ms: u64) -> BoxFuture<'_, ()> {
        Box::pin(SendFuture::new(async move {
            Delay::from(Duration::from_millis(ms)).await;
        }))
    }
}
