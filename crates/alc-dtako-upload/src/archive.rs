//! アップロードされた zip の展開 (Refs ippoan/rust-alc-api#725)。分割 ([`crate::split`]) と取り込み ([`crate::ingest`]) が使う。
//!
//! 圧縮は deflate と無圧縮だけ (`zip` の feature)。**展開後の大きさに上限**が在る (wasm の線形メモリは 1 度伸びると縮まない):
//! 開くときに、zip に書かれた非圧縮サイズの合計が上限を超えていれば展開せずに [`ArchiveError::TooLarge`]。
//! 読むときも、エントリごとに「書かれた非圧縮サイズ」までしか読まない (書かれた値より中身が大きい zip は [`ArchiveError::Invalid`])。

use std::io::{Cursor, Read};

use zip::ZipArchive;

/// 非圧縮サイズの合計の上限 (既定)。
pub(crate) const MAX_UNCOMPRESSED_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArchiveError {
    /// zip として開けない、展開できないエントリが在る、または中身が書かれた大きさと合わない
    Invalid,
    /// 非圧縮サイズの合計が上限を超える
    TooLarge,
}

pub(crate) struct Archive<'a> {
    zip: ZipArchive<Cursor<&'a [u8]>>,
}

impl<'a> Archive<'a> {
    /// zip を開き、非圧縮サイズの合計が `max_uncompressed_bytes` 以下であることを確かめる (ここでは展開しない)。
    pub(crate) fn open(
        zip_bytes: &'a [u8],
        max_uncompressed_bytes: u64,
    ) -> Result<Self, ArchiveError> {
        let mut zip = ZipArchive::new(Cursor::new(zip_bytes)).map_err(|_| ArchiveError::Invalid)?;
        // 合計が分からない zip (大きさを後ろに書く形のエントリが在る) は、エントリごとの値を足す
        let total = match zip.decompressed_size() {
            Some(total) => total,
            None => declared_total(&mut zip)?,
        };
        if total > u128::from(max_uncompressed_bytes) {
            return Err(ArchiveError::TooLarge);
        }
        Ok(Self { zip })
    }

    pub(crate) fn len(&self) -> usize {
        self.zip.len()
    }

    /// `index` 番目のエントリを展開する → (名前, 中身)。名前は zip に書かれた生の値。
    pub(crate) fn read_entry(&mut self, index: usize) -> Result<(String, Vec<u8>), ArchiveError> {
        let file = self.zip.by_index(index);
        let file = file.map_err(|_| ArchiveError::Invalid)?;
        let name = file.name().to_string();
        let declared = file.size();
        let mut bytes = Vec::new();
        let mut limited = file.take(declared.saturating_add(1));
        let read = limited.read_to_end(&mut bytes);
        read.map_err(|_| ArchiveError::Invalid)?;
        if bytes.len() as u64 > declared {
            return Err(ArchiveError::Invalid);
        }
        Ok((name, bytes))
    }
}

/// エントリごとに書かれた非圧縮サイズの合計 (展開しない)。
fn declared_total(zip: &mut ZipArchive<Cursor<&[u8]>>) -> Result<u128, ArchiveError> {
    let mut total = 0u128;
    for index in 0..zip.len() {
        let file = zip.by_index_raw(index);
        total += u128::from(file.map_err(|_| ArchiveError::Invalid)?.size());
    }
    Ok(total)
}
