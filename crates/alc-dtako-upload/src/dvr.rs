//! theearth の DVR (ドライブレコーダー) 動画通知の受け皿の、入力の検査・定数・応答の形 (W5a、Refs ippoan/rust-alc-api#680。
//! 元は ohishi-exp/nuxt-dtako-admin#1094)。
//!
//! dtako-scraper-relay の cron が theearth から動画一覧を取り、
//!
//! 1. `POST /dvr/notifications` — 一覧をバッチで起票する。応答の `pending` (新規 + まだ取りに行く価値のある既存行) を relay が回り、
//! 2. `POST /dvr/files/{id}` — `.vdf` の生バイトを送る。ここで保存先 (R2) に置く。
//!
//! どちらも auth-worker の内部用 proxy (shared-secret のクラス) から届く = **呼び手がテナントを名乗る**ので、行を引く文は
//! RLS 任せにせず `tenant_id` も明示する (SQL は `repo::sql`)。**値の上限・検査・key の形・応答の形は rust-alc-api の
//! `alc_dtako::dvr_notifications` と同じ**。通知 (LINE WORKS) はここでは出さない (relay が出す)。

use axum::body::Body;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 1 リクエストで受けるバッチの上限 (超えたら 400。relay が分けて送る)。
pub const MAX_BATCH_ITEMS: usize = 200;

/// `serial_no` / `file_name` の長さの上限。どちらも theearth 由来で、保存先の key の部品になる。
pub const MAX_SERIAL_NO_LEN: usize = 64;
pub const MAX_FILE_NAME_LEN: usize = 128;

/// 表示用の付随情報 (vehicle_cd / vehicle_name / driver_name / event_type) の長さの上限 (文字種は縛らない)。
pub const MAX_TEXT_FIELD_LEN: usize = 256;

/// `source_url` の長さの上限。**provenance 専用**の欄 (重複判定にも表示にも使わない) なので、
/// 無制限を避けるためだけの大きな値にする (ここで弾くと意味の無い欄のせいでバッチ全体が 400 になる)。
pub const MAX_SOURCE_URL_LEN: usize = 2048;

/// 実体の取得を諦めるまでの試行回数 (これ以上の行は `pending` に載せない)。
pub const MAX_FILE_ATTEMPTS: i32 = 6;

/// 受け付ける `.vdf` の最大サイズ (32MB)。超えたら 413 + `file_status = 'failed'`。
pub const MAX_FILE_BYTES: usize = 32 * 1024 * 1024;

/// 実体の取得の失敗として残す固定の語 (`last_error`)。**生のエラー文を入れない**。
pub const ERROR_TOO_LARGE: &str = "file exceeds 33554432 bytes";
pub const ERROR_BODY_STREAM: &str = "body stream error";
pub const ERROR_STORE: &str = "R2 upload failed";

/// 1 件ぶんの通知 (theearth の一覧の 1 行)。
#[derive(Debug, Clone, Deserialize)]
pub struct DvrNotificationCreate {
    pub serial_no: String,
    pub file_name: String,
    pub vehicle_cd: Option<String>,
    pub vehicle_name: Option<String>,
    pub driver_name: Option<String>,
    pub event_type: Option<String>,
    /// theearth が示す録画日時 (RFC 3339)。
    pub dvr_datetime: Option<DateTime<Utc>>,
    /// theearth の通知行が持つ source 側のパス (provenance 専用)。
    pub source_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DvrIngestBody {
    pub items: Vec<DvrNotificationCreate>,
}

/// relay が次に実体を取りに行くべき 1 行。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DvrPendingItem {
    pub id: Uuid,
    pub serial_no: String,
    pub file_name: String,
}

/// `POST /dvr/notifications` の応答。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DvrIngestResponse {
    /// 今回新しく作った行の数
    pub inserted: i64,
    /// 自然キーが既に在って作らなかった数 (`pending` に再掲したものも含む)
    pub skipped: i64,
    /// 実体をまだ取りに行くべき行 (今回作った行 + 既存で `pending` かつ試行回数が上限未満の行)
    pub pending: Vec<DvrPendingItem>,
}

/// `POST /dvr/files/{id}` の応答。
#[derive(Debug, Serialize)]
pub struct DvrFileStoredResponse {
    pub id: Uuid,
    pub file_status: &'static str,
    pub size: i64,
    pub r2_key: String,
}

/// 保存先の key の部品として安全か (英数字と `-` `_` `.` だけ、`..` を含まない)。
fn valid_key_component(v: &str, max_len: usize) -> bool {
    !v.is_empty()
        && v.len() <= max_len
        && !v.contains("..")
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

/// 表示用の欄は長さだけ見る (車番・氏名は日本語が入る)。
fn valid_text_field(v: Option<&String>) -> bool {
    v.map(|s| s.chars().count() <= MAX_TEXT_FIELD_LEN)
        .unwrap_or(true)
}

/// 1 件の検査 (rust-alc-api と同じ規則)。
pub fn validate(item: &DvrNotificationCreate) -> bool {
    valid_key_component(&item.serial_no, MAX_SERIAL_NO_LEN)
        && valid_key_component(&item.file_name, MAX_FILE_NAME_LEN)
        && valid_text_field(item.vehicle_cd.as_ref())
        && valid_text_field(item.vehicle_name.as_ref())
        && valid_text_field(item.driver_name.as_ref())
        && valid_text_field(item.event_type.as_ref())
        && item
            .source_url
            .as_ref()
            .map(|u| u.chars().count() <= MAX_SOURCE_URL_LEN)
            .unwrap_or(true)
}

/// 保存先の key (`{tenant_id}/dvr/{serial_no}/{file_name}`。dtako の prefix の規約に揃える)。
pub fn r2_key(tenant_id: Uuid, serial_no: &str, file_name: &str) -> String {
    format!("{tenant_id}/dvr/{serial_no}/{file_name}")
}

/// body の読みの失敗。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadError {
    /// [`MAX_FILE_BYTES`] を超えた
    TooLarge,
    /// stream そのものが壊れた
    Stream,
}

/// body を最大 [`MAX_FILE_BYTES`] まで読む (`Body` は axum の body の上限の外なので、読みながら自分で数える)。
pub async fn read_capped(body: Body) -> Result<Vec<u8>, ReadError> {
    let mut stream = body.into_data_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| ReadError::Stream)?;
        if buf.len() + chunk.len() > MAX_FILE_BYTES {
            return Err(ReadError::TooLarge);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}
