//! 分割の流れ: アップロード済みの zip を保存先から読み、CSV を運行NO ごとに分けて保存先に置き、
//! KUDGIVT を置けた運行に印 (`has_kudgivt`) を付ける (Refs ippoan/rust-alc-api#725)。
//!
//! backend (ippoan/rust-alc-api の `split_csv_from_r2`) と同じ順で、**置く key と中身のバイト列は同じ**
//! (読む側が object の ETag を指紋に使う)。1 エントリを分ける本体は backend と共有の
//! `alc_csv_parser::split_csv_entry` を呼ぶ (写しを持たない)。axum には依らない (口は [`crate::routes`])。
//!
//! DB はテナントを設定した接続で、テナントでも絞って引く ([`crate::pg`])。保存先の await を DB の
//! transaction の中に挟まない (接続の lock を取って `pg` の関数を呼び、返ったら離してから保存先へ)。

use std::collections::HashSet;
use std::fmt;
use std::io::{Cursor, Read};
use std::sync::Arc;

use alc_worker_db::PgClient;
use futures_util::lock::Mutex;
use uuid::Uuid;
use zip::ZipArchive;

use crate::pg;
use crate::store::{put_all_with_retry, ObjectStore, PutItem, Sleeper};

/// ログの重さ。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Warn,
    Error,
}

/// ログの差し込み口。この crate は `worker` に依らず、wasm では `tracing` に subscriber が無いので、
/// 載せる側 (直下の worker) が `console_warn!` / `console_error!` を渡す (テストは溜める偽物を渡す)。
///
/// **出すのは「固定の語 + 段の名前 + kind + 件数」まで。** key・運行NO・upload_id・テナント ID・
/// エラーの生の文を出さない。
pub type LogSink = Arc<dyn Fn(LogLevel, &str) + Send + Sync>;

/// [`split_upload`] の結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitOutcome {
    /// 回を使い切っても置けなかった CSV の数 (KUDGIVT 以外も数える)
    pub put_failed: usize,
    /// KUDGIVT の CSV を置けた運行NO
    pub succeeded_unko_nos: Vec<String>,
    /// KUDGIVT の CSV を置けなかった運行NO
    pub failed_unko_nos: Vec<String>,
}

/// [`split_upload`] の失敗。`Display` は段の名前と kind だけ (key・運行NO・テナント・エラーの生の文を含まない)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitError {
    /// アップロードの行が無い、または zip の key が入っていない
    NotFound,
    /// DB の失敗。中身は `alc_worker_db::kind` の label (SQLSTATE か、接続の失敗の種類)
    Db(String),
    /// zip が保存先に無い、または読めない
    Storage,
    /// zip を開けない、または展開できないエントリが在る
    Zip,
}

impl fmt::Display for SplitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("not found"),
            Self::Db(kind) => write!(f, "db ({kind})"),
            Self::Storage => f.write_str("storage"),
            Self::Zip => f.write_str("zip"),
        }
    }
}

impl std::error::Error for SplitError {}

fn db_error(e: tokio_postgres::Error) -> SplitError {
    SplitError::Db(alc_worker_db::kind(&e))
}

fn open_zip(zip_bytes: &[u8]) -> Result<ZipArchive<Cursor<&[u8]>>, SplitError> {
    ZipArchive::new(Cursor::new(zip_bytes)).map_err(|_| SplitError::Zip)
}

/// `index` 番目のエントリを展開する → (名前, 中身)。名前は zip に書かれた生の値。
fn read_entry(
    archive: &mut ZipArchive<Cursor<&[u8]>>,
    index: usize,
) -> Result<(String, Vec<u8>), SplitError> {
    let mut file = archive.by_index(index).map_err(|_| SplitError::Zip)?;
    let name = file.name().to_string();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|_| SplitError::Zip)?;
    Ok((name, bytes))
}

/// アップロード 1 件を分割する。
///
/// 1. zip の key を引く (行が無い・key が NULL は [`SplitError::NotFound`])
/// 2. zip を保存先から読む
/// 3. **全エントリが展開できることを先に確かめる** (ここまでは保存先に何も書かない。backend は全エントリを
///    展開し終えてから置き始め、`.csv` 以外の壊れたエントリでも失敗する — それを保つ)
/// 4. エントリを 1 つずつ: 展開 → 運行NO ごとに分ける → 置く (失敗したものだけやり直す) → 中身を手放して次へ
///    (メモリに載るのは zip 全体と、処理中の 1 エントリだけ)
/// 5. KUDGIVT を置けた運行に印を付ける (失敗は握り潰さない。保存先は書き終えているが、backend も同じ扱い)
pub async fn split_upload(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    sleeper: &dyn Sleeper,
    log: &LogSink,
    tenant_id: Uuid,
    upload_id: Uuid,
) -> Result<SplitOutcome, SplitError> {
    let key = {
        let mut client = pg.lock().await;
        pg::upload_zip_key(&mut client, tenant_id, upload_id).await
    };
    let key = key.map_err(db_error)?.ok_or(SplitError::NotFound)?;

    let zip_bytes = store.get(&key).await.map_err(|_| SplitError::Storage)?;
    let zip_bytes = zip_bytes.ok_or(SplitError::Storage)?;

    // 1 巡目 (検査): 全エントリを最後まで読んで捨てる
    let mut archive = open_zip(&zip_bytes)?;
    for index in 0..archive.len() {
        read_entry(&mut archive, index)?;
    }

    // 2 巡目 (処理)
    let key_tenant = tenant_id.to_string();
    let mut put_failed = 0usize;
    let mut succeeded_unko_nos: Vec<String> = Vec::new();
    let mut failed_unko_nos: Vec<String> = Vec::new();
    for index in 0..archive.len() {
        let (name, bytes) = read_entry(&mut archive, index)?;
        let items: Vec<PutItem<(bool, String)>> =
            alc_csv_parser::split_csv_entry(&key_tenant, &name, &bytes)
                .into_iter()
                .map(|f| PutItem {
                    key: f.key,
                    bytes: f.content,
                    content_type: "text/csv",
                    tag: (f.is_kudgivt, f.unko_no),
                })
                .collect();
        drop(bytes);
        let outcome = put_all_with_retry(store, sleeper, items).await;
        put_failed += outcome.failed.len();
        // 運行NO は、KUDGIVT を置けたものだけを「成功」に積む (置けていないのに印を付けない)
        succeeded_unko_nos.extend(kudgivt_unko_nos(outcome.succeeded));
        failed_unko_nos.extend(kudgivt_unko_nos(outcome.failed));
    }
    if put_failed > 0 {
        let message = format!("split: PUT failed for {put_failed} files");
        log(LogLevel::Warn, &message);
    }

    if !succeeded_unko_nos.is_empty() {
        let marked = {
            let mut client = pg.lock().await;
            pg::mark_has_kudgivt(&mut client, tenant_id, succeeded_unko_nos.clone()).await
        };
        let matched: HashSet<String> = marked.map_err(db_error)?.into_iter().collect();
        // 運行NO は乗務員ごとに複数行あることが在るので、行数ではなく集合で比べる。当たらなかったものは、
        // 突合キーのずれ (保存先の側は整えない生の文字列) を疑う材料として件数を出す。
        // backend の `find_unmatched_kudgivt_unko_nos` と同じ数え方 — rust-alc-api 側で共有の関数になったら置き換える
        let unmatched = succeeded_unko_nos
            .iter()
            .filter(|u| !matched.contains(u.as_str()))
            .count();
        if unmatched > 0 {
            let message = format!("split: has_kudgivt not applied: {unmatched} unko_no(s)");
            log(LogLevel::Warn, &message);
        }
    }

    Ok(SplitOutcome {
        put_failed,
        succeeded_unko_nos,
        failed_unko_nos,
    })
}

/// tag (`(is_kudgivt, unko_no)`) のうち、KUDGIVT のものの運行NO。
fn kudgivt_unko_nos(tags: Vec<(bool, String)>) -> impl Iterator<Item = String> {
    tags.into_iter()
        .filter(|(is_kudgivt, _)| *is_kudgivt)
        .map(|(_, unko_no)| unko_no)
}
