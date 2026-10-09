//! アップロードの取り込みの流れ: zip を保存先に置き、運行と日別を DB に入れ、運行NO ごとの CSV に分割する
//! (Refs ippoan/rust-alc-api#725)。backend (ippoan/rust-alc-api) の `POST /api/upload` と同じ仕事・同じ順。
//! axum の口は [`crate::routes`] (ここが axum から借りるのは `Bytes` の型だけ)。
//!
//! 段の順 ([`ingest_upload`]):
//! 1. 履歴を作る ([`pg::create_upload`])
//! 2. zip を保存先に置く (key = `{テナント}/uploads/{履歴の id}/{filename}`)
//! 3. key を履歴に記録する (単独の transaction。後の段が落ちても残る)
//! 4. zip を展開して KUDGURI と KUDGIVT を読む (KUDGURI が 0 行なら、KUDGIVT が無くても運行 0 件として進む)
//! 5. 準備 ([`pg::prepare_upload`])
//! 6. 「既に在る」運行だけ、分割済みの旧 KUDGIVT を保存先から読んで前回の分数を出す (運行NO ごとに 1 回、まとめて同時に読む。
//!    取れなければ `None`。取り込みは続ける)。
//!    今回の分数と日別は、共有の関数 (`alc_csv_parser::operation_changes`・`alc_compare::upload_daily`) で出す
//! 7. 運行の入れ替え + 日別の保存 + 完了の印 ([`pg::apply_upload`]。1 transaction)
//! 8. 分割 ([`split_upload`])。丸ごと失敗したら待って、全体を最大 [`PUT_RETRY_ATTEMPTS`] 回。尽きても取り込みは成功のまま
//! 9. 日別の計算し直し (`recalc_daily`。分割が成功したときだけ)。今回の行の乗務員 × その行の運行日・読取日の月ごとに、
//!    乗務員の再計算の口と同じ数え方 (乗務員の月の運行をまとめ、運行ごとの分割の出力で計算する) で 7 の日別を上書きする。
//!    計算に渡す運行は、今回の運行を含む束ねとその前後の束ねに縮める ([`crate::narrow`]。縮めた計算が月全体の計算と同じになると
//!    言える形だけ。今回の行に取り込みの前から在った運行が在るとき・やり直しは、いつも月全体)。
//!    失敗しても取り込みは成功のまま (7 の値が残る。ログは件数だけ)。保存先の GET は [`IngestLimits::max_daily_recalc_gets`] まで
//!
//! やり直し ([`rerun_upload`]) は、既に保存先に在る zip をもう一度取り込む: 履歴の zip の key を引く → zip を保存先から読む →
//! 上の 4〜8 をそのまま通す。履歴を作らない・zip を置き直さない・key を更新しない。
//!
//! 2〜7 (やり直しでは、key を引けた後) が失敗したら、履歴に失敗の印 ([`pg::mark_upload_failed`]) を付けてから失敗を返す。保存先の await を DB の
//! transaction の中に挟まない (接続の lock を取って `pg` の関数を呼び、返ったら離してから保存先へ)。
//!
//! 段ごとの所要は [`StageTimer`] に記録する (段の名前は固定の語: `history`・`put_zip`・`parse`・`prepare`・`old_kudgivt`・`apply`・`split`・`daily`、
//! やり直しは頭が `zip_key`・`get_zip`)。失敗したときは、終えた段までが残る。
//!
//! **失敗の文・ログ・履歴の `error_message` は固定の語だけ** ([`IngestError::label`])。parser や DB の生の文・入力の値・
//! key・運行NO・テナント ID を出さない。

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use alc_compare::upload_daily::{compute_daily_hours, DailyHours};
use alc_compare::DayKey;
use alc_csv_parser::kudgivt::{parse_kudgivt_for_crew, KudgivtRow};
use alc_csv_parser::kudguri::KudguriRow;
use alc_csv_parser::operation_changes::{minutes_for, minutes_from_events, OperationMinutes};
use alc_worker_db::PgClient;
use axum::body::Bytes;
use chrono::{Datelike, Duration, NaiveDate};
use futures_util::lock::Mutex;
use uuid::Uuid;

use crate::archive::{Archive, ArchiveError, MAX_UNCOMPRESSED_BYTES};
use crate::narrow::{narrow, Window, Written};
use crate::pg::{self, CreateUploadError, OperationInput, PreparedRow};
use crate::recalc::{
    compute_split_daily, kudguri_rows, month_range, month_unko_nos, unko_nos_of, SplitDaily,
};
use crate::split::{split_upload, LogLevel, LogSink, SplitOutcome};
use crate::store::{get_all, ObjectStore, Sleeper, PUT_RETRY_ATTEMPTS, PUT_RETRY_DELAYS_MS};
use crate::timing::StageTimer;

/// 取り込みの上限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestLimits {
    /// zip の非圧縮サイズの合計の上限 (超える zip は入力の誤り)
    pub max_uncompressed_bytes: u64,
    /// 日別の計算し直し (段 9) で使う保存先の GET の上限。超える乗務員 × 月から先は飛ばす
    pub max_daily_recalc_gets: usize,
}

/// 日別の計算し直しの GET の既定の上限。Workers Paid の subrequest の上限 (1 リクエスト 10,000) を、分割の PUT と分け合う。
pub const DAILY_RECALC_MAX_GETS: usize = 4000;

impl Default for IngestLimits {
    fn default() -> Self {
        Self {
            max_uncompressed_bytes: MAX_UNCOMPRESSED_BYTES,
            max_daily_recalc_gets: DAILY_RECALC_MAX_GETS,
        }
    }
}

/// 段 9 の対象と、計算に渡す運行を縮める材料。
struct DailyPlan {
    /// 日別を計算し直す対象 (年, 月, 乗務員)。月ごと・乗務員ごとの順に並ぶ
    targets: BTreeSet<(i32, u32, Uuid)>,
    /// 乗務員ごとの今回の運行NO
    new_unko_nos: HashMap<Uuid, BTreeSet<String>>,
    /// 段 7 が書いた日エントリ
    written: Vec<Written>,
    /// 縮めてよいか (今回の行に、取り込みの前から在った運行が無い。やり直しでは `false`)
    narrow: bool,
}

impl DailyPlan {
    /// 乗務員 (`driver_id`・乗務員CD `driver_cd`) の月の行 `rows` のうち、計算に渡す範囲。縮めないなら `None`。
    fn window(&self, driver_id: Uuid, driver_cd: &str, rows: &[KudguriRow]) -> Option<Window> {
        let new_unko_nos = self.new_unko_nos.get(&driver_id).filter(|_| self.narrow)?;
        narrow(rows, new_unko_nos, driver_cd, &self.written)
    }
}

/// 月の運行の運行NO (日別の保存で消す対象。月ごとに 1 回引いて乗務員で使い回す)。
type MonthUnkoNos = Arc<Vec<String>>;

/// [`ingest_upload`]・[`rerun_upload`] の結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    pub upload_id: Uuid,
    /// 流した KUDGURI の行数 (運行NO の種類の数ではない)
    pub operations_count: i32,
    /// 分割の結果。分割が回を使い切って失敗したら `put_failed = 1` で、運行NO の一覧は空
    pub split: SplitOutcome,
}

/// [`ingest_upload`]・[`rerun_upload`] の失敗。`Db`・`Storage` はこちら側の失敗、`NotFound` は対象が無い、ほかは入力の誤り。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestError {
    /// やり直しの対象が無い (履歴が無い・別のテナントの履歴・zip の key が入っていない)
    NotFound,
    /// テナントが存在しない (履歴は作られていない)
    TenantNotFound,
    /// zip として開けない、または展開できないエントリが在る
    InvalidZip,
    /// zip の非圧縮サイズの合計が上限を超える
    ZipTooLarge,
    /// 名前に KUDGURI を含むエントリが無い
    KudguriNotFound,
    /// KUDGURI を読めない (必須の列が無い・読取日が日付でない など)
    KudguriInvalid,
    /// KUDGURI に行が在るのに、名前に KUDGIVT を含むエントリが無い
    KudgivtNotFound,
    /// KUDGIVT を読めない
    KudgivtInvalid,
    /// DB の失敗。中身は `alc_worker_db::kind` の label (SQLSTATE か、接続の失敗の種類)
    Db(String),
    /// zip を保存先に置けない (やり直しでは、保存先から読めない・無い)
    Storage,
}

impl IngestError {
    /// 固定の語 (応答の本文・履歴の `error_message` に使う。識別子も生の文も含まない)。
    pub fn label(&self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::TenantNotFound => "tenant_not_found",
            Self::InvalidZip => "invalid_zip",
            Self::ZipTooLarge => "zip_too_large",
            Self::KudguriNotFound => "kudguri_not_found",
            Self::KudguriInvalid => "kudguri_invalid",
            Self::KudgivtNotFound => "kudgivt_not_found",
            Self::KudgivtInvalid => "kudgivt_invalid",
            Self::Db(_) => "db",
            Self::Storage => "storage",
        }
    }

    /// こちら側の失敗か (そうでなければ、入力の誤りか、対象が無い)。
    pub fn is_internal(&self) -> bool {
        matches!(self, Self::Db(_) | Self::Storage)
    }
}

/// 段の名前と kind だけ (ログ用)。
impl fmt::Display for IngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Db(kind) => write!(f, "db ({kind})"),
            other => f.write_str(other.label()),
        }
    }
}

impl std::error::Error for IngestError {}

fn db_error(e: tokio_postgres::Error) -> IngestError {
    IngestError::Db(alc_worker_db::kind(&e))
}

fn archive_error(e: ArchiveError) -> IngestError {
    match e {
        ArchiveError::Invalid => IngestError::InvalidZip,
        ArchiveError::TooLarge => IngestError::ZipTooLarge,
    }
}

/// zip 1 つを取り込む (段は module の doc)。
#[allow(clippy::too_many_arguments)]
pub async fn ingest_upload(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    sleeper: &dyn Sleeper,
    log: &LogSink,
    limits: IngestLimits,
    timer: &mut StageTimer<'_>,
    tenant_id: Uuid,
    filename: String,
    zip_bytes: Bytes,
) -> Result<IngestOutcome, IngestError> {
    let created = {
        let mut client = pg.lock().await;
        pg::create_upload(&mut client, tenant_id, filename.clone()).await
    };
    let upload_id = created.map_err(|e| match e {
        CreateUploadError::TenantNotFound => IngestError::TenantNotFound,
        CreateUploadError::Db(e) => db_error(e),
    })?;
    timer.lap("history");

    let applied = async {
        store_zip(pg, store, tenant_id, upload_id, &filename, &zip_bytes).await?;
        timer.lap("put_zip");
        import_zip(
            pg, store, log, limits, timer, tenant_id, upload_id, zip_bytes,
        )
        .await
    };
    let applied = applied.await;
    let ctx = Finish::new("upload", limits, tenant_id, upload_id);
    finish(pg, store, sleeper, log, timer, ctx, applied).await
}

/// 既に保存先に在る zip を、もう一度取り込む (失敗した履歴の復旧に使う)。
///
/// 履歴の zip の key を引き ([`pg::upload_zip_key`]。履歴が無い・別のテナントの履歴・key が NULL は [`IngestError::NotFound`])、
/// zip を保存先から読んで、[`ingest_upload`] の段 4〜8 を通す。履歴の status は、始めるときには変えない
/// (成功で completed と行数、key を引けた後の失敗で failed)。
#[allow(clippy::too_many_arguments)]
pub async fn rerun_upload(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    sleeper: &dyn Sleeper,
    log: &LogSink,
    limits: IngestLimits,
    timer: &mut StageTimer<'_>,
    tenant_id: Uuid,
    upload_id: Uuid,
) -> Result<IngestOutcome, IngestError> {
    let key = {
        let mut client = pg.lock().await;
        pg::upload_zip_key(&mut client, tenant_id, upload_id).await
    };
    let key = key.map_err(db_error)?.ok_or(IngestError::NotFound)?;
    timer.lap("zip_key");

    let applied = async {
        let zip_bytes = store.get(&key).await.ok().flatten();
        let zip_bytes = Bytes::from(zip_bytes.ok_or(IngestError::Storage)?);
        timer.lap("get_zip");
        import_zip(
            pg, store, log, limits, timer, tenant_id, upload_id, zip_bytes,
        )
        .await
    };
    let applied = applied.await;
    let ctx = Finish::new("rerun", limits, tenant_id, upload_id);
    finish(pg, store, sleeper, log, timer, ctx, applied).await
}

/// [`finish`] が使う値 (`what` = ログの頭。やり直し (`rerun`) は段 9 で計算に渡す運行を縮めない)。
struct Finish {
    what: &'static str,
    narrow: bool,
    max_gets: usize,
    tenant_id: Uuid,
    upload_id: Uuid,
}

impl Finish {
    fn new(what: &'static str, limits: IngestLimits, tenant_id: Uuid, upload_id: Uuid) -> Self {
        let max_gets = limits.max_daily_recalc_gets;
        Self {
            what,
            narrow: what != "rerun",
            max_gets,
            tenant_id,
            upload_id,
        }
    }
}

/// 取り込みの結果を受けて締める: 失敗なら履歴に失敗の印を付けて返し、成功なら分割 (段 8) と日別の計算し直し (段 9) へ進む。
async fn finish(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    sleeper: &dyn Sleeper,
    log: &LogSink,
    timer: &mut StageTimer<'_>,
    ctx: Finish,
    applied: Result<(i32, DailyPlan), IngestError>,
) -> Result<IngestOutcome, IngestError> {
    let (tenant_id, upload_id) = (ctx.tenant_id, ctx.upload_id);
    let (operations_count, mut plan) = match applied {
        Ok(applied) => applied,
        Err(e) => {
            // 印を付けること自体の失敗は握る (返すのは元の失敗)
            let mut client = pg.lock().await;
            let label = e.label().to_owned();
            let _ = pg::mark_upload_failed(&mut client, tenant_id, upload_id, label).await;
            return Err(e);
        }
    };

    let split = split_with_retry(pg, store, sleeper, log, tenant_id, upload_id).await;
    timer.lap("split");
    if split.put_failed == 0 {
        plan.narrow &= ctx.narrow;
        recalc_daily(pg, store, log, &ctx, plan).await;
    } else {
        log(
            LogLevel::Warn,
            &format!("{}: daily recalc skipped: split failed", ctx.what),
        );
    }
    timer.lap("daily");
    Ok(IngestOutcome {
        upload_id,
        operations_count,
        split,
    })
}

/// 段 2〜3: zip を保存先に置き、key を履歴に記録する。filename は加工せず key に入れる (backend と同じ key)。
async fn store_zip(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    tenant_id: Uuid,
    upload_id: Uuid,
    filename: &str,
    zip_bytes: &Bytes,
) -> Result<(), IngestError> {
    let key = format!("{tenant_id}/uploads/{upload_id}/{filename}");
    let put = store.put(&key, zip_bytes.to_vec(), "application/zip");
    put.await.map_err(|_| IngestError::Storage)?;
    let recorded = {
        let mut client = pg.lock().await;
        pg::set_upload_zip_key(&mut client, tenant_id, upload_id, key).await
    };
    recorded.map_err(db_error)?;
    Ok(())
}

/// 段 4〜7 (アップロードとやり直しで同じ)。返すのは流した行数と、段 9 で日別を計算し直す対象 (と縮める材料)。
#[allow(clippy::too_many_arguments)]
async fn import_zip(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    log: &LogSink,
    limits: IngestLimits,
    timer: &mut StageTimer<'_>,
    tenant_id: Uuid,
    upload_id: Uuid,
    zip_bytes: Bytes,
) -> Result<(i32, DailyPlan), IngestError> {
    // 4: 読み終えたら zip を手放す
    let (rows, kudgivt_rows) = read_rows(&zip_bytes, limits)?;
    drop(zip_bytes);
    let (rows, kudgivt_rows) = (Arc::new(rows), Arc::new(kudgivt_rows));
    timer.lap("parse");

    // 5
    let prepared = {
        let mut client = pg.lock().await;
        pg::prepare_upload(&mut client, tenant_id, rows.clone(), kudgivt_rows.clone()).await
    };
    let prepared = prepared.map_err(db_error)?;
    timer.lap("prepare");
    let mut plan = daily_plan(&rows, &prepared.rows);

    // 6
    let before = before_minutes(store, log, tenant_id, &rows, &prepared.rows).await;
    let inputs: Vec<OperationInput> = rows
        .iter()
        .zip(&prepared.rows)
        .zip(before)
        .map(|((row, p), before_minutes)| OperationInput {
            office_id: p.office_id,
            vehicle_id: p.vehicle_id,
            driver_id: p.driver_id,
            before_minutes,
            after_minutes: minutes_for(&kudgivt_rows, &row.unko_no, row.crew_role),
        })
        .collect();
    // フェリーは空 (アップロードの時点では、まだ保存先に分割されていない。backend と同じ)
    let classifications = prepared.classification_map();
    let daily = compute_daily_hours(&rows, &kudgivt_rows, &classifications, &HashMap::new());
    let written = daily
        .iter()
        .map(|((cd, date, _), h)| (cd.clone(), *date, h.unko_nos.clone()));
    plan.written = written.collect();
    drop(kudgivt_rows);
    timer.lap("old_kudgivt");

    // 7
    let applied = {
        let mut client = pg.lock().await;
        pg::apply_upload(&mut client, tenant_id, upload_id, rows, inputs, daily).await
    };
    let count = applied.map_err(|e| IngestError::Db(e.kind()))?;
    timer.lap("apply");
    Ok((count, plan))
}

/// 今回の行の乗務員 (引き当てられたもの) × その行の運行日・読取日の (年, 月) (月末の運行なら 2 か月になる) と、
/// 乗務員ごとの今回の運行NO。段 7 が書いた日エントリは、計算した後で呼び手が入れる。
fn daily_plan(rows: &[KudguriRow], prepared: &[PreparedRow]) -> DailyPlan {
    let mut targets = BTreeSet::new();
    let mut new_unko_nos: HashMap<Uuid, BTreeSet<String>> = HashMap::new();
    let drivers = prepared.iter().map(|p| p.driver_id);
    for (row, driver_id) in rows.iter().zip(drivers).filter_map(|(r, d)| Some((r, d?))) {
        for date in [Some(row.reading_date), row.operation_date]
            .into_iter()
            .flatten()
        {
            targets.insert((date.year(), date.month(), driver_id));
        }
        let unko_no = row.unko_no.clone();
        new_unko_nos.entry(driver_id).or_default().insert(unko_no);
    }
    DailyPlan {
        targets,
        new_unko_nos,
        written: Vec::new(),
        narrow: !prepared.iter().any(|p| p.exists),
    }
}

/// 段 9: 乗務員 × 月ごとに、乗務員の一括の再計算の口の 1 人ぶんと同じ処理 (乗務員の月の運行 → 分割の出力で計算 →
/// 1 transaction で保存。消す対象の運行NO は月の運行のもの = 月ごとに 1 回引く) で日別を上書きする
/// (縮められる乗務員 × 月は、縮めた運行で計算し、保存する鎖の日エントリだけを、その鎖の運行NO を消す対象にして保存する)。
/// 失敗 (乗務員が無い・KUDGIVT が 1 件も無い・DB) は数えて続け、終わりに件数だけ Warn。
/// 保存先の GET (運行NO の数の 2 倍) が上限を越える対象から先は飛ばし、件数だけ Warn。
async fn recalc_daily(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    log: &LogSink,
    ctx: &Finish,
    plan: DailyPlan,
) {
    let what = ctx.what;
    let (mut gets, mut failed, mut skipped) = (0usize, 0usize, 0usize);
    // いま見ている月と、その月の運行の運行NO (引けなければ `None`)
    let mut month: Option<((i32, u32), Option<MonthUnkoNos>)> = None;
    for &(year, month_no, driver_id) in &plan.targets {
        if skipped > 0 {
            skipped += 1;
            continue;
        }
        let (start, end) = month_range(year, month_no).expect("the month of a date");
        let range = (start, end + Duration::days(1));
        if month.as_ref().map(|(key, _)| *key) != Some((year, month_no)) {
            let unko_nos = {
                let mut client = pg.lock().await;
                month_unko_nos(&mut client, ctx.tenant_id, range.0, range.1).await
            };
            month = Some(((year, month_no), unko_nos.ok().map(Arc::new)));
        }
        let all_unko_nos = month.as_ref().and_then(|(_, nos)| nos.clone());
        let target = DailyTarget {
            driver_id,
            range,
            all_unko_nos,
        };
        let budget = ctx.max_gets - gets;
        match recalc_target(pg, store, log, ctx, &plan, target, budget).await {
            Ok(Some(used)) => gets += used,
            Ok(None) => skipped += 1,
            Err(()) => failed += 1,
        }
    }
    if failed > 0 {
        log(
            LogLevel::Warn,
            &format!("{what}: daily recalc failed: {failed}"),
        );
    }
    if skipped > 0 {
        let message = format!("{what}: daily recalc skipped over the GET limit: {skipped}");
        log(LogLevel::Warn, &message);
    }
}

/// 段 9 の乗務員 × 月 1 つ (`range` = 月初と月末の翌日。`all_unko_nos` = 月の運行の運行NO、引けなければ `None`)。
struct DailyTarget {
    driver_id: Uuid,
    range: (NaiveDate, NaiveDate),
    all_unko_nos: Option<MonthUnkoNos>,
}

/// 乗務員 × 月 1 つを計算し直して保存する。使った GET の数を返す。GET が `budget` を越えるなら (そこから先を) 何もせず `None`。
/// 縮められれば縮めた運行で計算し、計算の結果が縮めた前提から外れたら月全体で計算し直す (GET は両方を足す)。
/// 失敗 (月の運行・乗務員が引けない・KUDGIVT が 1 件も無い・DB) は `Err`。
async fn recalc_target(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    log: &LogSink,
    ctx: &Finish,
    plan: &DailyPlan,
    target: DailyTarget,
    budget: usize,
) -> Result<Option<usize>, ()> {
    let tenant_id = ctx.tenant_id;
    let all_unko_nos = target.all_unko_nos.ok_or(())?;
    let (start, fetch_end) = target.range;
    let driver_id = target.driver_id;
    let loaded = {
        let mut client = pg.lock().await;
        pg::driver_operations_for_recalc(&mut client, tenant_id, driver_id, start, fetch_end).await
    };
    let (driver_cd, ops) = loaded.ok().flatten().ok_or(())?;
    let rows = kudguri_rows(ops);
    let mut used = 0;
    if let Some(window) = plan.window(driver_id, &driver_cd, &rows) {
        used = 2 * unko_nos_of(&window.rows).len();
        if used > budget {
            return Ok(None);
        }
        let daily = split_daily(pg, store, log, ctx, &window.rows).await?;
        if let Some(kept) = window.saved_days(daily) {
            let unko_nos = Arc::new(window.saved_unko_nos);
            save_daily(pg, tenant_id, kept, unko_nos).await?;
            return Ok(Some(used));
        }
    }
    let gets = used + 2 * unko_nos_of(&rows).len();
    if gets > budget {
        return Ok(None);
    }
    let daily = split_daily(pg, store, log, ctx, &rows).await?;
    save_daily(pg, tenant_id, daily, all_unko_nos).await?;
    Ok(Some(gets))
}

/// 運行の行を分割の出力で計算する。失敗 (KUDGIVT が 1 件も無い・DB) は `Err`。
async fn split_daily(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    log: &LogSink,
    ctx: &Finish,
    rows: &[KudguriRow],
) -> Result<HashMap<DayKey, DailyHours>, ()> {
    let computed = compute_split_daily(pg, store, log, ctx.what, ctx.tenant_id, rows).await;
    match computed.map_err(drop)? {
        SplitDaily::Daily(daily) => Ok(daily),
        SplitDaily::KudgivtNotFound => Err(()),
    }
}

/// 日エントリを 1 transaction で保存する (`unko_nos` = 消す対象の運行NO)。
async fn save_daily(
    pg: &Mutex<PgClient>,
    tenant_id: Uuid,
    daily: HashMap<DayKey, DailyHours>,
    unko_nos: MonthUnkoNos,
) -> Result<(), ()> {
    let mut client = pg.lock().await;
    let saved = pg::save_daily_hours_in_tx(&mut client, tenant_id, daily, unko_nos).await;
    saved.map_err(drop)
}

/// zip を展開して KUDGURI と KUDGIVT の行を読む。全エントリを展開できることも確かめる (展開できないエントリが在れば失敗)。
/// 手元に残すのは、名前に `KUDGURI`・`KUDGIVT` を含むエントリの中身だけ。
fn read_rows(
    zip_bytes: &[u8],
    limits: IngestLimits,
) -> Result<(Vec<KudguriRow>, Vec<KudgivtRow>), IngestError> {
    let mut archive =
        Archive::open(zip_bytes, limits.max_uncompressed_bytes).map_err(archive_error)?;
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    for index in 0..archive.len() {
        let (name, bytes) = archive.read_entry(index).map_err(archive_error)?;
        let upper = name.to_uppercase();
        if upper.contains("KUDGURI") || upper.contains("KUDGIVT") {
            files.push((name, bytes));
        }
    }
    let rows = alc_csv_parser::kudguri_rows_in(&files).ok_or(IngestError::KudguriNotFound)?;
    let rows = rows.map_err(|_| IngestError::KudguriInvalid)?;
    if rows.is_empty() {
        return Ok((rows, Vec::new()));
    }
    let kudgivt_rows =
        alc_csv_parser::kudgivt_rows_in(&files).ok_or(IngestError::KudgivtNotFound)?;
    let kudgivt_rows = kudgivt_rows.map_err(|_| IngestError::KudgivtInvalid)?;
    Ok((rows, kudgivt_rows))
}

/// 行ごとの前回の分数 (`rows` と同じ順・同じ数)。「既に在る」行だけ、分割済みの旧 KUDGIVT
/// (`{テナント}/unko/{運行NO}/KUDGIVT.csv`) を運行NO ごとに 1 回読む (まとめて、同時 [`crate::store::GET_CONCURRENCY`] 本まで。
/// やり直しはしない)。読めない・無い・parse できないは `None`。
async fn before_minutes(
    store: &dyn ObjectStore,
    log: &LogSink,
    tenant_id: Uuid,
    rows: &[KudguriRow],
    prepared: &[PreparedRow],
) -> Vec<Option<OperationMinutes>> {
    // 既に在る運行NO を、出てきた順に重複なしで集める
    let mut wanted: Vec<(String, &str)> = Vec::new();
    for (row, p) in rows.iter().zip(prepared) {
        let unko_no = row.unko_no.as_str();
        if p.exists && !wanted.iter().any(|(_, seen)| *seen == unko_no) {
            wanted.push((format!("{tenant_id}/unko/{unko_no}/KUDGIVT.csv"), unko_no));
        }
    }
    // 結果は運行NO に結び付ける (返ってくる順に依らない)
    let old: HashMap<&str, Option<Vec<u8>>> = get_all(store, wanted).await.into_iter().collect();

    let mut before = Vec::with_capacity(rows.len());
    let mut unavailable = 0usize;
    for (row, p) in rows.iter().zip(prepared) {
        if !p.exists {
            before.push(None);
            continue;
        }
        let bytes = old[row.unko_no.as_str()].as_deref();
        let events = bytes.and_then(|b| parse_kudgivt_for_crew(b, row.crew_role).ok());
        let minutes = events.map(|events| minutes_from_events(&events));
        unavailable += usize::from(minutes.is_none());
        before.push(minutes);
    }
    if unavailable > 0 {
        let message = format!("upload: previous KUDGIVT unavailable for {unavailable} row(s)");
        log(LogLevel::Warn, &message);
    }
    before
}

/// 分割を、丸ごと失敗したら待ってやり直す (回数と待ちは PUT のやり直しと同じ値 = backend と同じ)。
/// 回を使い切ったら `put_failed = 1` を返す (運行NO は分からないので空のまま)。
async fn split_with_retry(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    sleeper: &dyn Sleeper,
    log: &LogSink,
    tenant_id: Uuid,
    upload_id: Uuid,
) -> SplitOutcome {
    for attempt in 1..=PUT_RETRY_ATTEMPTS {
        let e = match split_upload(pg, store, sleeper, log, tenant_id, upload_id).await {
            Ok(outcome) => return outcome,
            Err(e) => e,
        };
        let message = format!("upload: split failed ({attempt}/{PUT_RETRY_ATTEMPTS}): {e}");
        log(LogLevel::Warn, &message);
        if let Some(ms) = PUT_RETRY_DELAYS_MS.get(attempt - 1) {
            sleeper.sleep_ms(*ms).await;
        }
    }
    SplitOutcome {
        put_failed: 1,
        succeeded_unko_nos: Vec::new(),
        failed_unko_nos: Vec::new(),
    }
}
