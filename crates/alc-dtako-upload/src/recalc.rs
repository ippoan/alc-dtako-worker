//! 再計算の流れ (Refs ippoan/rust-alc-api#725)。口は [`crate::routes`] の `POST /recalculate` (月の全員)・
//! `POST /recalculate-driver` (乗務員 1 人)・`POST /recalculate-drivers` (乗務員の一括)・
//! `POST /recalculate-pending` (印の付いた 乗務員 × 月。Refs ippoan/alc-dtako-worker#23)。
//!
//! 日別の労働時間とセグメントを計算し直して保存する。計算と行の組み立ては backend と共有の関数
//! (`alc_csv_parser::kudguri::RecalcOperation`・`alc_compare::upload_daily::{ferry_data_from_text, compute_daily_hours}`)、
//! 保存は取り込みと同じ [`pg::save_daily_hours_with`]。3 口とも保存は**乗務員CD ごとに 1 つの transaction**。
//!
//! - 月の全員: 月の運行を DB から読み、運行ごとの分割の出力 (`{テナント}/unko/{運行NO}/KUDGIVT.csv`・`KUDGFRY.csv`) を
//!   **運行NO ごとに 1 回だけ**読む (同時 [`crate::store::GET_CONCURRENCY`] 本)。運行の行 (2 人乗務なら主と助手の 2 行) は
//!   そのまま計算に渡す。消す対象の運行NO は、その月の再計算の対象の運行 (読んだ運行の行) のもの。今回の計算に日エントリが
//!   出なかった運行の行も消える (3 口とも同じ)。1 人の保存が失敗したら、その人の分は戻り、そこで止まる
//! - 乗務員ごと (1 人・一括): 乗務員の運行を DB から読み、月の全員と同じく運行ごとの分割の出力を読む (KUDGIVT・KUDGFRY)。
//!   乗務員の運行の外にある運行NO の行は拾わない。消す対象の運行NO は月の全員と同じ
//!   (月の運行の一覧を 1 回引いて運行NO だけ使う。一括は 1 回引いて全員で使い回す)。一括は乗務員を 1 人ずつ読んで計算し、1 人の失敗 (KUDGIVT が 1 件も
//!   無い場合を含む) を数えて続ける (その人の transaction は戻る)
//! - 分割の出力を読んで計算する所 (`compute_split_daily`) は 4 口が共有する (保存の形は呼び手ごと)。乗務員 × 月 1 つを計算して
//!   1 transaction で保存する所 ([`recalc_driver_rows`]) は、一括の 1 人ぶんと印の口が共有する
//! - 日別の「要再計算」の印 (取り込みが付ける。[`crate::pg::apply_upload`]): 4 口とも、保存の transaction の中で、計算した
//!   乗務員 × 月 の印を消す ([`pg::save_daily_hours_in_tx`] の `clear`)。消すのは、口が運行を読み始める前の DB の時刻
//!   (印の口は印を読んだ時刻) までに付いた印だけ (計算の間に付いた印は残す)。月の全員は乗務員CD で、ほかは乗務員の id で指す
//! - 印の口 ([`recalc_pending`]): 印を全部読み、月・乗務員の順に 1 つずつ、乗務員 1 人の口と同じ処理 (乗務員の月の運行 →
//!   分割の出力で計算 → 保存。消す対象の運行NO は月の運行のもの = 月ごとに 1 回引く) で計算し直す。失敗 (乗務員が引けない・
//!   KUDGIVT が 1 件も無い・DB) は数えて続け、印を残す。保存先の GET (運行NO の数の 2 倍) の合計が上限を越える手前で止め、
//!   残りは印のまま (`remaining`)。1 つで上限を越えるものは、いつまでも入らないので失敗に数える
//! - 進み具合は event で返す ([`next_recalc_event`]。応答の stream の中で 1 歩ずつ進める)。呼び手が切れたら、そこで止まる
//!   (保存の済んだ乗務員の分は残る。もう一度呼べば揃う)
//!
//! event の `message` とログは固定の語・段の名前・kind・件数だけ (テナント・運行NO・乗務員・入力の値・生のエラー文を出さない)。

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use alc_compare::upload_daily::{compute_daily_hours, ferry_data_from_text, DailyHours, FerryData};
use alc_compare::DayKey;
use alc_csv_parser::kudgivt::{parse_kudgivt, KudgivtRow};
use alc_csv_parser::kudguri::{KudguriRow, RecalcOperation};
use alc_csv_parser::work_segments::EventClass;
use alc_worker_db::PgClient;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use futures_util::lock::Mutex;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::pg::{self, PendingClear, RecalcOperationRow};
use crate::split::{LogLevel, LogSink};
use crate::store::{get_all, ObjectStore};

/// 保存の進み具合を出す間隔 (日エントリの数)。乗務員 1 人を保存し終えるたびに、この数の境を越えたか見る。
pub const RECALC_SAVE_PROGRESS_EVERY: usize = 20;

/// 印の口の 1 回の保存先の GET の既定の上限。Workers Paid の subrequest の上限 (1 リクエスト 10,000) より十分に小さく。
pub const DAILY_RECALC_MAX_GETS: usize = 4000;

/// 年月から月初・月末 (両端を含む) を出す。月が不正なら `None`。
pub fn month_range(year: i32, month: u32) -> Option<(NaiveDate, NaiveDate)> {
    let start = NaiveDate::from_ymd_opt(year, month, 1)?;
    let end = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)?
    } - Duration::days(1);
    Some((start, end))
}

impl From<RecalcOperationRow> for RecalcOperation {
    fn from(row: RecalcOperationRow) -> Self {
        Self {
            unko_no: row.unko_no,
            reading_date: row.reading_date,
            operation_date: row.operation_date,
            driver_cd: row.driver_cd.unwrap_or_default(),
            departure_at: row.departure_at,
            return_at: row.return_at,
            total_distance: row.total_distance,
            drive_time_general: row.drive_time_general,
            drive_time_highway: row.drive_time_highway,
            drive_time_bypass: row.drive_time_bypass,
        }
    }
}

pub(crate) fn kudguri_rows(ops: Vec<RecalcOperationRow>) -> Vec<KudguriRow> {
    let into_row = |op: RecalcOperationRow| RecalcOperation::from(op).into_kudguri_row();
    ops.into_iter().map(into_row).collect()
}

/// 乗務員CD ごとの日エントリ (保存の単位)。
type DriverDays = (String, HashMap<DayKey, DailyHours>);

/// 何を計算し直すか。
#[derive(Clone, Copy)]
enum Target {
    /// 月の全員 (`POST /recalculate`)
    Month,
    /// 乗務員 1 人 (`POST /recalculate-driver`)
    Driver,
    /// 乗務員の一括 (`POST /recalculate-drivers`)
    Drivers,
}

enum Step {
    /// 月の全員: まだ何もしていない
    Start,
    /// 乗務員 1 人: まだ何もしていない
    DriverStart { driver_id: Uuid },
    /// 一括: まだ何もしていない
    BatchStart { ids: Vec<Uuid> },
    /// 月の全員: 運行を読んだ。次は保存先を読んで計算する
    Compute { rows: Vec<KudguriRow> },
    /// 乗務員 1 人: `start` を出した。次は運行と分割の出力を読んで計算する
    DriverLoad { driver_id: Uuid },
    /// 一括: `batch_start` を出した。次は乗務員を引き当てる
    BatchLoad { ids: Vec<Uuid> },
    /// 乗務員ごとに保存している (月の全員・乗務員 1 人。`driver_id` = 乗務員 1 人の id、月の全員は `None` で乗務員CD で印を消す)
    Save {
        groups: std::vec::IntoIter<DriverDays>,
        all_unko_nos: Arc<Vec<String>>,
        driver_id: Option<Uuid>,
        saved: usize,
        entries: usize,
    },
    /// 一括: 乗務員を 1 人ずつ計算して保存している (引き当てられなかった人は `None`。値は月の運行の行)
    Batch {
        drivers: std::vec::IntoIter<(Uuid, Option<Vec<KudguriRow>>)>,
        all_unko_nos: Arc<Vec<String>>,
        current: usize,
        done: usize,
        errors: usize,
    },
    /// `done` (一括は `batch_done`) を出す
    Done { done: usize, errors: usize },
    /// 終わりの event を出し終えた
    Finished,
}

/// 再計算の進み具合 (stream の状態)。
pub struct RecalcRun {
    pg: Arc<Mutex<PgClient>>,
    store: Arc<dyn ObjectStore>,
    log: LogSink,
    tenant_id: Uuid,
    year: i32,
    month: u32,
    target: Target,
    /// 月の全員・乗務員 1 人: 対象の運行の行数 (`done` の total)。一括: 乗務員の数
    total: usize,
    /// 運行を読み始める前の DB の時刻 (これまでに付いた印だけを消す。読む前は最小の時刻 = 何も消さない)
    before: DateTime<Utc>,
    step: Step,
}

impl RecalcRun {
    /// 月の全員。
    pub fn new(
        pg: Arc<Mutex<PgClient>>,
        store: Arc<dyn ObjectStore>,
        log: LogSink,
        tenant_id: Uuid,
        year: i32,
        month: u32,
    ) -> Self {
        Self {
            pg,
            store,
            log,
            tenant_id,
            year,
            month,
            target: Target::Month,
            total: 0,
            before: DateTime::<Utc>::MIN_UTC,
            step: Step::Start,
        }
    }

    /// 乗務員 1 人。
    pub fn driver(self, driver_id: Uuid) -> Self {
        Self {
            target: Target::Driver,
            step: Step::DriverStart { driver_id },
            ..self
        }
    }

    /// 乗務員の一括。
    pub fn drivers(self, driver_ids: Vec<Uuid>) -> Self {
        Self {
            target: Target::Drivers,
            step: Step::BatchStart { ids: driver_ids },
            ..self
        }
    }

    /// ログの頭 (口の名前)。
    fn name(&self) -> &'static str {
        match self.target {
            Target::Month => "recalculate",
            Target::Driver => "recalculate-driver",
            Target::Drivers => "recalculate-drivers",
        }
    }

    /// `error` を出して終わる。`stage` はログの段の名前 (固定の語。無ければログを出さない)。
    fn fail(mut self, message: &str, stage: Option<String>) -> Option<(Value, Self)> {
        if let Some(stage) = stage {
            (self.log)(LogLevel::Error, &format!("{} failed: {stage}", self.name()));
        }
        self.step = Step::Finished;
        Some((json!({ "event": "error", "message": message }), self))
    }

    fn warn(&self, what: &str) {
        (self.log)(LogLevel::Warn, &format!("{}: {what}", self.name()));
    }

    /// 運行の行を、分割の出力で計算する ([`compute_split_daily`])。
    async fn split_daily(&self, rows: &[KudguriRow]) -> Result<SplitDaily, tokio_postgres::Error> {
        let (store, name) = (self.store.as_ref(), self.name());
        compute_split_daily(&self.pg, store, &self.log, name, self.tenant_id, rows).await
    }

    /// 印の消し方 (月 = 口の月。乗務員は `driver_id` か乗務員CD で指す)。
    fn clear(&self, driver_id: Option<Uuid>, driver_cd: Option<String>) -> PendingClear {
        let (month, _) = month_range(self.year, self.month).expect("a checked month");
        PendingClear {
            month,
            driver_id,
            driver_cd,
            before: self.before,
        }
    }
}

fn db_stage(e: &tokio_postgres::Error) -> Option<String> {
    Some(format!("db ({})", alc_worker_db::kind(e)))
}

/// 次の event を作る (1 歩ずつ進める。event の無い歩きは、ここで続けて進む)。
///
/// - 月の全員: `progress{current: 0, total: 運行の行数, step: "start"}` → (保存しながら) `progress{current: 保存した日エントリの数,
///   total: 日エントリの数, step: "save"}` (20 件の境を越えるたびと最後) → `done{total, success: total, failed: 0}`
/// - 乗務員 1 人: `progress{current: 0, total: 0, step: "start"}` → `progress{…, step: "save"}` (上と同じ出し方) → `done{total: 運行の行数}`
/// - 一括: `batch_start{total_drivers}` → `progress{current: 終えた乗務員の数, total: 乗務員の数}` (1 人ごと) →
///   `batch_done{total, done, errors}`
///
/// 失敗は `error{message}` で終わる (`month_invalid`・`kudgivt_not_found`・`driver_not_found`・`internal_error`)。
pub async fn next_recalc_event(mut run: RecalcRun) -> Option<(Value, RecalcRun)> {
    loop {
        match std::mem::replace(&mut run.step, Step::Finished) {
            Step::Finished => return None,
            Step::Start => return month_start(run).await,
            Step::DriverStart { driver_id } => {
                run.step = Step::DriverLoad { driver_id };
                let event =
                    json!({ "event": "progress", "current": 0, "total": 0, "step": "start" });
                return Some((event, run));
            }
            Step::BatchStart { ids } => {
                run.total = ids.len();
                run.step = Step::BatchLoad { ids };
                let event = json!({ "event": "batch_start", "total_drivers": run.total });
                return Some((event, run));
            }
            Step::Compute { rows } => match run.split_daily(&rows).await {
                Ok(SplitDaily::Daily(daily)) => {
                    run.step = save_step(daily, unko_nos_of(&rows), None)
                }
                Ok(SplitDaily::KudgivtNotFound) => return run.fail("kudgivt_not_found", None),
                Err(e) => return run.fail("internal_error", db_stage(&e)),
            },
            Step::DriverLoad { driver_id } => {
                let Some((month_start, month_end)) = month_range(run.year, run.month) else {
                    return run.fail("month_invalid", None);
                };
                let fetch_end = month_end + Duration::days(1);
                let driver = load_driver(&run, driver_id, month_start, fetch_end).await;
                let (before, ops, all_unko_nos) = match driver {
                    Ok(Some(loaded)) => loaded,
                    Ok(None) => return run.fail("driver_not_found", None),
                    Err(e) => return run.fail("internal_error", db_stage(&e)),
                };
                let rows = kudguri_rows(ops);
                (run.total, run.before) = (rows.len(), before);
                match run.split_daily(&rows).await {
                    Ok(SplitDaily::Daily(daily)) => {
                        run.step = save_step(daily, all_unko_nos, Some(driver_id))
                    }
                    Ok(SplitDaily::KudgivtNotFound) => return run.fail("kudgivt_not_found", None),
                    Err(e) => return run.fail("internal_error", db_stage(&e)),
                }
            }
            Step::BatchLoad { ids } => {
                let Some((month_start, month_end)) = month_range(run.year, run.month) else {
                    return run.fail("month_invalid", None);
                };
                let fetch_end = month_end + Duration::days(1);
                // 時刻は乗務員の運行を読む前に引く (失敗は月の運行の一覧の失敗と一緒に見る)
                let before = {
                    let mut client = run.pg.lock().await;
                    pg::db_now(&mut client, run.tenant_id).await
                };
                let drivers = resolve_drivers(&run, &ids, month_start, month_end).await;
                let all_unko_nos = async {
                    let before = before?;
                    let mut client = run.pg.lock().await;
                    let tenant_id = run.tenant_id;
                    let unko_nos = month_unko_nos(&mut client, tenant_id, month_start, fetch_end);
                    Ok((before, unko_nos.await?))
                };
                let all_unko_nos = match all_unko_nos.await {
                    Ok((before, unko_nos)) => {
                        run.before = before;
                        unko_nos
                    }
                    Err(e) => return run.fail("internal_error", db_stage(&e)),
                };
                let drivers: Vec<(Uuid, Option<Vec<KudguriRow>>)> =
                    ids.into_iter().zip(drivers).collect();
                run.step = Step::Batch {
                    drivers: drivers.into_iter(),
                    all_unko_nos: Arc::new(all_unko_nos),
                    current: 0,
                    done: 0,
                    errors: 0,
                };
            }
            Step::Save {
                mut groups,
                all_unko_nos,
                driver_id,
                saved,
                entries,
            } => {
                let Some((driver_cd, days)) = groups.next() else {
                    run.step = Step::Done { done: 0, errors: 0 };
                    continue;
                };
                let count = days.len();
                // 乗務員 1 人は id で、月の全員は乗務員CD で、その 乗務員 × 月 の印を消す
                let clear = match driver_id {
                    Some(id) => run.clear(Some(id), None),
                    None => run.clear(None, Some(driver_cd)),
                };
                let result = {
                    let mut client = run.pg.lock().await;
                    let (tenant_id, unko_nos) = (run.tenant_id, all_unko_nos.clone());
                    let save = pg::save_daily_hours_in_tx(
                        &mut client,
                        tenant_id,
                        days,
                        unko_nos,
                        Some(clear),
                    );
                    save.await
                };
                if let Err(e) = result {
                    return run.fail("internal_error", db_stage(&e));
                }
                let now = saved + count;
                run.step = Step::Save {
                    groups,
                    all_unko_nos,
                    driver_id,
                    saved: now,
                    entries,
                };
                let crossed = now / RECALC_SAVE_PROGRESS_EVERY > saved / RECALC_SAVE_PROGRESS_EVERY;
                if crossed || now == entries {
                    let event = json!({ "event": "progress", "current": now, "total": entries, "step": "save" });
                    return Some((event, run));
                }
            }
            Step::Batch {
                mut drivers,
                all_unko_nos,
                current,
                mut done,
                mut errors,
            } => {
                let Some((driver_id, driver)) = drivers.next() else {
                    run.step = Step::Done { done, errors };
                    continue;
                };
                let ok = match driver {
                    Some(rows) => recalc_batch_driver(&run, driver_id, rows, &all_unko_nos).await,
                    None => false,
                };
                if ok {
                    done += 1;
                } else {
                    errors += 1;
                }
                let current = current + 1;
                run.step = Step::Batch {
                    drivers,
                    all_unko_nos,
                    current,
                    done,
                    errors,
                };
                let event = json!({ "event": "progress", "current": current, "total": run.total });
                return Some((event, run));
            }
            Step::Done { done, errors } => {
                let total = run.total;
                let event = match run.target {
                    Target::Month => {
                        json!({ "event": "done", "total": total, "success": total, "failed": 0 })
                    }
                    Target::Driver => json!({ "event": "done", "total": total }),
                    Target::Drivers => {
                        json!({ "event": "batch_done", "total": total, "done": done, "errors": errors })
                    }
                };
                return Some((event, run));
            }
        }
    }
}

/// 月の全員の最初の歩き: 運行を読んで `start` を出す。
async fn month_start(mut run: RecalcRun) -> Option<(Value, RecalcRun)> {
    let Some((month_start, month_end)) = month_range(run.year, run.month) else {
        return run.fail("month_invalid", None);
    };
    let fetch_end = month_end + Duration::days(1);
    let ops = async {
        let mut client = run.pg.lock().await;
        let tenant_id = run.tenant_id;
        let before = pg::db_now(&mut client, tenant_id).await?;
        let ops = pg::operations_for_recalc(&mut client, tenant_id, month_start, fetch_end);
        Ok((before, ops.await?))
    };
    let rows = match ops.await {
        Ok((before, ops)) => {
            run.before = before;
            kudguri_rows(ops)
        }
        Err(e) => return run.fail("internal_error", db_stage(&e)),
    };
    run.total = rows.len();
    run.step = Step::Compute { rows };
    let event = json!({ "event": "progress", "current": 0, "total": run.total, "step": "start" });
    Some((event, run))
}

/// 運行の行の運行NO を、重複なしで行の順に並べる (保存で消す対象)。
pub(crate) fn unko_nos_of(rows: &[KudguriRow]) -> Vec<String> {
    let mut unko_nos: Vec<String> = Vec::new();
    for row in rows {
        if !unko_nos.contains(&row.unko_no) {
            unko_nos.push(row.unko_no.clone());
        }
    }
    unko_nos
}

/// その月の再計算の対象の運行 ([`pg::operations_for_recalc`]) の運行NO (乗務員の口が、月の全員の口と同じ行を消すため)。
pub(crate) async fn month_unko_nos(
    client: &mut PgClient,
    tenant_id: Uuid,
    month_start: NaiveDate,
    fetch_end: NaiveDate,
) -> Result<Vec<String>, tokio_postgres::Error> {
    let ops = pg::operations_for_recalc(client, tenant_id, month_start, fetch_end).await?;
    let mut unko_nos: Vec<String> = Vec::new();
    for op in ops {
        if !unko_nos.contains(&op.unko_no) {
            unko_nos.push(op.unko_no);
        }
    }
    Ok(unko_nos)
}

/// 乗務員 1 人の運行の読み込みの結果 (読み始める前の DB の時刻・運行・消す対象の運行NO)。
type LoadedDriver = (DateTime<Utc>, Vec<RecalcOperationRow>, Vec<String>);

/// 乗務員 1 人の、読み始める前の DB の時刻と運行と、消す対象の運行NO (月の運行のもの) を読む。乗務員が居なければ `None`。
async fn load_driver(
    run: &RecalcRun,
    driver_id: Uuid,
    month_start: NaiveDate,
    fetch_end: NaiveDate,
) -> Result<Option<LoadedDriver>, tokio_postgres::Error> {
    let tenant_id = run.tenant_id;
    let mut client = run.pg.lock().await;
    let before = pg::db_now(&mut client, tenant_id).await?;
    let driver =
        pg::driver_operations_for_recalc(&mut client, tenant_id, driver_id, month_start, fetch_end)
            .await?;
    let Some((_, ops)) = driver else {
        return Ok(None);
    };
    let all_unko_nos = month_unko_nos(&mut client, tenant_id, month_start, fetch_end).await?;
    Ok(Some((before, ops, all_unko_nos)))
}

/// 計算の出力を乗務員CD ごとに分けた保存の段 (`all_unko_nos` = 消す対象の運行NO。月の運行のもの。
/// `driver_id` = 乗務員 1 人の id、月の全員は `None`)。
fn save_step(
    daily: HashMap<DayKey, DailyHours>,
    all_unko_nos: Vec<String>,
    driver_id: Option<Uuid>,
) -> Step {
    let all_unko_nos = Arc::new(all_unko_nos);
    let entries = daily.len();
    let mut by_driver: BTreeMap<String, HashMap<DayKey, DailyHours>> = BTreeMap::new();
    for (key, hours) in daily {
        by_driver
            .entry(key.0.clone())
            .or_default()
            .insert(key, hours);
    }
    let groups: Vec<DriverDays> = by_driver.into_iter().collect();
    Step::Save {
        groups: groups.into_iter(),
        all_unko_nos,
        driver_id,
        saved: 0,
        entries,
    }
}

/// [`compute_split_daily`] の結果。
pub(crate) enum SplitDaily {
    /// 行が在るのに、KUDGIVT が 1 件も読めない
    KudgivtNotFound,
    /// 計算した日別 (乗務員CD ごとに分けていない)
    Daily(HashMap<DayKey, DailyHours>),
}

/// 運行の行を、運行ごとの分割の出力 (KUDGIVT・KUDGFRY) で計算する (4 口が共有する)。
/// 分割の出力を読み、KUDGIVT が 1 件も無ければ (行が在るとき) [`SplitDaily::KudgivtNotFound`]、在れば分類を読んで
/// (未登録の event は既定の分類で登録する) `compute_daily_hours` を通す。保存はしない (保存の形は呼び手ごと)。
/// `name` はログの頭 (口の名前)。
pub(crate) async fn compute_split_daily(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    log: &LogSink,
    name: &str,
    tenant_id: Uuid,
    rows: &[KudguriRow],
) -> Result<SplitDaily, tokio_postgres::Error> {
    let (kudgivt_rows, ferry) = read_split_outputs(store, log, name, tenant_id, rows).await;
    if kudgivt_rows.is_empty() && !rows.is_empty() {
        return Ok(SplitDaily::KudgivtNotFound);
    }
    let samples = Arc::new(kudgivt_rows.clone());
    let classifications: HashMap<String, EventClass> = {
        let mut client = pg.lock().await;
        pg::prepare_upload(&mut client, tenant_id, Arc::new(Vec::new()), samples)
            .await?
            .classification_map()
    };
    let daily = compute_daily_hours(rows, &kudgivt_rows, &classifications, &ferry);
    Ok(SplitDaily::Daily(daily))
}

/// 一括の乗務員を 1 人ずつ引き当てる (乗務員CD と月の運行。無い・引けないは `None`)。
async fn resolve_drivers(
    run: &RecalcRun,
    ids: &[Uuid],
    month_start: NaiveDate,
    month_end: NaiveDate,
) -> Vec<Option<Vec<KudguriRow>>> {
    let fetch_end = month_end + Duration::days(1);
    let mut drivers = Vec::with_capacity(ids.len());
    for &driver_id in ids {
        let loaded = {
            let mut client = run.pg.lock().await;
            let tenant_id = run.tenant_id;
            pg::driver_operations_for_recalc(
                &mut client,
                tenant_id,
                driver_id,
                month_start,
                fetch_end,
            )
            .await
        };
        drivers.push(match loaded {
            Ok(Some((_, ops))) => Some(kudguri_rows(ops)),
            Ok(None) => {
                run.warn("driver not found");
                None
            }
            Err(e) => {
                run.warn(&format!("driver failed: db ({})", alc_worker_db::kind(&e)));
                None
            }
        });
    }
    drivers
}

/// [`recalc_driver_rows`] の失敗。
pub(crate) enum DriverRecalcError {
    /// 行が在るのに、KUDGIVT が 1 件も読めない
    KudgivtNotFound,
    Db(tokio_postgres::Error),
}

/// 乗務員 × 月 1 つを計算し直して保存する (一括の 1 人ぶんと印の口が共有する): 運行の行を分割の出力で計算し
/// ([`compute_split_daily`])、1 transaction で保存して、その transaction の中で `clear` の印を消す
/// ([`pg::save_daily_hours_in_tx`])。失敗したら日別も印も元のまま。
#[allow(clippy::too_many_arguments)]
pub(crate) async fn recalc_driver_rows(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    log: &LogSink,
    name: &str,
    tenant_id: Uuid,
    rows: &[KudguriRow],
    all_unko_nos: Arc<Vec<String>>,
    clear: PendingClear,
) -> Result<(), DriverRecalcError> {
    let computed = compute_split_daily(pg, store, log, name, tenant_id, rows).await;
    let SplitDaily::Daily(daily) = computed.map_err(DriverRecalcError::Db)? else {
        return Err(DriverRecalcError::KudgivtNotFound);
    };
    let mut client = pg.lock().await;
    let saved =
        pg::save_daily_hours_in_tx(&mut client, tenant_id, daily, all_unko_nos, Some(clear));
    saved.await.map_err(DriverRecalcError::Db)
}

/// 一括の乗務員 1 人 ([`recalc_driver_rows`])。成功なら `true`。KUDGIVT が 1 件も無ければ、その人だけ失敗に数える。
async fn recalc_batch_driver(
    run: &RecalcRun,
    driver_id: Uuid,
    rows: Vec<KudguriRow>,
    all_unko_nos: &Arc<Vec<String>>,
) -> bool {
    let (store, name, tenant_id) = (run.store.as_ref(), run.name(), run.tenant_id);
    let clear = run.clear(Some(driver_id), None);
    let unko_nos = all_unko_nos.clone();
    let recalc = recalc_driver_rows(
        &run.pg, store, &run.log, name, tenant_id, &rows, unko_nos, clear,
    );
    match recalc.await {
        Ok(()) => true,
        Err(DriverRecalcError::KudgivtNotFound) => {
            run.warn("driver failed: kudgivt_not_found");
            false
        }
        Err(DriverRecalcError::Db(e)) => {
            run.warn(&format!("driver failed: db ({})", alc_worker_db::kind(&e)));
            false
        }
    }
}

/// [`recalc_pending`] の結果 (件数だけ)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingOutcome {
    /// 計算し直して印を消した 乗務員 × 月 の数
    pub processed: usize,
    /// 失敗した数 (印は残る)
    pub failed: usize,
    /// GET の上限で手を付けなかった数 (印は残る。失敗は含めない)
    pub remaining: usize,
}

/// 月の運行の運行NO (日別の保存で消す対象。月ごとに 1 回引いて乗務員で使い回す)。
type MonthUnkoNos = Arc<Vec<String>>;

/// 印の口の本体 (`POST /recalculate-pending`。段は module の doc)。印を読めなければ `Err`。
pub async fn recalc_pending(
    pg: &Mutex<PgClient>,
    store: &dyn ObjectStore,
    log: &LogSink,
    tenant_id: Uuid,
    max_gets: usize,
) -> Result<PendingOutcome, tokio_postgres::Error> {
    const NAME: &str = "recalculate-pending";
    let marks = {
        let mut client = pg.lock().await;
        pg::recalc_pending_marks(&mut client, tenant_id).await?
    };
    let mut outcome = PendingOutcome {
        processed: 0,
        failed: 0,
        remaining: 0,
    };
    let mut gets = 0usize;
    // いま見ている月と、その月の運行の運行NO (引けなければ `None`)
    let mut month: Option<(NaiveDate, Option<MonthUnkoNos>)> = None;
    for mark in marks {
        if outcome.remaining > 0 {
            outcome.remaining += 1;
            continue;
        }
        let (start, end) = month_range(mark.month.year(), mark.month.month()).expect("a month");
        let fetch_end = end + Duration::days(1);
        if month.as_ref().map(|(key, _)| *key) != Some(start) {
            let unko_nos = {
                let mut client = pg.lock().await;
                month_unko_nos(&mut client, tenant_id, start, fetch_end).await
            };
            month = Some((start, unko_nos.ok().map(Arc::new)));
        }
        let all_unko_nos = month.as_ref().and_then(|(_, nos)| nos.clone());
        let loaded = {
            let mut client = pg.lock().await;
            let driver_id = mark.driver_id;
            pg::driver_operations_for_recalc(&mut client, tenant_id, driver_id, start, fetch_end)
                .await
        };
        let (Some(all_unko_nos), Ok(Some((_, ops)))) = (all_unko_nos, loaded) else {
            outcome.failed += 1;
            continue;
        };
        let rows = kudguri_rows(ops);
        let need = 2 * unko_nos_of(&rows).len();
        if need > max_gets {
            outcome.failed += 1;
            continue;
        }
        if need > max_gets - gets {
            outcome.remaining += 1;
            continue;
        }
        gets += need;
        let clear = PendingClear {
            month: start,
            driver_id: Some(mark.driver_id),
            driver_cd: None,
            before: mark.read_at,
        };
        let recalc =
            recalc_driver_rows(pg, store, log, NAME, tenant_id, &rows, all_unko_nos, clear);
        match recalc.await {
            Ok(()) => outcome.processed += 1,
            Err(_) => outcome.failed += 1,
        }
    }
    if outcome.failed > 0 || outcome.remaining > 0 {
        let PendingOutcome {
            processed,
            failed,
            remaining,
        } = outcome;
        let message =
            format!("{NAME}: processed {processed}, failed {failed}, remaining {remaining}");
        log(LogLevel::Warn, &message);
    }
    Ok(outcome)
}

/// 運行NO ごとに 1 回、分割の出力の KUDGIVT と KUDGFRY を読む (同時に。GET は運行NO の数の 2 倍)。
/// 読めない・無い・parse できないものは飛ばす (読めない KUDGIVT は件数を `name` の頭で Warn)。
async fn read_split_outputs(
    store: &dyn ObjectStore,
    log: &LogSink,
    name: &str,
    tenant_id: Uuid,
    rows: &[KudguriRow],
) -> (Vec<KudgivtRow>, HashMap<String, FerryData>) {
    let mut unko_nos: Vec<&str> = Vec::new();
    for row in rows {
        if !unko_nos.contains(&row.unko_no.as_str()) {
            unko_nos.push(&row.unko_no);
        }
    }
    let mut wanted: Vec<(String, (usize, bool))> = Vec::new();
    for (index, unko_no) in unko_nos.iter().enumerate() {
        wanted.push((
            format!("{tenant_id}/unko/{unko_no}/KUDGIVT.csv"),
            (index, true),
        ));
        wanted.push((
            format!("{tenant_id}/unko/{unko_no}/KUDGFRY.csv"),
            (index, false),
        ));
    }
    let mut got = get_all(store, wanted).await;
    // 返ってくる順に依らないよう、運行NO の順 (KUDGIVT が先) に並べ直す
    got.sort_by_key(|((index, is_kudgivt), _)| (*index, !*is_kudgivt));

    let mut kudgivt_rows: Vec<KudgivtRow> = Vec::new();
    let mut ferry: HashMap<String, FerryData> = HashMap::new();
    let mut unreadable = 0usize;
    for ((index, is_kudgivt), bytes) in got {
        let unko_no = unko_nos[index];
        if is_kudgivt {
            let parsed = bytes.map(|b| parse_kudgivt(&String::from_utf8_lossy(&b)));
            match parsed {
                Some(Ok(events)) => kudgivt_rows.extend(events),
                _ => unreadable += 1,
            }
        } else if let Some(data) = bytes
            .map(|b| alc_csv_parser::decode_shift_jis(&b))
            .and_then(|text| ferry_data_from_text(&text))
        {
            ferry.insert(unko_no.to_owned(), data);
        }
    }
    if unreadable > 0 {
        let message = format!("{name}: KUDGIVT unavailable for {unreadable} operation(s)");
        log(LogLevel::Warn, &message);
    }
    (kudgivt_rows, ferry)
}
