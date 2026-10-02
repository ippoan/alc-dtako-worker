//! 月の全員の再計算の流れ (Refs ippoan/rust-alc-api#725)。口は [`crate::routes`] の `POST /recalculate`。
//!
//! 月の運行を DB から読み、運行ごとに分割の出力 (`{テナント}/unko/{運行NO}/KUDGIVT.csv`・`KUDGFRY.csv`) を保存先から読み、
//! 日別の労働時間とセグメントを計算し直して保存する。計算と行の組み立ては backend と共有の関数
//! (`alc_csv_parser::kudguri::RecalcOperation`・`alc_compare::upload_daily::{ferry_data_from_text, compute_daily_hours}`)、
//! 保存は取り込みと同じ [`pg::save_daily_hours_with`]。
//!
//! - 保存先の KUDGIVT・KUDGFRY は**運行NO ごとに 1 回だけ**読む (同時 [`crate::store::GET_CONCURRENCY`] 本)。
//!   運行の行 (2 人乗務なら主と助手の 2 行) はそのまま計算に渡す
//! - 保存は**乗務員CD ごとに 1 つの transaction**。消す対象の運行NO は月の全体のもの ([`pg::daily_unko_nos`]) を渡すので、
//!   まとめて保存したときと同じ行が消える。1 人の保存が失敗したら、その人の分は戻り、そこで止まる
//! - 進み具合は event で返す ([`next_recalc_event`]。応答の stream の中で 1 歩ずつ進める)。呼び手が切れたら、そこで止まる
//!   (保存の済んだ乗務員の分は残る。もう一度呼べば揃う)
//!
//! event の `message` とログは固定の語・段の名前・kind・件数だけ (テナント・運行NO・入力の値・生のエラー文を出さない)。

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use alc_compare::upload_daily::{compute_daily_hours, ferry_data_from_text, DailyHours, FerryData};
use alc_compare::DayKey;
use alc_csv_parser::kudgivt::{parse_kudgivt, KudgivtRow};
use alc_csv_parser::kudguri::{KudguriRow, RecalcOperation};
use alc_worker_db::PgClient;
use chrono::{Duration, NaiveDate};
use futures_util::lock::Mutex;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::pg::{self, RecalcOperationRow};
use crate::split::{LogLevel, LogSink};
use crate::store::{get_all, ObjectStore};

/// 保存の進み具合を出す間隔 (日エントリの数)。
pub const RECALC_SAVE_PROGRESS_EVERY: usize = 20;

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

/// 乗務員CD ごとの日エントリ (保存の単位)。
type DriverDays = (String, HashMap<DayKey, DailyHours>);

enum Step {
    /// 運行をまだ読んでいない
    Start,
    /// 運行を読んだ。次は保存先を読んで計算する
    Compute { rows: Vec<KudguriRow> },
    /// 乗務員ごとに保存している
    Save {
        groups: std::vec::IntoIter<DriverDays>,
        all_unko_nos: Arc<Vec<String>>,
        saved: usize,
        entries: usize,
    },
    /// `done` を出す
    Done,
    /// `done` か `error` を出し終えた
    Finished,
}

/// 月の再計算の進み具合 (stream の状態)。
pub struct RecalcRun {
    pg: Arc<Mutex<PgClient>>,
    store: Arc<dyn ObjectStore>,
    log: LogSink,
    tenant_id: Uuid,
    year: i32,
    month: u32,
    /// 対象の運行の行数 (`progress` の start と `done` の total)
    total: usize,
    step: Step,
}

impl RecalcRun {
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
            total: 0,
            step: Step::Start,
        }
    }

    /// `error` を出して終わる。`stage` はログの段の名前 (固定の語。無ければログを出さない)。
    fn fail(mut self, message: &str, stage: Option<String>) -> Option<(Value, Self)> {
        if let Some(stage) = stage {
            (self.log)(LogLevel::Error, &format!("recalculate failed: {stage}"));
        }
        self.step = Step::Finished;
        Some((json!({ "event": "error", "message": message }), self))
    }
}

fn db_stage(e: &tokio_postgres::Error) -> Option<String> {
    Some(format!("db ({})", alc_worker_db::kind(e)))
}

/// 次の event を作る (1 歩ずつ進める。event の無い歩きは、ここで続けて進む)。
///
/// 並び: `progress{current: 0, total: 運行の行数, step: "start"}` → (保存しながら) `progress{current: 保存した日エントリの数,
/// total: 日エントリの数, step: "save"}` (20 件を越えるたびと最後) → `done{total, success: total, failed: 0}`。
/// 失敗は `error{message}` で終わる (`month_invalid`・`kudgivt_not_found`・`internal_error`)。
pub async fn next_recalc_event(mut run: RecalcRun) -> Option<(Value, RecalcRun)> {
    loop {
        match std::mem::replace(&mut run.step, Step::Finished) {
            Step::Finished => return None,
            Step::Start => {
                let Some((month_start, month_end)) = month_range(run.year, run.month) else {
                    return run.fail("month_invalid", None);
                };
                let fetch_end = month_end + Duration::days(1);
                let ops = {
                    let mut client = run.pg.lock().await;
                    pg::operations_for_recalc(&mut client, run.tenant_id, month_start, fetch_end)
                        .await
                };
                let ops = match ops {
                    Ok(ops) => ops,
                    Err(e) => return run.fail("internal_error", db_stage(&e)),
                };
                let into_row =
                    |op: RecalcOperationRow| RecalcOperation::from(op).into_kudguri_row();
                let rows: Vec<KudguriRow> = ops.into_iter().map(into_row).collect();
                run.total = rows.len();
                run.step = Step::Compute { rows };
                let event = json!({ "event": "progress", "current": 0, "total": run.total, "step": "start" });
                return Some((event, run));
            }
            Step::Compute { rows } => {
                let (kudgivt_rows, ferry) = read_split_outputs(&run, &rows).await;
                if kudgivt_rows.is_empty() && !rows.is_empty() {
                    return run.fail("kudgivt_not_found", None);
                }
                let prepared = {
                    let mut client = run.pg.lock().await;
                    let kudgivt = Arc::new(kudgivt_rows.clone());
                    pg::prepare_upload(&mut client, run.tenant_id, Arc::new(Vec::new()), kudgivt)
                        .await
                };
                let classifications = match prepared {
                    Ok(prepared) => prepared.classification_map(),
                    Err(e) => return run.fail("internal_error", db_stage(&e)),
                };
                let daily = compute_daily_hours(&rows, &kudgivt_rows, &classifications, &ferry);
                let all_unko_nos = Arc::new(pg::daily_unko_nos(&daily));
                let entries = daily.len();
                let mut by_driver: BTreeMap<String, HashMap<DayKey, DailyHours>> = BTreeMap::new();
                for (key, hours) in daily {
                    by_driver
                        .entry(key.0.clone())
                        .or_default()
                        .insert(key, hours);
                }
                let groups: Vec<DriverDays> = by_driver.into_iter().collect();
                run.step = Step::Save {
                    groups: groups.into_iter(),
                    all_unko_nos,
                    saved: 0,
                    entries,
                };
            }
            Step::Save {
                mut groups,
                all_unko_nos,
                saved,
                entries,
            } => {
                let Some((_, days)) = groups.next() else {
                    run.step = Step::Done;
                    continue;
                };
                let count = days.len();
                let result = {
                    let mut client = run.pg.lock().await;
                    let unko_nos = all_unko_nos.clone();
                    pg::save_daily_hours_in_tx(&mut client, run.tenant_id, days, unko_nos).await
                };
                if let Err(e) = result {
                    return run.fail("internal_error", db_stage(&e));
                }
                let now = saved + count;
                run.step = Step::Save {
                    groups,
                    all_unko_nos,
                    saved: now,
                    entries,
                };
                let crossed = now / RECALC_SAVE_PROGRESS_EVERY > saved / RECALC_SAVE_PROGRESS_EVERY;
                if crossed || now == entries {
                    let event = json!({ "event": "progress", "current": now, "total": entries, "step": "save" });
                    return Some((event, run));
                }
            }
            Step::Done => {
                let total = run.total;
                let event =
                    json!({ "event": "done", "total": total, "success": total, "failed": 0 });
                return Some((event, run));
            }
        }
    }
}

/// 運行NO ごとに 1 回、分割の出力の KUDGIVT と KUDGFRY を読む (同時に)。読めない・無い・parse できないものは飛ばす。
async fn read_split_outputs(
    run: &RecalcRun,
    rows: &[KudguriRow],
) -> (Vec<KudgivtRow>, HashMap<String, FerryData>) {
    let mut unko_nos: Vec<&str> = Vec::new();
    for row in rows {
        if !unko_nos.contains(&row.unko_no.as_str()) {
            unko_nos.push(&row.unko_no);
        }
    }
    let tenant_id = run.tenant_id;
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
    let mut got = get_all(run.store.as_ref(), wanted).await;
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
        let message = format!("recalculate: KUDGIVT unavailable for {unreadable} operation(s)");
        (run.log)(LogLevel::Warn, &message);
    }
    (kudgivt_rows, ferry)
}
