//! 拘束時間の帳票の組み立て (純粋な部分。W5c-3)。
//!
//! rust-alc-api の `alc_dtako::dtako_restraint_report::build_report_core` を写したもの。違いは、DB を読む段を外へ出したことだけ
//! ([`MonthRange`] で読む範囲を決め、呼び手が [`ReportInputs`] を読んで [`build_report`] に渡す)。
//! 日の行・週の小計・月の合計の規則、時刻の書き方 (`DateTime<Utc>` の時・分をそのまま = 取り込みは JST の壁時計を UTC として保存している)
//! は rust と同じ。

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc, Weekday};
use uuid::Uuid;

use super::models::{
    DailyWorkHoursRow, MonthlyTotal, OpTimesRow, OperationDetail, ReportInputs, RestraintDayRow,
    RestraintReportResponse, SegmentRow, WeeklySubtotal,
};

/// 最大拘束時間: デフォルト275時間（分換算16500）
pub const MAX_RESTRAINT_MINUTES: i32 = 275 * 60;

/// 帳票 1 か月ぶんの読む範囲。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonthRange {
    pub month_start: NaiveDate,
    pub month_end: NaiveDate,
    /// 前運転平均の 1 日目に使う前日
    pub prev_day: NaiveDate,
    /// 年度の累計を読む範囲 (年度の始め 〜 前月末)。4 月は前月末が年度の始めより前なので無い
    pub fiscal: Option<(NaiveDate, NaiveDate)>,
}

impl MonthRange {
    /// 年月が日付にならなければ `None` (口は 400 `invalid year/month`)。
    pub fn new(year: i32, month: u32) -> Option<Self> {
        let month_start = NaiveDate::from_ymd_opt(year, month, 1)?;
        let next_month = if month == 12 {
            NaiveDate::from_ymd_opt(year + 1, 1, 1)
        } else {
            NaiveDate::from_ymd_opt(year, month + 1, 1)
        }?;
        let month_end = next_month - Duration::days(1);
        let prev_day = month_start - Duration::days(1);
        // Fiscal year cumulative (April to previous month)
        let fiscal_year_start = if month >= 4 {
            NaiveDate::from_ymd_opt(year, 4, 1)?
        } else {
            NaiveDate::from_ymd_opt(year - 1, 4, 1)?
        };
        let prev_month_end = month_start - Duration::days(1);
        let fiscal =
            (fiscal_year_start <= prev_month_end).then_some((fiscal_year_start, prev_month_end));
        Some(Self {
            month_start,
            month_end,
            prev_day,
            fiscal,
        })
    }
}

fn fmt_trunc(dt: &DateTime<Utc>) -> String {
    format!("{}:{:02}", dt.hour(), dt.minute())
}

/// 1 か月ぶんの帳票を組み立てる (rust の `build_report_core` の DB を読んだ後の部分)。
pub fn build_report(
    driver_id: Uuid,
    driver_name: &str,
    year: i32,
    month: u32,
    range: MonthRange,
    inputs: &ReportInputs,
) -> RestraintReportResponse {
    let MonthRange {
        month_start,
        month_end,
        ..
    } = range;

    // 同日に複数行（始業時刻が異なる運行）がある場合のためVecで保持
    let mut dwh_map: HashMap<NaiveDate, Vec<&DailyWorkHoursRow>> = HashMap::new();
    for r in &inputs.daily_work_hours {
        dwh_map.entry(r.work_date).or_default().push(r);
    }

    let op_times_map: HashMap<NaiveDate, &OpTimesRow> = inputs
        .operation_times
        .iter()
        .map(|r| (r.operation_date, r))
        .collect();

    // Group segments by date, then by unko_no
    let mut day_groups: BTreeMap<NaiveDate, Vec<&SegmentRow>> = BTreeMap::new();
    for seg in &inputs.segments {
        day_groups.entry(seg.work_date).or_default().push(seg);
    }

    // Build day rows (pass 1)
    let mut days = Vec::new();
    let mut cumulative = 0i32;
    let mut prev_main_drive: Option<i32> = inputs.prev_day_drive;

    // Weekly tracking
    let mut weekly_subtotals = Vec::new();
    let mut week_drive = 0i32;
    let mut week_cargo = 0i32;
    let mut week_break = 0i32;
    let mut week_restraint = 0i32;
    let mut current_week_end: Option<NaiveDate> = None;

    let mut current_date = month_start;
    while current_date <= month_end {
        // Check if this is a week boundary (Sunday)
        if current_date.weekday() == Weekday::Sun && current_date > month_start {
            push_weekly_if_needed(
                &mut weekly_subtotals,
                current_date - Duration::days(1),
                week_drive,
                week_cargo,
                week_break,
                week_restraint,
            );
            week_drive = 0;
            week_cargo = 0;
            week_break = 0;
            week_restraint = 0;
        }
        current_week_end = Some(current_date);

        if let Some(segs) = day_groups.get(&current_date) {
            // Group by unko_no
            let mut op_map: BTreeMap<&str, (i32, i32, i32, i32)> = BTreeMap::new();

            for seg in segs {
                let entry = op_map.entry(&seg.unko_no).or_insert((0, 0, 0, 0));
                entry.0 += seg.drive_minutes;
                entry.1 += seg.cargo_minutes;
                let seg_break = (seg.work_minutes - seg.drive_minutes - seg.cargo_minutes).max(0);
                entry.2 += seg_break;
                entry.3 += seg.work_minutes;
            }

            // 始業: operations.departure_at（分切り捨て）
            // 終業: daily_work_segmentsの最後のend_at（分切り捨て）
            // ※ return_atは帰庫処理時刻で数十秒のズレがあるため、セグメント終了を使う
            let day_start = op_times_map
                .get(&current_date)
                .map(|ot| fmt_trunc(&ot.first_departure))
                .or_else(|| segs.iter().map(|s| s.start_at).min().map(|t| fmt_trunc(&t)));
            // 終業: operation_date単位のセグメント最終end_at（日跨ぎ対応）
            let day_end = op_times_map
                .get(&current_date)
                .map(|ot| fmt_trunc(&ot.last_seg_end))
                .or_else(|| segs.iter().map(|s| s.end_at).max().map(|t| fmt_trunc(&t)));

            let operations: Vec<OperationDetail> = op_map
                .iter()
                .map(
                    |(unko_no, (drive, cargo, brk, restraint))| OperationDetail {
                        unko_no: unko_no.to_string(),
                        drive_minutes: *drive,
                        cargo_minutes: *cargo,
                        break_minutes: *brk,
                        restraint_minutes: *restraint,
                    },
                )
                .collect();

            // daily_work_hours から取得（同日複数行対応）
            let dwh_list = dwh_map.get(&current_date);
            let seg_restraint: i32 = operations.iter().map(|o| o.restraint_minutes).sum();

            // dwh_listがない場合はセグメントから1行生成
            // dwh_listがある場合は各dwh行ごとに1行ずつ生成
            let dwh_entries: Vec<Option<&&DailyWorkHoursRow>> = match dwh_list {
                Some(list) => list.iter().map(Some).collect(),
                None => vec![None],
            };

            for (dwh_idx, dwh) in dwh_entries.iter().enumerate() {
                let day_drive = dwh
                    .map(|r| r.drive_minutes)
                    .unwrap_or_else(|| operations.iter().map(|o| o.drive_minutes).sum());
                let day_cargo = dwh
                    .map(|r| r.cargo_minutes)
                    .unwrap_or_else(|| operations.iter().map(|o| o.cargo_minutes).sum());
                let day_restraint = dwh.map(|r| r.total_work_minutes).unwrap_or(seg_restraint);
                let day_break = (day_restraint - day_drive - day_cargo).max(0);
                let overlap_drive = dwh.map(|r| r.overlap_drive_minutes).unwrap_or(0);
                let overlap_cargo = dwh.map(|r| r.overlap_cargo_minutes).unwrap_or(0);
                let overlap_break = dwh.map(|r| r.overlap_break_minutes).unwrap_or(0);
                let overlap_restraint = dwh.map(|r| r.overlap_restraint_minutes).unwrap_or(0);

                cumulative += day_restraint;

                let drive_avg_before = prev_main_drive.map(|prev| (prev + day_drive) / 2);
                let drive_avg = match prev_main_drive {
                    Some(prev) => (prev + day_drive) as f64 / 2.0,
                    None => day_drive as f64,
                };

                let actual_work = day_drive + day_cargo;
                let ot_late_night = dwh.map(|r| r.ot_late_night_minutes).unwrap_or(0);
                let total_overtime = (actual_work - 480).max(0);
                let overtime = (total_overtime - ot_late_night).max(0);

                let rest_period = dwh.and_then(|r| r.total_rest_minutes).filter(|&v| v > 0);
                let late_night = dwh.map(|r| r.late_night_minutes).unwrap_or(0);

                week_drive += day_drive;
                week_cargo += day_cargo;
                week_break += day_break;
                week_restraint += day_restraint;

                // 同日複数行の場合、始業はdwh行のstart_time、終業は1行目だけセグメントから
                let (row_start, row_end) = if dwh_entries.len() > 1 {
                    let st = dwh
                        .map(|r| format!("{}:{:02}", r.start_time.hour(), r.start_time.minute()));
                    (st, if dwh_idx == 0 { day_end.clone() } else { None })
                } else {
                    (day_start.clone(), day_end.clone())
                };

                days.push(RestraintDayRow {
                    date: current_date,
                    is_holiday: false,
                    start_time: row_start,
                    end_time: row_end,
                    operations: if dwh_idx == 0 {
                        operations.clone()
                    } else {
                        Vec::new()
                    },
                    drive_minutes: day_drive,
                    cargo_minutes: day_cargo,
                    break_minutes: day_break,
                    restraint_total_minutes: day_restraint + overlap_restraint,
                    restraint_cumulative_minutes: cumulative,
                    drive_average_minutes: (drive_avg * 100.0).round() / 100.0,
                    rest_period_minutes: rest_period,
                    remarks: String::new(),
                    overlap_drive_minutes: overlap_drive,
                    overlap_cargo_minutes: overlap_cargo,
                    overlap_break_minutes: overlap_break,
                    overlap_restraint_minutes: overlap_restraint,
                    restraint_main_minutes: day_restraint,
                    drive_avg_before,
                    drive_avg_after: None,
                    actual_work_minutes: actual_work,
                    overtime_minutes: overtime,
                    late_night_minutes: late_night,
                    overtime_late_night_minutes: ot_late_night,
                });
                prev_main_drive = Some(day_drive);
            }
        } else {
            // No work on this day (holiday/off)
            days.push(RestraintDayRow {
                date: current_date,
                is_holiday: true,
                start_time: None,
                end_time: None,
                operations: Vec::new(),
                drive_minutes: 0,
                cargo_minutes: 0,
                break_minutes: 0,
                restraint_total_minutes: 0,
                restraint_cumulative_minutes: cumulative,
                drive_average_minutes: 0.0,
                rest_period_minutes: None,
                remarks: "休".to_string(),
                overlap_drive_minutes: 0,
                overlap_cargo_minutes: 0,
                overlap_break_minutes: 0,
                overlap_restraint_minutes: 0,
                restraint_main_minutes: 0,
                drive_avg_before: None,
                drive_avg_after: None,
                actual_work_minutes: 0,
                overtime_minutes: 0,
                late_night_minutes: 0,
                overtime_late_night_minutes: 0,
            });
            // 休日の場合、prev_main_drive は 0 として扱う（CSV準拠）
            prev_main_drive = Some(0);
        }

        current_date += Duration::days(1);
    }

    // Pass 2: 後運転平均を埋める（当日の主運転 + 翌日の主運転）/ 2
    for i in 0..days.len() {
        if days[i].is_holiday {
            continue;
        }
        let current_main_drive = days[i].drive_minutes;
        let next_main_drive = days.get(i + 1).map_or(0, |d| d.drive_minutes); // 休日なら0, 月末なら0
        days[i].drive_avg_after = Some((current_main_drive + next_main_drive) / 2);
    }

    // Final weekly subtotal
    push_weekly_if_needed(
        &mut weekly_subtotals,
        current_week_end.unwrap_or(month_end),
        week_drive,
        week_cargo,
        week_break,
        week_restraint,
    );

    let monthly_total = MonthlyTotal {
        drive_minutes: days.iter().map(|d| d.drive_minutes).sum(),
        cargo_minutes: days.iter().map(|d| d.cargo_minutes).sum(),
        break_minutes: days.iter().map(|d| d.break_minutes).sum(),
        restraint_minutes: cumulative,
        fiscal_year_cumulative_minutes: inputs.fiscal_cumulative,
        fiscal_year_total_minutes: inputs.fiscal_cumulative + cumulative,
        overlap_drive_minutes: days.iter().map(|d| d.overlap_drive_minutes).sum(),
        overlap_cargo_minutes: days.iter().map(|d| d.overlap_cargo_minutes).sum(),
        overlap_break_minutes: days.iter().map(|d| d.overlap_break_minutes).sum(),
        overlap_restraint_minutes: days.iter().map(|d| d.overlap_restraint_minutes).sum(),
        actual_work_minutes: days.iter().map(|d| d.actual_work_minutes).sum(),
        overtime_minutes: days.iter().map(|d| d.overtime_minutes).sum(),
        late_night_minutes: days.iter().map(|d| d.late_night_minutes).sum(),
        overtime_late_night_minutes: 0,
    };

    RestraintReportResponse {
        driver_id,
        driver_name: driver_name.to_string(),
        year,
        month,
        max_restraint_minutes: MAX_RESTRAINT_MINUTES,
        days,
        weekly_subtotals,
        monthly_total,
    }
}

fn push_weekly_if_needed(
    subtotals: &mut Vec<WeeklySubtotal>,
    week_end_date: NaiveDate,
    drive: i32,
    cargo: i32,
    brk: i32,
    restraint: i32,
) {
    if restraint > 0 {
        subtotals.push(WeeklySubtotal {
            week_end_date,
            drive_minutes: drive,
            cargo_minutes: cargo,
            break_minutes: brk,
            restraint_minutes: restraint,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn month_range_covers_the_month_and_the_fiscal_year() {
        // 12 月は翌年の 1/1 の前日が月末。年度の累計は 4/1 〜 11/30
        let dec = MonthRange::new(2025, 12).unwrap();
        assert_eq!(
            dec,
            MonthRange {
                month_start: date(2025, 12, 1),
                month_end: date(2025, 12, 31),
                prev_day: date(2025, 11, 30),
                fiscal: Some((date(2025, 4, 1), date(2025, 11, 30))),
            }
        );
        // 1 月の年度は前年の 4/1 から
        assert_eq!(
            MonthRange::new(2026, 1).unwrap().fiscal,
            Some((date(2025, 4, 1), date(2025, 12, 31)))
        );
        // 4 月は年度の始めなので累計を読まない
        assert_eq!(MonthRange::new(2026, 4).unwrap().fiscal, None);
        // 日付にならない年月
        assert_eq!(MonthRange::new(2026, 13), None);
        assert_eq!(MonthRange::new(2026, 0), None);
        // 翌月が日付の範囲を超える
        assert_eq!(MonthRange::new(NaiveDate::MAX.year(), 12), None);
    }

    /// 前日の運転が無い (`None`) と、1 日目の前運転平均は当日の運転そのもの・`drive_avg_before` は無し。
    #[test]
    fn first_day_without_prev_drive_uses_its_own_drive() {
        let range = MonthRange::new(2026, 2).unwrap();
        let at = |h: u32| Utc.with_ymd_and_hms(2026, 2, 1, h, 0, 0).unwrap();
        let inputs = ReportInputs {
            segments: vec![SegmentRow {
                work_date: date(2026, 2, 1),
                unko_no: "U1".to_owned(),
                start_at: at(8),
                end_at: at(17),
                work_minutes: 540,
                drive_minutes: 300,
                cargo_minutes: 60,
            }],
            ..ReportInputs::default()
        };
        let r = build_report(Uuid::nil(), "名前", 2026, 2, range, &inputs);
        let day1 = &r.days[0];
        assert_eq!(
            (day1.start_time.as_deref(), day1.end_time.as_deref()),
            (Some("8:00"), Some("17:00"))
        );
        assert_eq!(day1.drive_avg_before, None);
        assert_eq!(day1.drive_average_minutes, 300.0);
        // 翌日 (休み) の運転は 0 なので後運転平均は半分
        assert_eq!(day1.drive_avg_after, Some(150));
        // 実働 360 は 8 時間未満なので時間外なし
        assert_eq!(day1.overtime_minutes, 0);
        assert_eq!(r.max_restraint_minutes, MAX_RESTRAINT_MINUTES);
        assert_eq!(r.weekly_subtotals.len(), 1);
        assert_eq!(r.monthly_total.restraint_minutes, 540);
    }

    /// 実働が 8 時間を超えた分が時間外。時間外深夜の分は時間外から引く。
    #[test]
    fn overtime_excludes_overtime_late_night() {
        let range = MonthRange::new(2026, 2).unwrap();
        let inputs = ReportInputs {
            segments: vec![SegmentRow {
                work_date: date(2026, 2, 3),
                unko_no: "U1".to_owned(),
                start_at: Utc.with_ymd_and_hms(2026, 2, 3, 1, 0, 0).unwrap(),
                end_at: Utc.with_ymd_and_hms(2026, 2, 3, 15, 0, 0).unwrap(),
                work_minutes: 840,
                drive_minutes: 500,
                cargo_minutes: 100,
            }],
            daily_work_hours: vec![DailyWorkHoursRow {
                work_date: date(2026, 2, 3),
                start_time: chrono::NaiveTime::from_hms_opt(1, 0, 0).unwrap(),
                total_work_minutes: 840,
                total_rest_minutes: Some(0),
                late_night_minutes: 0,
                drive_minutes: 500,
                cargo_minutes: 100,
                overlap_drive_minutes: 0,
                overlap_cargo_minutes: 0,
                overlap_break_minutes: 0,
                overlap_restraint_minutes: 0,
                ot_late_night_minutes: 30,
            }],
            prev_day_drive: Some(200),
            ..ReportInputs::default()
        };
        let r = build_report(Uuid::nil(), "", 2026, 2, range, &inputs);
        let day = &r.days[2];
        assert_eq!(day.actual_work_minutes, 600);
        assert_eq!(day.overtime_minutes, 90);
        // 休息 0 は載せない
        assert_eq!(day.rest_period_minutes, None);
    }
}
