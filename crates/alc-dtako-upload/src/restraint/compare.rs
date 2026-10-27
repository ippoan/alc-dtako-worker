//! 拘束時間管理表の CSV とシステムの帳票の比較 (純粋な部分。W5c-3)。
//!
//! rust-alc-api の `alc_dtako::dtako_restraint_report::compare_csv` の、DB を読まない部分を写したもの。
//! CSV の読み方 (`parse_restraint_csv`)・既知のずれの注記 (`annotate_known_bugs`)・分の書き方 (`fmt_min`) は
//! 共有の crate `alc-compare` のまま。

use alc_compare::{annotate_known_bugs, fmt_min, CsvDriverData, DiffItem};
use chrono::Datelike;

use super::models::{CompareResult, RestraintReportResponse, SystemDayRow, SystemDriverData};

/// CSV の最初の日付の行 (`2月1日`) から年月を推す。年は CSV から取れないので 2026 固定、読めなければ 2026 年 1 月
/// (rust と同じ。要改善)。
pub fn csv_year_month(csv_drivers: &[CsvDriverData]) -> (i32, u32) {
    csv_drivers
        .first()
        .and_then(|d| d.days.first())
        .and_then(|day| {
            let s = &day.date;
            let m_pos = s.find('月')?;
            s[..m_pos].parse::<u32>().ok()
        })
        .map(|m| (2026i32, m))
        .unwrap_or((2026, 1))
}

/// 帳票の日の行を、CSV と同じ書き方 (`2月1日`・`H:MM`) の行にする。
pub fn system_days(report: &RestraintReportResponse) -> Vec<SystemDayRow> {
    report
        .days
        .iter()
        .map(|d| SystemDayRow {
            date: format!("{}月{}日", d.date.month(), d.date.day()),
            start_time: d.start_time.clone().unwrap_or_default(),
            end_time: d.end_time.clone().unwrap_or_default(),
            drive: fmt_min(d.drive_minutes),
            overlap_drive: fmt_min(d.overlap_drive_minutes),
            cargo: fmt_min(d.cargo_minutes),
            overlap_cargo: fmt_min(d.overlap_cargo_minutes),
            subtotal: fmt_min(d.restraint_main_minutes),
            overlap_subtotal: fmt_min(d.overlap_restraint_minutes),
            total: fmt_min(d.restraint_total_minutes),
            cumulative: fmt_min(d.restraint_cumulative_minutes),
            actual_work: fmt_min(d.actual_work_minutes),
            overtime: fmt_min(d.overtime_minutes),
            late_night: fmt_min(d.late_night_minutes),
        })
        .collect()
}

/// 日付マッチング方式の差分検出。
/// csv_days の休日行をスキップし、sys_days を日付で順方向検索してマッチさせる (休日行のずれや欠損日に対応する)。
pub fn detect_diffs_matched(
    csv_days: &[alc_compare::CsvDayRow],
    sys_days: &[SystemDayRow],
) -> Vec<DiffItem> {
    let mut diffs = Vec::new();
    let mut sys_idx = 0;
    for csv_day in csv_days {
        if csv_day.is_holiday {
            continue;
        }
        // 同じ日付のsys_dayを探す（複数行対応: sys_idxから順に探す）
        let Some(pos) = sys_days[sys_idx..]
            .iter()
            .position(|s| s.date == csv_day.date)
        else {
            continue; // システムにない日はスキップ
        };
        let sys_day = &sys_days[sys_idx + pos];
        sys_idx += pos + 1;
        let checks = [
            ("運転", &csv_day.drive, &sys_day.drive),
            ("重複運転", &csv_day.overlap_drive, &sys_day.overlap_drive),
            ("小計", &csv_day.subtotal, &sys_day.subtotal),
            (
                "重複小計",
                &csv_day.overlap_subtotal,
                &sys_day.overlap_subtotal,
            ),
            ("合計", &csv_day.total, &sys_day.total),
            ("累計", &csv_day.cumulative, &sys_day.cumulative),
            ("実働", &csv_day.actual_work, &sys_day.actual_work),
            ("時間外", &csv_day.overtime, &sys_day.overtime),
            ("深夜", &csv_day.late_night, &sys_day.late_night),
        ];
        for (field, csv_val, sys_val) in checks {
            let cv = csv_val.trim();
            let sv = sys_val.trim();
            if cv != sv {
                diffs.push(DiffItem {
                    date: csv_day.date.clone(),
                    field: field.to_string(),
                    csv_val: cv.to_string(),
                    sys_val: sv.to_string(),
                    known_bug: None,
                });
            }
        }
    }
    diffs
}

/// CSV の乗務員 1 人ぶんの比較の結果を作る。
/// `matched` は CSV の乗務員CD に当たった乗務員の id と、その帳票 (組み立てに失敗したら `None`)。当たらなければ `None`。
pub fn compare_result(
    csv_d: &CsvDriverData,
    matched: Option<(String, Option<RestraintReportResponse>)>,
) -> CompareResult {
    let (driver_id, system, mut diffs) = match matched {
        Some((did, Some(report))) => {
            let sys_days = system_days(&report);
            let diffs = detect_diffs_matched(&csv_d.days, &sys_days);
            let total = &report.monthly_total;
            let sys_data = SystemDriverData {
                days: sys_days,
                total_drive: fmt_min(total.drive_minutes),
                total_overlap_drive: fmt_min(total.overlap_drive_minutes),
                total_restraint: fmt_min(total.restraint_minutes),
                total_actual_work: fmt_min(total.actual_work_minutes),
                total_overtime: fmt_min(total.overtime_minutes),
                total_late_night: fmt_min(total.late_night_minutes),
            };
            (Some(did), Some(sys_data), diffs)
        }
        Some((did, None)) => (Some(did), None, Vec::new()),
        None => (None, None, Vec::new()),
    };
    // 既知バグアノテーション適用
    annotate_known_bugs(&csv_d.driver_cd, &mut diffs, &mut []);
    let known_bug_diffs = diffs.iter().filter(|d| d.known_bug.is_some()).count();
    let unknown_diffs = diffs.len() - known_bug_diffs;
    CompareResult {
        driver_name: csv_d.driver_name.clone(),
        driver_cd: csv_d.driver_cd.clone(),
        driver_id,
        csv: csv_d.clone(),
        system,
        diffs,
        known_bug_diffs,
        unknown_diffs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alc_compare::CsvDayRow;

    fn csv_day(date: &str, is_holiday: bool, drive: &str) -> CsvDayRow {
        CsvDayRow {
            date: date.to_string(),
            is_holiday,
            start_time: String::new(),
            end_time: String::new(),
            drive: drive.to_string(),
            overlap_drive: String::new(),
            cargo: String::new(),
            overlap_cargo: String::new(),
            break_time: String::new(),
            overlap_break: String::new(),
            subtotal: String::new(),
            overlap_subtotal: String::new(),
            total: String::new(),
            cumulative: String::new(),
            rest: String::new(),
            actual_work: String::new(),
            overtime: String::new(),
            late_night: String::new(),
            ot_late_night: String::new(),
            remarks: String::new(),
        }
    }

    fn sys_day(date: &str, drive: &str) -> SystemDayRow {
        SystemDayRow {
            date: date.to_string(),
            start_time: String::new(),
            end_time: String::new(),
            drive: drive.to_string(),
            overlap_drive: String::new(),
            cargo: String::new(),
            overlap_cargo: String::new(),
            subtotal: String::new(),
            overlap_subtotal: String::new(),
            total: String::new(),
            cumulative: String::new(),
            actual_work: String::new(),
            overtime: String::new(),
            late_night: String::new(),
        }
    }

    #[test]
    fn detect_diffs_matched_skips_holidays_and_missing_days() {
        // 休日行・システムに無い日は比べない。同じ日付が 2 行あれば順に当てる。前後の空白は無視
        let csv = vec![
            csv_day("2月1日", true, ""),
            csv_day("2月2日", false, "5:00"),
            csv_day("2月2日", false, "3:00"),
            csv_day("2月3日", false, "4:00"),
            csv_day("2月4日", false, " 2:00 "),
        ];
        let sys = vec![
            sys_day("2月1日", "9:99"),
            sys_day("2月2日", "5:00"),
            sys_day("2月2日", "3:30"),
            sys_day("2月4日", "2:00"),
        ];
        let diffs = detect_diffs_matched(&csv, &sys);
        assert_eq!(diffs.len(), 1);
        assert_eq!(
            (
                diffs[0].date.as_str(),
                diffs[0].field.as_str(),
                diffs[0].csv_val.as_str(),
                diffs[0].sys_val.as_str()
            ),
            ("2月2日", "運転", "3:00", "3:30")
        );
        assert!(diffs[0].known_bug.is_none());
    }

    #[test]
    fn csv_year_month_reads_the_first_date() {
        let driver = |days: Vec<CsvDayRow>| CsvDriverData {
            driver_name: String::new(),
            driver_cd: String::new(),
            days,
            total_drive: String::new(),
            total_cargo: String::new(),
            total_break: String::new(),
            total_restraint: String::new(),
            total_actual_work: String::new(),
            total_overtime: String::new(),
            total_late_night: String::new(),
            total_ot_late_night: String::new(),
        };
        assert_eq!(
            csv_year_month(&[driver(vec![csv_day("3月1日", true, "")])]),
            (2026, 3)
        );
        // 読めなければ 2026 年 1 月
        assert_eq!(
            csv_year_month(&[driver(vec![csv_day("x月1日", true, "")])]),
            (2026, 1)
        );
        assert_eq!(csv_year_month(&[driver(vec![])]), (2026, 1));
        assert_eq!(csv_year_month(&[]), (2026, 1));
    }
}
