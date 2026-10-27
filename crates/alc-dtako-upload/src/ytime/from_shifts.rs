//! 勤怠の勤務の列 (始業・終業 + 実働でない区間) を Y時間 シート行にする pure logic。
//!
//! 行の入れ方の規則はここに書かない。勤務を `SegmentInput` に写して
//! `builder::build_y_time_rows` に渡すだけ。ここが持つのは入力の検証と、
//! 行を作らない勤務の名指し (`excluded`) だけ。DB / R2 アクセスは含まない。

use std::collections::HashSet;

use chrono::{NaiveDateTime, Timelike};

use super::builder::{build_y_time_rows, fits_no_row_shape, SegmentInput};
use super::models::{
    YTimeExcludedReason, YTimeExcludedShift, YTimeRowsRequest, YTimeRowsResponse, YTimeShiftInput,
};

/// 期間 (`from`〜`to`、両端を含む) の上限の日数。
pub const MAX_PERIOD_DAYS: i64 = 400;
/// 1 リクエストの勤務の上限。
pub const MAX_SHIFTS: usize = 2000;
/// 1 勤務の実働でない区間の上限。
pub const MAX_NON_WORKING_PER_SHIFT: usize = 100;

/// 勤務の列から行を作る。`Err` は入力の検証の破れ (呼び手の誤り。HTTP では 400)。
///
/// 検証を通った勤務のうち、行を作らないものは理由つきで `excluded` に入れ、
/// 残りだけを `build_y_time_rows` に渡す (黙って丸めない・黙って切り詰めない)。
pub fn rows_from_shifts(req: YTimeRowsRequest) -> Result<YTimeRowsResponse, String> {
    validate(&req)?;

    let overlapping = overlapping_shifts(&req.shifts);
    let mut segments: Vec<SegmentInput> = Vec::with_capacity(req.shifts.len());
    let mut excluded: Vec<YTimeExcludedShift> = Vec::new();

    for (shift, overlaps) in req.shifts.into_iter().zip(overlapping) {
        if let Some(reason) = exclusion_reason(&shift, overlaps) {
            excluded.push(YTimeExcludedShift {
                start: shift.start,
                end: shift.end,
                reason,
            });
            continue;
        }
        // 種別は見ない (どの種別も実働でない)
        let rest_intervals: Vec<(NaiveDateTime, NaiveDateTime)> = shift
            .non_working
            .iter()
            .flatten()
            .map(|n| (n.start, n.end))
            .collect();
        segments.push(SegmentInput {
            start: shift.start,
            end: shift.end,
            rest_minutes: rest_intervals
                .iter()
                .map(|(s, e)| (*e - *s).num_minutes() as i32)
                .sum(),
            rest_intervals,
            note: shift.note,
        });
    }

    let (rows, warnings) = build_y_time_rows(segments, req.from, req.to);
    Ok(YTimeRowsResponse {
        rows,
        warnings,
        excluded,
    })
}

/// 勤務から行を作らない理由。`None` なら行にする。複数当たるときは上から先に当たったもの。
fn exclusion_reason(
    shift: &YTimeShiftInput,
    overlaps_another: bool,
) -> Option<YTimeExcludedReason> {
    if shift.non_working.is_none() {
        return Some(YTimeExcludedReason::NoNonWorking);
    }
    if (shift.end.date() - shift.start.date()).num_days() >= 2 {
        return Some(YTimeExcludedReason::ThreeDays);
    }
    if overlaps_another {
        return Some(YTimeExcludedReason::Overlap);
    }
    if fits_no_row_shape(shift.start, shift.end) {
        return Some(YTimeExcludedReason::NightBands);
    }
    None
}

/// 勤務ごとに「同じ入力の中の別の勤務と時間帯 `[start, end)` が重なるか」。
/// 終業と次の始業が同じ時刻なのは重なりではない。
fn overlapping_shifts(shifts: &[YTimeShiftInput]) -> Vec<bool> {
    let mut order: Vec<usize> = (0..shifts.len()).collect();
    order.sort_by_key(|&i| shifts[i].start);

    let mut overlaps = vec![false; shifts.len()];
    // 始業の早い順に見て、ここまでの終業の最大とその持ち主を覚える
    let mut latest: Option<usize> = None;
    for i in order {
        match latest {
            Some(j) if shifts[i].start < shifts[j].end => {
                overlaps[i] = true;
                overlaps[j] = true;
                if shifts[i].end > shifts[j].end {
                    latest = Some(i);
                }
            }
            _ => latest = Some(i),
        }
    }
    overlaps
}

fn validate(req: &YTimeRowsRequest) -> Result<(), String> {
    if req.from > req.to {
        return Err("from > to".to_string());
    }
    if (req.to - req.from).num_days() >= MAX_PERIOD_DAYS {
        return Err(format!("期間は {MAX_PERIOD_DAYS} 日以内"));
    }
    if req.shifts.len() > MAX_SHIFTS {
        return Err(format!("shifts は {MAX_SHIFTS} 本以内"));
    }

    let mut seen: HashSet<(NaiveDateTime, NaiveDateTime)> = HashSet::new();
    for (i, shift) in req.shifts.iter().enumerate() {
        // 秒つきを分に切り捨てて受けない (下の「勤務の中に収まる」の検査が 1 分ずれるため)
        if shift.start.second() != 0 || shift.end.second() != 0 {
            return Err(format!("shifts[{i}]: start / end の秒は 0"));
        }
        if shift.end <= shift.start {
            return Err(format!("shifts[{i}]: end は start より後"));
        }
        if !seen.insert((shift.start, shift.end)) {
            return Err(format!(
                "shifts[{i}]: 始業と終業が同じ勤務が 2 回入っている"
            ));
        }
        let Some(non_working) = &shift.non_working else {
            continue;
        };
        if non_working.len() > MAX_NON_WORKING_PER_SHIFT {
            return Err(format!(
                "shifts[{i}]: non_working は {MAX_NON_WORKING_PER_SHIFT} 個以内"
            ));
        }
        let mut prev_end = shift.start;
        for (k, n) in non_working.iter().enumerate() {
            if n.start.second() != 0 || n.end.second() != 0 {
                return Err(format!(
                    "shifts[{i}].non_working[{k}]: start / end の秒は 0"
                ));
            }
            if n.end <= n.start {
                return Err(format!("shifts[{i}].non_working[{k}]: end は start より後"));
            }
            if n.start < shift.start || n.end > shift.end {
                return Err(format!("shifts[{i}].non_working[{k}]: 勤務の外に出ている"));
            }
            if n.start < prev_end {
                return Err(format!(
                    "shifts[{i}].non_working[{k}]: 昇順でないか、前の区間と重なっている"
                ));
            }
            prev_end = n.end;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ytime::models::{YTimeNonWorking, YTimeRow};
    use chrono::NaiveDate;

    fn dt(day: u32, h: u32, m: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2024, 4, day)
            .unwrap()
            .and_hms_opt(h, m, 0)
            .unwrap()
    }

    fn d(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2024, 4, day).unwrap()
    }

    fn shift(
        start: NaiveDateTime,
        end: NaiveDateTime,
        non_working: &[(NaiveDateTime, NaiveDateTime)],
    ) -> YTimeShiftInput {
        YTimeShiftInput {
            start,
            end,
            non_working: Some(
                non_working
                    .iter()
                    .map(|(s, e)| YTimeNonWorking { start: *s, end: *e })
                    .collect(),
            ),
            note: None,
        }
    }

    fn req(shifts: Vec<YTimeShiftInput>) -> YTimeRowsRequest {
        YTimeRowsRequest {
            from: d(1),
            to: d(30),
            shifts,
        }
    }

    fn rest_cells_total(r: &YTimeRow) -> i32 {
        r.rest_prev_5_22
            + r.rest_prev_22_0
            + r.rest_today_0_5
            + r.rest_today_5_22
            + r.rest_today_22_0
            + r.rest_next_0_5
            + r.rest_next_5_22
    }

    #[test]
    fn same_day_shift_becomes_one_row_with_all_non_working_in_cells() {
        // 06:00-20:00、休息 10:00-14:00 + 休憩 14:00-14:30 (種別は見ない)
        let s = shift(
            dt(6, 6, 0),
            dt(6, 20, 0),
            &[(dt(6, 10, 0), dt(6, 14, 0)), (dt(6, 14, 0), dt(6, 14, 30))],
        );
        let resp = rows_from_shifts(req(vec![s])).unwrap();
        assert_eq!(resp.rows.len(), 1);
        let r = &resp.rows[0];
        assert_eq!(r.date, d(6));
        assert!(!r.previous_day_start);
        assert_eq!(r.start_minutes_of_day, 6 * 60);
        assert_eq!(r.end_minutes_from_bucket_date, 20 * 60);
        assert_eq!(r.rest_today_5_22, 270);
        assert_eq!(rest_cells_total(r), 270);
        assert!(resp.warnings.is_empty());
        assert!(resp.excluded.is_empty());
    }

    #[test]
    fn end_date_row_keeps_all_non_working_in_cells() {
        // 22:10 → 翌 09:05、休憩 23:00-23:30 と 翌 02:00-03:00。実働 9h25m → 終業日の行
        let s = shift(
            dt(3, 22, 10),
            dt(4, 9, 5),
            &[(dt(3, 23, 0), dt(3, 23, 30)), (dt(4, 2, 0), dt(4, 3, 0))],
        );
        let resp = rows_from_shifts(req(vec![s])).unwrap();
        let r = &resp.rows[0];
        assert_eq!(r.date, d(4));
        assert!(r.previous_day_start);
        assert_eq!(r.start_minutes_of_day, 22 * 60 + 10);
        assert_eq!(r.end_minutes_from_bucket_date, 9 * 60 + 5);
        assert_eq!(r.rest_prev_22_0, 30);
        assert_eq!(r.rest_today_0_5, 60);
        assert_eq!(rest_cells_total(r), 90);
    }

    #[test]
    fn shift_longer_than_24h_uses_start_date_row_with_24h_plus_end() {
        // 03:00 → 翌 10:00 (31h)。休憩 04:00-04:30 と 翌 06:00-07:00。
        // 実働 7h 以上だが始業が 5:00 より前 → 始業日の行、終業は 34:00
        let s = shift(
            dt(10, 3, 0),
            dt(11, 10, 0),
            &[(dt(10, 4, 0), dt(10, 4, 30)), (dt(11, 6, 0), dt(11, 7, 0))],
        );
        let resp = rows_from_shifts(req(vec![s])).unwrap();
        let r = &resp.rows[0];
        assert_eq!(r.date, d(10));
        assert!(!r.previous_day_start);
        assert_eq!(r.end_minutes_from_bucket_date, 34 * 60);
        assert_eq!(r.rest_today_0_5, 30);
        assert_eq!(r.rest_next_5_22, 60);
        assert_eq!(rest_cells_total(r), 90);
        assert!(resp.excluded.is_empty());
    }

    #[test]
    fn two_shifts_on_same_day_merge_with_gap_as_rest() {
        // 06:00-10:00 (休憩 08:00-08:15) と 13:00-18:00 (休憩 15:00-15:45)。間 180 分
        let a = shift(dt(8, 6, 0), dt(8, 10, 0), &[(dt(8, 8, 0), dt(8, 8, 15))]);
        let b = shift(dt(8, 13, 0), dt(8, 18, 0), &[(dt(8, 15, 0), dt(8, 15, 45))]);
        let resp = rows_from_shifts(req(vec![b, a])).unwrap();
        assert_eq!(resp.rows.len(), 1);
        let r = &resp.rows[0];
        assert_eq!(r.start_minutes_of_day, 6 * 60);
        assert_eq!(r.end_minutes_from_bucket_date, 18 * 60);
        assert_eq!(rest_cells_total(r), 15 + 45 + 180);
        assert_eq!(
            resp.warnings,
            vec!["2024-04-08: 複数 segment 結合 (1 行に集約: 最早始業 / 最遅終業 / 間の 180 分を休憩に算入)".to_string()]
        );
    }

    #[test]
    fn empty_non_working_is_a_row_without_rest() {
        let resp = rows_from_shifts(req(vec![shift(dt(6, 8, 0), dt(6, 17, 0), &[])])).unwrap();
        assert_eq!(resp.rows.len(), 1);
        assert_eq!(rest_cells_total(&resp.rows[0]), 0);
        assert!(resp.excluded.is_empty());
    }

    #[test]
    fn note_and_period_pass_through_to_builder() {
        let mut s = shift(dt(6, 8, 0), dt(6, 17, 0), &[]);
        s.note = Some("備考".to_string());
        let mut r = req(vec![s]);
        let resp = rows_from_shifts(r.clone()).unwrap();
        assert_eq!(resp.rows[0].note.as_deref(), Some("備考"));

        // 行の日付が期間の外なら行にならない (除外の名指しもしない — 既存の経路と同じ)
        r.from = d(7);
        let resp = rows_from_shifts(r).unwrap();
        assert!(resp.rows.is_empty());
        assert!(resp.excluded.is_empty());
    }

    /// 除外された勤務が行に入らず、ほかの勤務の行が変わらないことを確かめる。
    fn assert_excluded(extra: Vec<YTimeShiftInput>, reason: YTimeExcludedReason) {
        let keep = shift(
            dt(20, 8, 0),
            dt(20, 17, 0),
            &[(dt(20, 12, 0), dt(20, 13, 0))],
        );
        let alone = rows_from_shifts(req(vec![keep.clone()])).unwrap();
        assert_eq!(alone.rows.len(), 1);

        let mut shifts = extra.clone();
        shifts.push(keep);
        let resp = rows_from_shifts(req(shifts)).unwrap();
        assert_eq!(resp.rows, alone.rows);
        assert_eq!(resp.warnings, alone.warnings);
        let expected: Vec<YTimeExcludedShift> = extra
            .iter()
            .map(|s| YTimeExcludedShift {
                start: s.start,
                end: s.end,
                reason,
            })
            .collect();
        assert_eq!(resp.excluded, expected);
    }

    #[test]
    fn excludes_shift_without_non_working() {
        let mut s = shift(dt(6, 8, 0), dt(6, 17, 0), &[]);
        s.non_working = None;
        assert_excluded(vec![s], YTimeExcludedReason::NoNonWorking);
    }

    #[test]
    fn excludes_shift_spanning_three_days() {
        // 始業日と終業日が 2 日離れている (4/6 22:00 → 4/8 01:00)
        let s = shift(dt(6, 22, 0), dt(8, 1, 0), &[]);
        assert_excluded(vec![s], YTimeExcludedReason::ThreeDays);
    }

    #[test]
    fn excludes_both_overlapping_shifts() {
        let a = shift(dt(6, 8, 0), dt(6, 17, 0), &[]);
        let b = shift(dt(6, 16, 0), dt(6, 20, 0), &[]);
        assert_excluded(vec![a, b], YTimeExcludedReason::Overlap);
    }

    #[test]
    fn excludes_shift_fitting_no_row_shape() {
        // 03:00 → 翌 23:00
        let s = shift(dt(6, 3, 0), dt(7, 23, 0), &[]);
        assert_excluded(vec![s], YTimeExcludedReason::NightBands);
    }

    #[test]
    fn overlap_marks_every_shift_in_a_chain_but_not_touching_ones() {
        // a が b・c を内包、d は a の終業と同時刻に始まる (重なりではない)
        let a = shift(dt(6, 6, 0), dt(6, 18, 0), &[]);
        let b = shift(dt(6, 7, 0), dt(6, 8, 0), &[]);
        let c = shift(dt(6, 9, 0), dt(6, 10, 0), &[]);
        let e = shift(dt(6, 18, 0), dt(6, 20, 0), &[]);
        assert_eq!(
            overlapping_shifts(&[e.clone(), c, a, b]),
            vec![false, true, true, true]
        );
        // 後から始まるほうが長い (終業の最大の持ち主が替わる)
        let p = shift(dt(7, 6, 0), dt(7, 9, 0), &[]);
        let q = shift(dt(7, 8, 0), dt(7, 12, 0), &[]);
        let r = shift(dt(7, 11, 0), dt(7, 13, 0), &[]);
        assert_eq!(
            overlapping_shifts(&[p, q, r, e]),
            vec![true, true, true, false]
        );
    }

    #[test]
    fn reason_priority_is_no_non_working_then_three_days_then_overlap() {
        // 3 日にまたがり、かつ別の勤務と重なる → three_days。相手は overlap
        let long = shift(dt(6, 22, 0), dt(8, 1, 0), &[]);
        let other = shift(dt(7, 8, 0), dt(7, 17, 0), &[]);
        // non_working が null で、かつ別の勤務と重なる → no_non_working
        let mut unfolded = shift(dt(7, 9, 0), dt(7, 10, 0), &[]);
        unfolded.non_working = None;
        let resp = rows_from_shifts(req(vec![long, other, unfolded])).unwrap();
        assert!(resp.rows.is_empty());
        let reasons: Vec<_> = resp.excluded.iter().map(|e| e.reason).collect();
        assert_eq!(
            reasons,
            vec![
                YTimeExcludedReason::ThreeDays,
                YTimeExcludedReason::Overlap,
                YTimeExcludedReason::NoNonWorking,
            ]
        );
    }

    #[test]
    fn validation_errors() {
        let ok = || shift(dt(6, 8, 0), dt(6, 17, 0), &[(dt(6, 12, 0), dt(6, 13, 0))]);
        let many_shifts: Vec<YTimeShiftInput> = (0..=MAX_SHIFTS as i64)
            .map(|i| {
                let start = dt(1, 0, 0) + chrono::Duration::minutes(i);
                shift(start, start + chrono::Duration::minutes(1), &[])
            })
            .collect();
        let many_non_working: Vec<(NaiveDateTime, NaiveDateTime)> = (0..=MAX_NON_WORKING_PER_SHIFT
            as i64)
            .map(|i| {
                let start = dt(6, 8, 0) + chrono::Duration::minutes(i);
                (start, start + chrono::Duration::minutes(1))
            })
            .collect();

        let sec = chrono::Duration::seconds(30);

        // (名前, 入力, エラーに含まれる語)
        let cases: Vec<(&str, YTimeRowsRequest, &str)> = vec![
            (
                "from > to",
                YTimeRowsRequest {
                    from: d(2),
                    to: d(1),
                    shifts: vec![],
                },
                "from > to",
            ),
            (
                "期間が 401 日",
                YTimeRowsRequest {
                    from: d(1),
                    to: d(1) + chrono::Duration::days(MAX_PERIOD_DAYS),
                    shifts: vec![],
                },
                "期間は 400 日以内",
            ),
            ("勤務が 2001 本", req(many_shifts), "shifts は 2000 本以内"),
            (
                "始業に秒が付いている",
                req(vec![shift(dt(6, 8, 0) + sec, dt(6, 17, 0), &[])]),
                "shifts[0]: start / end の秒は 0",
            ),
            (
                "終業に秒が付いている",
                req(vec![ok(), shift(dt(7, 8, 0), dt(7, 17, 0) + sec, &[])]),
                "shifts[1]: start / end の秒は 0",
            ),
            (
                "non_working が null の勤務の秒",
                req(vec![YTimeShiftInput {
                    start: dt(6, 8, 0) + sec,
                    end: dt(6, 17, 0),
                    non_working: None,
                    note: None,
                }]),
                "shifts[0]: start / end の秒は 0",
            ),
            (
                "区間の始まりに秒が付いている",
                req(vec![shift(
                    dt(6, 8, 0),
                    dt(6, 17, 0),
                    &[(dt(6, 12, 0) + sec, dt(6, 13, 0))],
                )]),
                "shifts[0].non_working[0]: start / end の秒は 0",
            ),
            (
                "区間の終わりに秒が付いている",
                req(vec![shift(
                    dt(6, 8, 0),
                    dt(6, 17, 0),
                    &[
                        (dt(6, 10, 0), dt(6, 11, 0)),
                        (dt(6, 12, 0), dt(6, 13, 0) + sec),
                    ],
                )]),
                "shifts[0].non_working[1]: start / end の秒は 0",
            ),
            (
                "end == start",
                req(vec![shift(dt(6, 8, 0), dt(6, 8, 0), &[])]),
                "shifts[0]: end は start より後",
            ),
            (
                "end < start",
                req(vec![ok(), shift(dt(6, 18, 0), dt(6, 17, 0), &[])]),
                "shifts[1]: end は start より後",
            ),
            (
                "同じ勤務が 2 回",
                req(vec![ok(), ok()]),
                "shifts[1]: 始業と終業が同じ勤務が 2 回",
            ),
            (
                "区間が 101 個",
                req(vec![shift(dt(6, 8, 0), dt(6, 17, 0), &many_non_working)]),
                "shifts[0]: non_working は 100 個以内",
            ),
            (
                "区間の end <= start",
                req(vec![shift(
                    dt(6, 8, 0),
                    dt(6, 17, 0),
                    &[(dt(6, 12, 0), dt(6, 12, 0))],
                )]),
                "shifts[0].non_working[0]: end は start より後",
            ),
            (
                "区間が始業より前",
                req(vec![shift(
                    dt(6, 8, 0),
                    dt(6, 17, 0),
                    &[(dt(6, 7, 0), dt(6, 9, 0))],
                )]),
                "勤務の外に出ている",
            ),
            (
                "区間が終業より後",
                req(vec![shift(
                    dt(6, 8, 0),
                    dt(6, 17, 0),
                    &[(dt(6, 16, 0), dt(6, 18, 0))],
                )]),
                "勤務の外に出ている",
            ),
            (
                "区間が昇順でない",
                req(vec![shift(
                    dt(6, 8, 0),
                    dt(6, 17, 0),
                    &[(dt(6, 14, 0), dt(6, 15, 0)), (dt(6, 10, 0), dt(6, 11, 0))],
                )]),
                "shifts[0].non_working[1]: 昇順でないか",
            ),
            (
                "区間どうしが重なる",
                req(vec![shift(
                    dt(6, 8, 0),
                    dt(6, 17, 0),
                    &[(dt(6, 10, 0), dt(6, 12, 0)), (dt(6, 11, 0), dt(6, 13, 0))],
                )]),
                "shifts[0].non_working[1]: 昇順でないか",
            ),
        ];
        for (name, input, expected) in cases {
            let err = rows_from_shifts(input).expect_err(name);
            assert!(err.contains(expected), "{name}: {err}");
        }
    }

    #[test]
    fn limits_are_inclusive() {
        // 期間ちょうど 400 日・勤務ちょうど 2000 本・区間ちょうど 100 個は通る
        let non_working: Vec<(NaiveDateTime, NaiveDateTime)> = (0..MAX_NON_WORKING_PER_SHIFT
            as i64)
            .map(|i| {
                let start = dt(6, 8, 0) + chrono::Duration::minutes(i);
                (start, start + chrono::Duration::minutes(1))
            })
            .collect();
        let mut shifts: Vec<YTimeShiftInput> = (1..MAX_SHIFTS as i64)
            .map(|i| {
                let start = dt(10, 0, 0) + chrono::Duration::minutes(i);
                shift(start, start + chrono::Duration::minutes(1), &[])
            })
            .collect();
        shifts.push(shift(dt(6, 8, 0), dt(6, 17, 0), &non_working));
        assert_eq!(shifts.len(), MAX_SHIFTS);
        let resp = rows_from_shifts(YTimeRowsRequest {
            from: d(1),
            to: d(1) + chrono::Duration::days(MAX_PERIOD_DAYS - 1),
            shifts,
        })
        .unwrap();
        assert!(resp.excluded.is_empty());
    }

    #[test]
    fn request_json_accepts_kind_and_missing_optional_fields() {
        let body = serde_json::json!({
            "from": "2024-04-01",
            "to": "2024-04-30",
            "shifts": [
                { "start": "2024-04-03 22:10:00", "end": "2024-04-04 09:05:00",
                  "non_working": [
                      { "start": "2024-04-04 02:00:00", "end": "2024-04-04 03:00:00", "kind": "break_event" },
                      { "start": "2024-04-04 04:00:00", "end": "2024-04-04 04:30:00" }
                  ],
                  "note": null },
                { "start": "2024-04-06 08:00:00", "end": "2024-04-06 17:00:00", "non_working": null },
                { "start": "2024-04-07 08:00:00", "end": "2024-04-07 17:00:00" }
            ]
        });
        let parsed: YTimeRowsRequest = serde_json::from_value(body).unwrap();
        assert_eq!(parsed.shifts[0].start, dt(3, 22, 10));
        assert_eq!(parsed.shifts[0].non_working.as_ref().unwrap().len(), 2);
        assert!(parsed.shifts[1].non_working.is_none());
        assert!(parsed.shifts[2].non_working.is_none());

        // 時刻の形が違えば読めない (日付だけ・T 区切り)
        for bad in ["2024-04-03", "2024-04-03T22:10:00"] {
            let body = serde_json::json!({
                "from": "2024-04-01", "to": "2024-04-30",
                "shifts": [{ "start": bad, "end": "2024-04-04 09:05:00", "non_working": [] }]
            });
            assert!(serde_json::from_value::<YTimeRowsRequest>(body).is_err());
        }
    }

    #[test]
    fn response_json_shape() {
        let kept = shift(dt(3, 22, 10), dt(4, 9, 5), &[(dt(4, 2, 0), dt(4, 3, 0))]);
        let second = shift(dt(4, 13, 0), dt(4, 17, 0), &[]);
        let three_days = shift(dt(10, 22, 0), dt(12, 1, 0), &[]);
        let resp = rows_from_shifts(req(vec![kept, second, three_days])).unwrap();
        assert_eq!(
            serde_json::to_value(&resp).unwrap(),
            serde_json::json!({
                "rows": [{
                    "date": "2024-04-04",
                    "previous_day_start": true,
                    "start_minutes_of_day": 1330,
                    "end_minutes_from_bucket_date": 1020,
                    "rest_prev_5_22": 0,
                    "rest_prev_22_0": 0,
                    "rest_today_0_5": 60,
                    "rest_today_5_22": 235,
                    "rest_today_22_0": 0,
                    "rest_next_0_5": 0,
                    "rest_next_5_22": 0,
                    "note": null
                }],
                "warnings": [
                    "2024-04-04: 複数 segment 結合 (1 行に集約: 最早始業 / 最遅終業 / 間の 235 分を休憩に算入)"
                ],
                "excluded": [
                    { "start": "2024-04-10 22:00:00", "end": "2024-04-12 01:00:00", "reason": "three_days" }
                ]
            })
        );
    }
}
