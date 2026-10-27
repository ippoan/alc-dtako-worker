//! KUDGIVT の行 → SegmentInput への変換 (rust-alc-api の `dtako_y_time_export::csv_aggregator` の純粋な部分を写したもの。
//! 保存先からの読みは [`super`] が持つ)。
//!
//! 既存の `alc_csv_parser` を流用し、本機能のためだけに新しい parsing は導入しない。

use alc_csv_parser::kudgivt::KudgivtRow;
use alc_csv_parser::work_segments::{
    default_classification, split_by_rest, EventClass, WorkSegment,
};
use chrono::NaiveDateTime;
use std::collections::HashMap;

use super::builder::SegmentInput;

/// 1 運行 (KUDGIVT events + departure/return) を `Vec<SegmentInput>` に変換する。
///
/// 分類は既定の表 (`alc_csv_parser::work_segments::default_classification`) だけを使う。
/// この read-only エンドポイントは tenant の event_classifications テーブルに対する
/// upsert 副作用を起こさない。tenant が分類を上書きしている場合 (例: 110 を Drive に
/// 再分類) には対応しないが、Phase 1 のスコープでは default のみで充分。
///
/// - `split_by_rest` で WorkSegment を出す
/// - 各 segment 内の 301 events を sum し rest_minutes として付与
/// - 1 segment が 24h を超えた場合 (= 休息イベント無しで 24h+ 連続労働、改善基準告示違反データ) は
///   **24h で強制 cut**。各 sub-segment の rest_minutes は該当範囲の 301 events を sum しなおす。
///   これにより builder 側 ((A) bucketing) が前日/当日/翌日 (3 暦日) の範囲で対応できる
pub fn build_segment_inputs(
    events: &[KudgivtRow],
    departure_at: NaiveDateTime,
    return_at: NaiveDateTime,
) -> Vec<SegmentInput> {
    let classifications: HashMap<String, EventClass> = events
        .iter()
        .map(|e| (e.event_cd.clone(), default_classification(&e.event_cd).1))
        .collect();

    let event_refs: Vec<&KudgivtRow> = events.iter().collect();
    let segments: Vec<WorkSegment> =
        split_by_rest(departure_at, return_at, &event_refs, &classifications);

    segments
        .into_iter()
        .flat_map(|s| split_at_24h(events, s.start, s.end))
        .collect()
}

/// 1 WorkSegment を 24h 境界で分割。`end - start <= 24h` ならそのまま 1 個。
fn split_at_24h(
    events: &[KudgivtRow],
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Vec<SegmentInput> {
    let mut out = Vec::new();
    let mut cur = start;
    while cur < end {
        let chunk_end = std::cmp::min(cur + chrono::Duration::hours(24), end);
        let intervals = collect_break_intervals(events, cur, chunk_end);
        let rest_minutes = intervals
            .iter()
            .map(|(s, e)| (*e - *s).num_minutes() as i32)
            .sum();
        out.push(SegmentInput {
            start: cur,
            end: chunk_end,
            rest_minutes,
            rest_intervals: intervals,
            note: None,
        });
        cur = chunk_end;
    }
    out
}

/// `[seg_start, seg_end]` 内の event_cd=301 の (start, end) を集める。
/// duration_minutes が None の場合はその event は無視 (期間 0 とみなす)。
fn collect_break_intervals(
    events: &[KudgivtRow],
    seg_start: NaiveDateTime,
    seg_end: NaiveDateTime,
) -> Vec<(NaiveDateTime, NaiveDateTime)> {
    events
        .iter()
        .filter(|e| e.event_cd == "301")
        .filter(|e| e.start_at >= seg_start && e.start_at <= seg_end)
        .filter_map(|e| {
            let dur = e.duration_minutes?;
            if dur <= 0 {
                return None;
            }
            let s = e.start_at;
            let raw_end = s + chrono::Duration::minutes(dur as i64);
            // segment 末尾でクリップ (segment 跨ぎ休憩は途中で切る)
            let e_clip = raw_end.min(seg_end);
            if e_clip <= s {
                None
            } else {
                Some((s, e_clip))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn dt(date: (i32, u32, u32), time: (u32, u32, u32)) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(date.0, date.1, date.2)
            .unwrap()
            .and_hms_opt(time.0, time.1, time.2)
            .unwrap()
    }

    fn ev(event_cd: &str, start: NaiveDateTime, dur: Option<i32>) -> KudgivtRow {
        KudgivtRow {
            unko_no: "UN1".into(),
            reading_date: NaiveDate::from_ymd_opt(2024, 4, 15).unwrap(),
            driver_cd: "D1".into(),
            driver_name: "Test".into(),
            crew_role: 1,
            start_at: start,
            end_at: dur.map(|d| start + chrono::Duration::minutes(d as i64)),
            event_cd: event_cd.into(),
            event_name: "x".into(),
            duration_minutes: dur,
            section_distance: None,
        }
    }

    #[test]
    fn default_mapping_covers_known_codes() {
        assert_eq!(default_classification("201").1, EventClass::Drive);
        assert_eq!(default_classification("202").1, EventClass::Cargo);
        assert_eq!(default_classification("203").1, EventClass::Cargo);
        assert_eq!(default_classification("204").1, EventClass::Cargo);
        assert_eq!(default_classification("301").1, EventClass::Break);
        assert_eq!(default_classification("302").1, EventClass::RestSplit);
        assert_eq!(default_classification("999").1, EventClass::Ignore);
    }

    #[test]
    fn collect_break_intervals_filters_by_event_cd_and_range() {
        let events = vec![
            ev("301", dt((2024, 4, 15), (10, 0, 0)), Some(30)),
            ev("301", dt((2024, 4, 15), (14, 0, 0)), Some(15)),
            ev("301", dt((2024, 4, 15), (20, 0, 0)), Some(60)), // 範囲外
            ev("302", dt((2024, 4, 15), (12, 0, 0)), Some(540)), // event_cd 違い
        ];
        let intervals = collect_break_intervals(
            &events,
            dt((2024, 4, 15), (8, 0, 0)),
            dt((2024, 4, 15), (18, 0, 0)),
        );
        assert_eq!(intervals.len(), 2);
        let total: i32 = intervals
            .iter()
            .map(|(s, e)| (*e - *s).num_minutes() as i32)
            .sum();
        assert_eq!(total, 30 + 15);
    }

    #[test]
    fn build_segment_inputs_with_no_rest_split_returns_one_segment() {
        // 8:00 〜 17:00、間に 301 が 60 分
        let events = vec![
            ev("201", dt((2024, 4, 15), (8, 0, 0)), Some(120)),
            ev("301", dt((2024, 4, 15), (12, 0, 0)), Some(60)),
            ev("201", dt((2024, 4, 15), (13, 0, 0)), Some(240)),
        ];
        let segs = build_segment_inputs(
            &events,
            dt((2024, 4, 15), (8, 0, 0)),
            dt((2024, 4, 15), (17, 0, 0)),
        );
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].rest_minutes, 60);
        assert_eq!(segs[0].start, dt((2024, 4, 15), (8, 0, 0)));
    }

    #[test]
    fn build_segment_inputs_splits_at_long_rest() {
        // 4/15 22:00 〜 4/16 09:00 だが、間に 302 (休息 600 分) が 4/15 23:00〜4/16 09:00
        let events = vec![
            ev("201", dt((2024, 4, 15), (22, 0, 0)), Some(60)),
            ev(
                "302",
                dt((2024, 4, 15), (23, 0, 0)),
                Some(600), // 4/15 23:00 → 4/16 09:00
            ),
            ev("201", dt((2024, 4, 16), (9, 0, 0)), Some(60)),
        ];
        let segs = build_segment_inputs(
            &events,
            dt((2024, 4, 15), (22, 0, 0)),
            dt((2024, 4, 16), (10, 0, 0)),
        );
        // 22:00〜23:00 と 9:00〜10:00 (actual_end は events 最終終了時刻 4/16 10:00) に分割
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].start, dt((2024, 4, 15), (22, 0, 0)));
        assert_eq!(segs[0].end, dt((2024, 4, 15), (23, 0, 0)));
        assert_eq!(segs[1].start, dt((2024, 4, 16), (9, 0, 0)));
    }

    /// 長さが 0 以下の休憩と、区間の終わりちょうどに始まる休憩は数えない
    /// (alc-dtako-worker で足したテスト。rust では mock の結合テストが通していた分岐)。
    #[test]
    fn collect_break_intervals_skips_zero_length_and_clipped_to_nothing() {
        let events = vec![
            ev("301", dt((2024, 4, 15), (10, 0, 0)), Some(0)),
            ev("301", dt((2024, 4, 15), (11, 0, 0)), None),
            ev("301", dt((2024, 4, 15), (18, 0, 0)), Some(30)),
        ];
        let intervals = collect_break_intervals(
            &events,
            dt((2024, 4, 15), (8, 0, 0)),
            dt((2024, 4, 15), (18, 0, 0)),
        );
        assert!(intervals.is_empty(), "{intervals:?}");
    }
}
