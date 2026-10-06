use chrono::{NaiveDate, NaiveDateTime, Timelike};
use std::collections::HashMap;

use super::kudgivt::KudgivtRow;

/// 改善基準告示の休息基準（分）
const REST_THRESHOLD_PRINCIPAL: i32 = 540; // 原則: 連続540分以上
const REST_SPLIT_MIN: i32 = 180; // 分割特例: 1回180分以上
const REST_SPLIT_2_TOTAL: i32 = 600; // 2分割: 合計600分以上
const REST_SPLIT_3_TOTAL: i32 = 720; // 3分割: 合計720分以上
const MAX_WORK_HOURS: i64 = 24 * 60; // 24時間ルール（分）

/// 1つの勤務日（始業〜終業）
#[derive(Debug, Clone)]
pub struct Workday {
    pub start: NaiveDateTime, // 始業
    pub end: NaiveDateTime,   // 終業
    pub date: NaiveDate,      // 帰属日 = start.date()
}

/// ドライバーの全302イベントから勤務日（始業〜終業）を決定する
///
/// ルール（改善基準告示 令和6年4月）:
/// 1. 休息基準を満たした場合 → 休息開始で終業、休息終了後の次の拘束開始で新規始業
///    - [原則] 連続540分以上
///    - [分割特例] 1回180分以上の休息の累計が 2分割=600分 / 3分割=720分
/// 2. 始業から24h経過で休息基準未達 → 強制日締め
///
/// - `rest_events`: 302イベント（時系列ソート済み）
/// - `first_start`: 最初の拘束開始（出社日時等）
/// - `last_end`: 最後の拘束終了
/// - `is_long_distance`: 宿泊を伴う長距離貨物運送（例外基準: 480分）
pub fn determine_workdays(
    rest_events: &[(NaiveDateTime, i32)], // (start_at, duration_minutes)
    first_start: NaiveDateTime,
    last_end: NaiveDateTime,
    is_long_distance: bool,
) -> Vec<Workday> {
    let mut workdays = Vec::new();
    let mut current_start = first_start;
    let mut split_rests: Vec<i32> = Vec::new(); // 分割特例用: 180分以上の休息を蓄積
    for &(rest_start, rest_duration) in rest_events {
        let rest_end = rest_start + chrono::Duration::minutes(rest_duration as i64);

        // 24時間ルール: 始業から24h経過していたら強制日締め（複数回分割の可能性）
        let mut handled_by_24h = false;
        loop {
            let max_end = current_start + chrono::Duration::minutes(MAX_WORK_HOURS);
            if rest_start >= max_end {
                // 休息開始が24h後より後 → 24h境界で強制分割
                workdays.push(Workday {
                    start: current_start,
                    end: max_end,
                    date: current_start.date(),
                });
                current_start = max_end;
                split_rests.clear();
            } else if rest_start < max_end && rest_end > max_end {
                // 24hマークが休息の途中に落ちる場合:
                // 「始業から24時間後が休息中なら休息の開始が終業になる」
                // 休息終了が新しい始業
                workdays.push(Workday {
                    start: current_start,
                    end: rest_start,
                    date: current_start.date(),
                });
                current_start = rest_end;
                split_rests.clear();
                handled_by_24h = true;
                break;
            } else {
                break;
            }
        }
        if handled_by_24h {
            continue;
        }

        // 原則: 連続540分以上
        // 長距離例外: 最後の休息のみ480分以上（運行終了後の休息基準）
        let is_last_rest = rest_events
            .last()
            .map(|&(s, _)| s == rest_start)
            .unwrap_or(false);
        let threshold = if is_long_distance && is_last_rest {
            480
        } else {
            REST_THRESHOLD_PRINCIPAL
        };
        if rest_duration >= threshold {
            workdays.push(Workday {
                start: current_start,
                end: rest_start,
                date: current_start.date(),
            });
            current_start = rest_end;
            split_rests.clear();
            continue;
        }

        // 分割特例: 180分以上の休息を蓄積してチェック
        if rest_duration < REST_SPLIT_MIN {
            continue;
        }
        split_rests.push(rest_duration);
        let total: i32 = split_rests.iter().sum();
        let threshold = match split_rests.len() {
            2 => REST_SPLIT_2_TOTAL,
            n if n >= 3 => REST_SPLIT_3_TOTAL,
            _ => i32::MAX, // 1回だけでは分割特例不成立
        };
        if total >= threshold {
            workdays.push(Workday {
                start: current_start,
                end: rest_start,
                date: current_start.date(),
            });
            current_start = rest_end;
            split_rests.clear();
        }
    }

    // 最後の勤務日（24hルールで複数日に分割される可能性あり）
    while current_start < last_end {
        let max_end = current_start + chrono::Duration::minutes(MAX_WORK_HOURS);
        if last_end > max_end {
            workdays.push(Workday {
                start: current_start,
                end: max_end,
                date: current_start.date(),
            });
            current_start = max_end;
        } else {
            workdays.push(Workday {
                start: current_start,
                end: last_end,
                date: current_start.date(),
            });
            break;
        }
    }

    workdays
}

/// イベント分類
#[derive(Debug, Clone, PartialEq)]
pub enum EventClass {
    Drive,     // 運転 (110)
    Cargo,     // 荷役 (202=積み, 203=降し)
    RestSplit, // 勤務区間の区切り (302=休息)
    Break,     // 拘束内だが労働時間外 (301=休憩)
    Ignore,    // 無視 (101=実車, 103=高速道, 412=アイドリング等)
}

impl EventClass {
    /// 分類の文字列 (DB の `classification` 列の値) → 分類。知らない文字列は `Ignore`。
    pub fn from_classification_str(classification: &str) -> Self {
        match classification {
            "drive" => EventClass::Drive,
            "cargo" => EventClass::Cargo,
            "work" => EventClass::Drive, // legacy fallback
            "rest_split" => EventClass::RestSplit,
            "break" => EventClass::Break,
            _ => EventClass::Ignore,
        }
    }
}

/// イベントCD の既定の分類 → (分類の文字列, 分類)。未登録のコードは `("ignore", Ignore)`。
///
/// 既定の分類の表はこの 1 つ (backend の `alc-dtako`・`alc-compare` と、分割 worker が同じものを呼ぶ)。
pub fn default_classification(event_cd: &str) -> (&'static str, EventClass) {
    match event_cd {
        "201" => ("drive", EventClass::Drive),          // 走行(運転)
        "202" => ("cargo", EventClass::Cargo),          // 積み
        "203" => ("cargo", EventClass::Cargo),          // 降し
        "204" => ("cargo", EventClass::Cargo),          // その他 → 荷役
        "302" => ("rest_split", EventClass::RestSplit), // 休息
        "301" => ("break", EventClass::Break),          // 休憩
        _ => ("ignore", EventClass::Ignore),            // その他は無視
    }
}

/// 1つの連続勤務区間
#[derive(Debug, Clone)]
pub struct WorkSegment {
    pub start: NaiveDateTime,
    pub end: NaiveDateTime,
    pub labor_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
}

/// 日別に分割された勤務区間
#[derive(Debug, Clone)]
pub struct DailyWorkSegment {
    pub date: NaiveDate,
    pub start: NaiveDateTime,
    pub end: NaiveDateTime,
    pub work_minutes: i32,
    pub labor_minutes: i32,
    pub late_night_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
}

/// day_start〜day_end 間の深夜時間（22:00〜翌5:00）を分単位で返す
/// 日跨ぎ対応: 0:00境界で分割して各日の深夜時間を合算する
pub fn calc_late_night_mins(day_start: NaiveDateTime, day_end: NaiveDateTime) -> i32 {
    // 同一日 or ちょうど翌日0:00 → 単一日ロジック
    if day_end.date() == day_start.date()
        || (day_end.date() == day_start.date().succ_opt().unwrap()
            && day_end.hour() == 0
            && day_end.minute() == 0)
    {
        return calc_late_night_single_day(day_start, day_end);
    }
    // 日跨ぎ: 0:00境界で分割して合算
    let mut total = 0i32;
    let mut cur = day_start;
    while cur.date() < day_end.date() {
        let midnight = cur.date().succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap();
        total += calc_late_night_single_day(cur, midnight);
        cur = midnight;
    }
    total += calc_late_night_single_day(cur, day_end);
    total
}

/// 同一日内（day_endが翌日0:00含む）の深夜時間を計算
fn calc_late_night_single_day(day_start: NaiveDateTime, day_end: NaiveDateTime) -> i32 {
    let mut total = 0i32;
    let start_h = day_start.hour() * 60 + day_start.minute();
    let end_h = if day_end.date() > day_start.date() && day_end.hour() == 0 && day_end.minute() == 0
    {
        1440u32
    } else {
        day_end.hour() * 60 + day_end.minute()
    };
    // 0:00〜5:00 (0〜300分)
    let early_start = start_h;
    let early_end = end_h.min(300);
    if early_end > early_start {
        total += (early_end - early_start) as i32;
    }
    // 22:00〜24:00 (1320〜1440分)
    let late_start = start_h.max(1320);
    let late_end = end_h.min(1440);
    if late_end > late_start {
        total += (late_end - late_start) as i32;
    }
    total
}

/// KUDGIVT イベント列と分類マップから、KUDGURI 1運行を勤務区間に分割する
///
/// - `departure_at`: 出社日時 (KUDGURI)
/// - `return_at`: 退社日時 (KUDGURI)
/// - `events`: この運行の全KUDGIVTイベント
/// - `classifications`: event_cd → EventClass のマップ
pub fn split_by_rest(
    departure_at: NaiveDateTime,
    return_at: NaiveDateTime,
    events: &[&KudgivtRow],
    classifications: &HashMap<String, EventClass>,
) -> Vec<WorkSegment> {
    // 休息(rest_split)イベントを start_at 昇順でソート
    let mut rest_events: Vec<&&KudgivtRow> = events
        .iter()
        .filter(|e| {
            classifications
                .get(&e.event_cd)
                .map(|c| *c == EventClass::RestSplit)
                .unwrap_or(false)
        })
        .collect();
    rest_events.sort_by_key(|e| e.start_at);

    // 労働(drive/cargo)イベントを start_at 昇順でソート
    let mut labor_events: Vec<&&KudgivtRow> = events
        .iter()
        .filter(|e| {
            classifications
                .get(&e.event_cd)
                .map(|c| *c == EventClass::Drive || *c == EventClass::Cargo)
                .unwrap_or(false)
        })
        .collect();
    labor_events.sort_by_key(|e| e.start_at);

    // 実際の終了時刻 = イベントの最終終了時刻（なければreturn_at）
    let actual_end = events
        .iter()
        .map(|e| {
            let dur = e.duration_minutes.unwrap_or(0);
            if dur > 0 {
                e.start_at + chrono::Duration::minutes(dur as i64)
            } else {
                // duration=0 のイベント（運行開始/終了等）は start_at を使う
                e.start_at
            }
        })
        .max()
        .unwrap_or(return_at);

    let mut segments = Vec::new();
    let mut current_start = departure_at;

    for rest in &rest_events {
        let rest_start = rest.start_at;
        let duration = rest.duration_minutes.unwrap_or(0);
        let rest_end = rest_start + chrono::Duration::minutes(duration as i64);

        if rest_start > current_start {
            let (drive, cargo) =
                sum_events_in_range(&labor_events, classifications, current_start, rest_start);
            segments.push(WorkSegment {
                start: current_start,
                end: rest_start,
                labor_minutes: drive + cargo,
                drive_minutes: drive,
                cargo_minutes: cargo,
            });
        }

        current_start = rest_end.min(actual_end);
    }

    // 最後の区間
    if current_start < actual_end {
        let (drive, cargo) =
            sum_events_in_range(&labor_events, classifications, current_start, actual_end);
        segments.push(WorkSegment {
            start: current_start,
            end: actual_end,
            labor_minutes: drive + cargo,
            drive_minutes: drive,
            cargo_minutes: cargo,
        });
    }

    segments
}

/// 24時間超のセグメントを24h境界で強制分割する（休息未取得時例外）
/// 改善基準告示: 集計開始時刻の24時間後を日締め時刻とする
///
/// workday_ends: determine_workdaysのwd.end一覧（始業基準の24h境界）
/// workday_endsがある場合、seg基準ではなく始業基準で分割する
pub fn split_segments_at_24h_with_workdays(
    segments: Vec<WorkSegment>,
    workday_ends: &[NaiveDateTime],
) -> Vec<WorkSegment> {
    let max_mins = 24 * 60i64;
    let mut result = Vec::new();
    for seg in segments {
        let total_mins = (seg.end - seg.start).num_minutes();
        // workday境界がセグメント内にあれば分割（24h未満でも）
        // 境界は分単位に切り捨て済みなので、seg側も分単位で比較
        let seg_start_trunc = seg.start.with_second(0).unwrap_or(seg.start);
        let seg_end_trunc = seg.end.with_second(0).unwrap_or(seg.end);
        let wd_boundaries: Vec<NaiveDateTime> = workday_ends
            .iter()
            .filter(|&&b| b > seg_start_trunc && b < seg_end_trunc)
            .copied()
            .collect();
        if total_mins <= max_mins && wd_boundaries.is_empty() {
            result.push(seg);
            continue;
        }
        let mut boundaries = wd_boundaries;
        if boundaries.is_empty() {
            // workday境界がない場合はseg基準で24h分割（従来動作）
            let mut cur_start = seg.start;
            while cur_start < seg.end {
                let cur_end = (cur_start + chrono::Duration::minutes(max_mins)).min(seg.end);
                boundaries.push(cur_end);
                cur_start = cur_end;
            }
            boundaries.pop(); // seg.endは不要
        }
        boundaries.sort();

        let total_labor = seg.labor_minutes as f64;
        let total_drive = seg.drive_minutes as f64;
        let total_cargo = seg.cargo_minutes as f64;
        let total_wall = total_mins as f64;
        let mut cur_start = seg.start;
        for boundary in boundaries.iter().chain(std::iter::once(&seg.end)) {
            if *boundary <= cur_start {
                continue;
            }
            let cur_end = *boundary;
            let chunk_mins = (cur_end - cur_start).num_minutes() as f64;
            let ratio = chunk_mins / total_wall;
            result.push(WorkSegment {
                start: cur_start,
                end: cur_end,
                labor_minutes: (total_labor * ratio).round() as i32,
                drive_minutes: (total_drive * ratio).round() as i32,
                cargo_minutes: (total_cargo * ratio).round() as i32,
            });
            cur_start = cur_end;
        }
    }
    result
}

/// 互換ラッパー: workday境界なしの24h分割
pub fn split_segments_at_24h(segments: Vec<WorkSegment>) -> Vec<WorkSegment> {
    split_segments_at_24h_with_workdays(segments, &[])
}

/// 指定範囲内のイベントを運転/荷役に分けて duration_minutes を合計
pub fn sum_events_in_range(
    events: &[&&KudgivtRow],
    classifications: &HashMap<String, EventClass>,
    range_start: NaiveDateTime,
    range_end: NaiveDateTime,
) -> (i32, i32) {
    let mut drive = 0i32;
    let mut cargo = 0i32;
    for e in events
        .iter()
        .filter(|e| e.start_at >= range_start && e.start_at < range_end)
    {
        let dur = e.duration_minutes.unwrap_or(0);
        match classifications.get(&e.event_cd) {
            Some(EventClass::Drive) => drive += dur,
            Some(EventClass::Cargo) => cargo += dur,
            _ => {}
        }
    }
    (drive, cargo)
}

/// 勤務区間を0:00境界で日別に分割する
pub fn split_segments_by_day(segments: &[WorkSegment]) -> Vec<DailyWorkSegment> {
    let mut daily = Vec::new();

    for seg in segments {
        let mut current = seg.start.date();
        let end_date = seg.end.date();
        // 秒を切り捨ててHH:MM精度に揃える（web地球号互換）
        let start_trunc = seg.start.with_second(0).unwrap_or(seg.start);
        let end_trunc = seg.end.with_second(0).unwrap_or(seg.end);
        let total_work_mins = (end_trunc - start_trunc).num_minutes().max(1) as f64;

        while current <= end_date {
            let day_start = if current == seg.start.date() {
                seg.start
            } else {
                current.and_hms_opt(0, 0, 0).unwrap()
            };
            let day_end = if current == end_date {
                seg.end
            } else {
                (current + chrono::Duration::days(1))
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            };

            // 秒を切り捨ててHH:MM精度に揃える（web地球号互換）
            let day_start_trunc = day_start.with_second(0).unwrap_or(day_start);
            let day_end_trunc = day_end.with_second(0).unwrap_or(day_end);
            let work_mins = (day_end_trunc - day_start_trunc).num_minutes() as i32;
            if work_mins <= 0 {
                current += chrono::Duration::days(1);
                continue;
            }

            let ratio = work_mins as f64 / total_work_mins;
            let labor_mins = (seg.labor_minutes as f64 * ratio).round() as i32;
            let drive_mins = (seg.drive_minutes as f64 * ratio).round() as i32;
            let cargo_mins = (seg.cargo_minutes as f64 * ratio).round() as i32;
            let late_night = calc_late_night_mins(day_start, day_end);

            daily.push(DailyWorkSegment {
                date: current,
                start: day_start,
                end: day_end,
                work_minutes: work_mins,
                labor_minutes: labor_mins,
                late_night_minutes: late_night,
                drive_minutes: drive_mins,
                cargo_minutes: cargo_mins,
            });

            current += chrono::Duration::days(1);
        }
    }

    daily
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn make_classifications() -> HashMap<String, EventClass> {
        let mut m = HashMap::new();
        m.insert("110".to_string(), EventClass::Drive);
        m.insert("202".to_string(), EventClass::Cargo);
        m.insert("203".to_string(), EventClass::Cargo);
        m.insert("302".to_string(), EventClass::RestSplit);
        m.insert("301".to_string(), EventClass::Break);
        m.insert("101".to_string(), EventClass::Ignore);
        m.insert("103".to_string(), EventClass::Ignore);
        m.insert("412".to_string(), EventClass::Ignore);
        m
    }

    fn make_event(
        unko_no: &str,
        start_at: NaiveDateTime,
        event_cd: &str,
        duration: Option<i32>,
    ) -> KudgivtRow {
        KudgivtRow {
            unko_no: unko_no.to_string(),
            reading_date: NaiveDate::from_ymd_opt(2026, 2, 27).unwrap(),
            driver_cd: "2".to_string(),
            driver_name: "テスト".to_string(),
            crew_role: 1,
            start_at,
            end_at: duration.map(|d| start_at + chrono::Duration::minutes(d as i64)),
            event_cd: event_cd.to_string(),
            event_name: "test".to_string(),
            duration_minutes: duration,
            section_distance: None,
        }
    }

    #[test]
    fn test_default_classification() {
        test_group!("イベント分類");
        test_case!("既定の分類: 6 コードと未登録", {
            assert_eq!(default_classification("201"), ("drive", EventClass::Drive));
            assert_eq!(default_classification("202"), ("cargo", EventClass::Cargo));
            assert_eq!(default_classification("203"), ("cargo", EventClass::Cargo));
            assert_eq!(default_classification("204"), ("cargo", EventClass::Cargo));
            assert_eq!(
                default_classification("302"),
                ("rest_split", EventClass::RestSplit)
            );
            assert_eq!(default_classification("301"), ("break", EventClass::Break));
            assert_eq!(
                default_classification("110"),
                ("ignore", EventClass::Ignore)
            );
            assert_eq!(default_classification(""), ("ignore", EventClass::Ignore));
        });
    }

    #[test]
    fn test_event_class_from_classification_str() {
        test_group!("イベント分類");
        test_case!(
            "分類の文字列 → 分類 (work は旧い値で Drive、未知は Ignore)",
            {
                let from = EventClass::from_classification_str;
                assert_eq!(from("drive"), EventClass::Drive);
                assert_eq!(from("cargo"), EventClass::Cargo);
                assert_eq!(from("work"), EventClass::Drive);
                assert_eq!(from("rest_split"), EventClass::RestSplit);
                assert_eq!(from("break"), EventClass::Break);
                assert_eq!(from("ignore"), EventClass::Ignore);
                assert_eq!(from("DRIVE"), EventClass::Ignore);
                assert_eq!(from(""), EventClass::Ignore);
            }
        );
        test_case!("既定の分類の文字列は、同じ分類に戻る", {
            for cd in ["201", "202", "203", "204", "302", "301", "999"] {
                let (s, class) = default_classification(cd);
                assert_eq!(EventClass::from_classification_str(s), class, "{cd}");
            }
        });
    }

    fn dt(y: i32, m: u32, d: u32, h: u32, mi: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, mi, 0)
            .unwrap()
    }

    #[test]
    fn test_no_rest_events_single_segment() {
        test_group!("CSVパーサー");
        test_case!("休息なし単一区間", {
            let dep = dt(2026, 2, 24, 10, 0);
            let ret = dt(2026, 2, 24, 18, 0);
            let events = vec![make_event("001", dt(2026, 2, 24, 10, 0), "110", Some(300))];
            let refs: Vec<&KudgivtRow> = events.iter().collect();
            let cls = make_classifications();

            let segments = split_by_rest(dep, ret, &refs, &cls);
            assert_eq!(segments.len(), 1);
            assert_eq!(segments[0].start, dep);
            // actual_end = イベント終了時刻 (10:00 + 300min = 15:00)
            assert_eq!(segments[0].end, dt(2026, 2, 24, 15, 0));
            assert_eq!(segments[0].labor_minutes, 300);
        });
    }

    #[test]
    fn test_single_rest_splits_into_two() {
        test_group!("CSVパーサー");
        test_case!("単一休息で2区間に分割", {
            let dep = dt(2026, 2, 24, 10, 0);
            let ret = dt(2026, 2, 25, 18, 0);
            let events = vec![
                make_event("001", dt(2026, 2, 24, 10, 0), "110", Some(240)), // 運転 4h
                make_event("001", dt(2026, 2, 24, 14, 0), "302", Some(600)), // 休息 10h
                make_event("001", dt(2026, 2, 25, 0, 0), "110", Some(480)),  // 運転 8h
            ];
            let refs: Vec<&KudgivtRow> = events.iter().collect();
            let cls = make_classifications();

            let segments = split_by_rest(dep, ret, &refs, &cls);
            assert_eq!(segments.len(), 2);
            // 区間1: 10:00 → 14:00
            assert_eq!(segments[0].start, dt(2026, 2, 24, 10, 0));
            assert_eq!(segments[0].end, dt(2026, 2, 24, 14, 0));
            assert_eq!(segments[0].labor_minutes, 240);
            // 区間2: 00:00(休息終了) → 08:00 (0:00 + 480min)
            assert_eq!(segments[1].start, dt(2026, 2, 25, 0, 0));
            assert_eq!(segments[1].end, dt(2026, 2, 25, 8, 0));
            assert_eq!(segments[1].labor_minutes, 480);
        });
    }

    #[test]
    fn test_multi_day_operation_with_real_data() {
        test_group!("CSVパーサー");
        test_case!("複数日運行の実データパース", {
            // 2/24 10:13出社 → 2/27 16:00退社
            let dep = dt(2026, 2, 24, 10, 13);
            let ret = dt(2026, 2, 27, 16, 0);
            let events = vec![
                make_event("001", dt(2026, 2, 24, 10, 25), "110", Some(324)), // 運転
                make_event("001", dt(2026, 2, 24, 14, 40), "302", Some(1123)), // 休息 ~18.7h
                make_event("001", dt(2026, 2, 25, 9, 30), "110", Some(200)),  // 運転
                make_event("001", dt(2026, 2, 25, 21, 31), "302", Some(780)), // 休息 13h
                make_event("001", dt(2026, 2, 26, 10, 30), "110", Some(300)), // 運転
                make_event("001", dt(2026, 2, 26, 21, 25), "302", Some(572)), // 休息 ~9.5h
                make_event("001", dt(2026, 2, 27, 7, 0), "110", Some(400)),   // 運転
            ];
            let refs: Vec<&KudgivtRow> = events.iter().collect();
            let cls = make_classifications();

            let segments = split_by_rest(dep, ret, &refs, &cls);
            assert_eq!(segments.len(), 4);

            // 区間1: 10:13 → 14:40 (4h27m)
            assert_eq!(segments[0].start, dt(2026, 2, 24, 10, 13));
            assert_eq!(segments[0].end, dt(2026, 2, 24, 14, 40));

            // 区間2: 14:40 + 1123min = ~09:23翌日 → 21:31
            // 1123 min = 18h43m → 14:40 + 18:43 = 2/25 09:23
            assert_eq!(segments[1].end, dt(2026, 2, 25, 21, 31));

            // 区間3: 21:31 + 780min = ~10:31翌日 → 21:25
            assert_eq!(segments[2].end, dt(2026, 2, 26, 21, 25));

            // 区間4: 7:00 + 400min = 13:40
            assert_eq!(segments[3].end, dt(2026, 2, 27, 13, 40));

            // 拘束時間は24時間にはならない
            for seg in &segments {
                let mins = (seg.end - seg.start).num_minutes();
                assert!(mins < 24 * 60, "segment should be < 24h, got {}min", mins);
            }
        });
    }

    #[test]
    fn test_split_segments_by_day() {
        test_group!("CSVパーサー");
        test_case!("日別分割と深夜時間計算", {
            let segments = vec![WorkSegment {
                start: dt(2026, 2, 24, 22, 0),
                end: dt(2026, 2, 25, 6, 0),
                labor_minutes: 400,
                drive_minutes: 300,
                cargo_minutes: 100,
            }];

            let daily = split_segments_by_day(&segments);
            assert_eq!(daily.len(), 2);

            // Day 1: 22:00 → 00:00 = 120min
            assert_eq!(daily[0].date, NaiveDate::from_ymd_opt(2026, 2, 24).unwrap());
            assert_eq!(daily[0].work_minutes, 120);
            assert_eq!(daily[0].late_night_minutes, 120); // 22:00-24:00 is all late night

            // Day 2: 00:00 → 06:00 = 360min
            assert_eq!(daily[1].date, NaiveDate::from_ymd_opt(2026, 2, 25).unwrap());
            assert_eq!(daily[1].work_minutes, 360);
            assert_eq!(daily[1].late_night_minutes, 300); // 00:00-05:00

            // labor按分: 120/480*400=100, 360/480*400=300
            assert_eq!(daily[0].labor_minutes, 100);
            assert_eq!(daily[1].labor_minutes, 300);
        });
    }

    #[test]
    fn test_calc_late_night_mins() {
        test_group!("CSVパーサー");
        test_case!("深夜時間計算", {
            // 22:00〜翌05:00 の全深夜帯
            assert_eq!(
                calc_late_night_mins(dt(2026, 1, 1, 22, 0), dt(2026, 1, 1, 23, 30),),
                90
            );

            // 0:00〜5:00
            assert_eq!(
                calc_late_night_mins(dt(2026, 1, 1, 0, 0), dt(2026, 1, 1, 5, 0),),
                300
            );

            // 昼間のみ
            assert_eq!(
                calc_late_night_mins(dt(2026, 1, 1, 8, 0), dt(2026, 1, 1, 17, 0),),
                0
            );
        });
    }

    #[test]
    fn test_24h_mark_during_rest() {
        test_group!("CSVパーサー");
        test_case!("24hマークが休息中に発生", {
            // 始業: 2/21 08:30
            // 休息: 2/22 06:00〜15:00 (540min)
            // 24hマーク: 2/22 08:30 → 休息の途中
            // 期待: workday1 ends at 06:00 (rest_start), workday2 starts at 15:00 (rest_end)
            let rest_events = vec![(dt(2026, 2, 22, 6, 0), 540)];
            let first_start = dt(2026, 2, 21, 8, 30);
            let last_end = dt(2026, 2, 22, 20, 0);
            let workdays = determine_workdays(&rest_events, first_start, last_end, false);
            assert_eq!(workdays.len(), 2);
            assert_eq!(workdays[0].start, dt(2026, 2, 21, 8, 30));
            assert_eq!(workdays[0].end, dt(2026, 2, 22, 6, 0)); // 休息開始 = 終業
            assert_eq!(workdays[1].start, dt(2026, 2, 22, 15, 0)); // 休息終了 = 始業
            assert_eq!(workdays[1].end, dt(2026, 2, 22, 20, 0));
        });
    }

    #[test]
    fn test_24h_mark_during_short_rest() {
        test_group!("CSVパーサー");
        test_case!("24hマークが短い休息中に発生", {
            // 始業: 2/21 08:30
            // 休息: 2/22 07:00〜11:00 (240min, < 540min)
            // 24hマーク: 2/22 08:30 → 休息の途中
            // 短い休息でも24hルールで日締めされる
            let rest_events = vec![(dt(2026, 2, 22, 7, 0), 240)];
            let first_start = dt(2026, 2, 21, 8, 30);
            let last_end = dt(2026, 2, 22, 20, 0);
            let workdays = determine_workdays(&rest_events, first_start, last_end, false);
            assert_eq!(workdays.len(), 2);
            assert_eq!(workdays[0].end, dt(2026, 2, 22, 7, 0)); // 休息開始 = 終業
            assert_eq!(workdays[1].start, dt(2026, 2, 22, 11, 0)); // 休息終了 = 始業
        });
    }

    #[test]
    fn test_24h_mark_after_short_rest_no_split() {
        test_group!("CSVパーサー");
        test_case!("24hマーク前に休息終了で分割なし", {
            // 1039ケース: 383min休息が24hマーク前に終了 → 新ルール不発動
            // 始業: 2/21 08:30
            // 休息: 2/21 23:17〜2/22 05:40 (383min)
            // 24hマーク: 2/22 08:30 → 休息は05:40に終了済み
            let rest_events = vec![(dt(2026, 2, 21, 23, 17), 383)];
            let first_start = dt(2026, 2, 21, 8, 30);
            let last_end = dt(2026, 2, 22, 20, 20);
            let workdays = determine_workdays(&rest_events, first_start, last_end, false);
            // 383min < 540min → 休息による分割なし
            // 24hルールで08:30に強制分割
            assert_eq!(workdays.len(), 2);
            assert_eq!(workdays[0].end, dt(2026, 2, 22, 8, 30)); // 24h境界
            assert_eq!(workdays[1].start, dt(2026, 2, 22, 8, 30)); // 24h境界から開始
        });
    }

    #[test]
    fn test_rest_after_24h_boundary_forced_split() {
        test_group!("CSVパーサー");
        test_case!("休息開始が24h後より後の場合の強制分割", {
            // 始業: 2/21 08:00
            // 休息: 2/22 10:00〜20:00 (600min) — 24hマーク(2/22 08:00)の後
            // 期待: 24h境界で強制分割 → workday1: 08:00〜翌08:00, workday2: 翌08:00〜10:00, workday3: 20:00〜...
            let rest_events = vec![(dt(2026, 2, 22, 10, 0), 600)];
            let first_start = dt(2026, 2, 21, 8, 0);
            let last_end = dt(2026, 2, 23, 6, 0);
            let workdays = determine_workdays(&rest_events, first_start, last_end, false);
            // First: forced at 24h boundary (2/22 08:00)
            assert_eq!(workdays[0].start, dt(2026, 2, 21, 8, 0));
            assert_eq!(workdays[0].end, dt(2026, 2, 22, 8, 0));
            assert!(workdays.len() >= 2);
        });
    }

    #[test]
    fn test_split_rest_2_total_600() {
        test_group!("CSVパーサー");
        test_case!("2分割特例: 180分以上×2の合計600分以上", {
            // 始業: 2/21 06:00
            // 休息1: 2/21 12:00 (300min = 5h) → split_rests = [300]
            // 休息2: 2/21 22:00 (300min = 5h) → split_rests = [300, 300], total=600 >= 600 → 日締め
            let rest_events = vec![(dt(2026, 2, 21, 12, 0), 300), (dt(2026, 2, 21, 22, 0), 300)];
            let first_start = dt(2026, 2, 21, 6, 0);
            let last_end = dt(2026, 2, 22, 12, 0);
            let workdays = determine_workdays(&rest_events, first_start, last_end, false);
            assert_eq!(workdays.len(), 2);
            assert_eq!(workdays[0].start, dt(2026, 2, 21, 6, 0));
            assert_eq!(workdays[0].end, dt(2026, 2, 21, 22, 0)); // 2回目の休息開始で日締め
            assert_eq!(workdays[1].start, dt(2026, 2, 22, 3, 0)); // 22:00 + 300min = 翌03:00
        });
    }

    #[test]
    fn test_split_rest_3_total_720() {
        test_group!("CSVパーサー");
        test_case!("3分割特例: 180分以上×3の合計720分以上", {
            // 3回の休息: 各240min、合計720 >= 720
            let rest_events = vec![
                (dt(2026, 2, 21, 10, 0), 240),
                (dt(2026, 2, 21, 18, 0), 240),
                (dt(2026, 2, 22, 2, 0), 240),
            ];
            let first_start = dt(2026, 2, 21, 6, 0);
            let last_end = dt(2026, 2, 22, 12, 0);
            let workdays = determine_workdays(&rest_events, first_start, last_end, false);
            assert_eq!(workdays.len(), 2);
            assert_eq!(workdays[0].end, dt(2026, 2, 22, 2, 0)); // 3回目の休息開始で日締め
        });
    }

    #[test]
    fn test_long_distance_last_rest_480() {
        test_group!("CSVパーサー");
        test_case!("長距離貨物: 最後の休息480分で日締め", {
            let rest_events = vec![(dt(2026, 2, 21, 14, 0), 490)];
            let first_start = dt(2026, 2, 21, 6, 0);
            let last_end = dt(2026, 2, 22, 6, 0);
            // is_long_distance=true → 最後の休息は480分で日締め (490 >= 480)
            let workdays = determine_workdays(&rest_events, first_start, last_end, true);
            assert_eq!(workdays.len(), 2);
            assert_eq!(workdays[0].end, dt(2026, 2, 21, 14, 0));
        });
    }

    #[test]
    fn test_event_duration_zero_uses_start_at() {
        test_group!("CSVパーサー");
        test_case!("duration=0のイベントはstart_atを使用", {
            let dep = dt(2026, 2, 24, 10, 0);
            let ret = dt(2026, 2, 24, 18, 0);
            // duration=0 のイベント (e.g. 運行開始)
            let events = vec![
                make_event("001", dt(2026, 2, 24, 10, 0), "101", Some(0)), // Ignore, duration=0
                make_event("001", dt(2026, 2, 24, 10, 30), "110", Some(120)), // Drive 2h
            ];
            let refs: Vec<&KudgivtRow> = events.iter().collect();
            let cls = make_classifications();
            let segments = split_by_rest(dep, ret, &refs, &cls);
            assert_eq!(segments.len(), 1);
            // actual_end = max(10:00+0=10:00, 10:30+120=12:30) = 12:30
            assert_eq!(segments[0].end, dt(2026, 2, 24, 12, 30));
        });
    }

    #[test]
    fn test_split_segments_at_24h_no_workdays() {
        test_group!("CSVパーサー");
        test_case!("24h超セグメントのworkday境界なし分割", {
            // 25時間のセグメント → 24h + 1h に分割
            let segments = vec![WorkSegment {
                start: dt(2026, 2, 24, 8, 0),
                end: dt(2026, 2, 25, 9, 0), // 25h
                labor_minutes: 1000,
                drive_minutes: 600,
                cargo_minutes: 400,
            }];
            let result = split_segments_at_24h(segments);
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].start, dt(2026, 2, 24, 8, 0));
            assert_eq!(result[0].end, dt(2026, 2, 25, 8, 0)); // 24h
            assert_eq!(result[1].start, dt(2026, 2, 25, 8, 0));
            assert_eq!(result[1].end, dt(2026, 2, 25, 9, 0)); // 1h
                                                              // 按分チェック
            assert!(result[0].labor_minutes > result[1].labor_minutes);
        });
    }

    #[test]
    fn test_sum_events_unknown_class() {
        test_group!("CSVパーサー");
        test_case!("未知のevent_cdは無視される", {
            let events = vec![
                make_event("001", dt(2026, 2, 24, 10, 0), "999", Some(60)), // unknown
                make_event("001", dt(2026, 2, 24, 11, 0), "110", Some(60)), // Drive
            ];
            let refs: Vec<&KudgivtRow> = events.iter().collect();
            let double_refs: Vec<&&KudgivtRow> = refs.iter().collect();
            let cls = make_classifications();
            let (drive, cargo) = sum_events_in_range(
                &double_refs,
                &cls,
                dt(2026, 2, 24, 9, 0),
                dt(2026, 2, 24, 12, 0),
            );
            assert_eq!(drive, 60);
            assert_eq!(cargo, 0);
        });
    }

    #[test]
    fn test_calc_late_night_multi_day() {
        test_group!("CSVパーサー");
        test_case!("深夜時間計算: 複数日跨ぎ", {
            // 2/24 23:00 → 2/26 02:00 (2日跨ぎ)
            let total = calc_late_night_mins(dt(2026, 2, 24, 23, 0), dt(2026, 2, 26, 2, 0));
            // 2/24: 23:00-24:00 = 60min
            // 2/25: 0:00-5:00 = 300min, 22:00-24:00 = 120min → 420min
            // 2/26: 0:00-2:00 = 120min
            assert_eq!(total, 60 + 420 + 120);
        });
    }

    #[test]
    fn test_split_segments_at_24h_with_workday_boundary_skip() {
        test_group!("CSVパーサー");
        test_case!("workday境界がseg開始以前の場合スキップ", {
            let segments = vec![WorkSegment {
                start: dt(2026, 2, 25, 8, 0),
                end: dt(2026, 2, 26, 10, 0), // 26h
                labor_minutes: 600,
                drive_minutes: 400,
                cargo_minutes: 200,
            }];
            // boundary at 2/25 06:00 is before seg start, should be ignored
            let boundaries = vec![
                dt(2026, 2, 25, 6, 0),  // before seg start → skipped
                dt(2026, 2, 25, 20, 0), // within seg → split
            ];
            let result = split_segments_at_24h_with_workdays(segments, &boundaries);
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].end, dt(2026, 2, 25, 20, 0));
        });
    }

    /// 分割特例: 1回だけ 180分以上の休息で total < threshold (i32::MAX) → 閉じ括弧カバー
    #[test]
    fn test_determine_workdays_single_split_rest_below_threshold() {
        test_group!("CSVパーサー");
        test_case!("分割特例: 1回180分のみでは不成立", {
            // 1回だけ 200分 (>= 180) の休息 → split_rests = [200], len=1, threshold=i32::MAX
            // total < threshold なので if ブロック不成立
            let rest_events = vec![(dt(2026, 2, 20, 14, 0), 200)];
            let first_start = dt(2026, 2, 20, 8, 0);
            let last_end = dt(2026, 2, 20, 22, 0);
            let workdays = determine_workdays(&rest_events, first_start, last_end, false);
            assert_eq!(workdays.len(), 1);
        });
    }

    /// 短い休息 (< 180分) は分割特例の対象外 → continue カバー
    #[test]
    fn test_determine_workdays_short_rest_skipped() {
        test_group!("CSVパーサー");
        test_case!("短い休息は分割特例をスキップ", {
            // 60分の休息 (< 180) → 分割特例スキップ (continue)
            let rest_events = vec![(dt(2026, 2, 20, 14, 0), 60)];
            let first_start = dt(2026, 2, 20, 8, 0);
            let last_end = dt(2026, 2, 20, 20, 0);
            let workdays = determine_workdays(&rest_events, first_start, last_end, false);
            assert_eq!(workdays.len(), 1);
        });
    }

    /// boundary ループ内の continue (行389): 重複 boundary でカバー
    #[test]
    fn test_split_segments_duplicate_boundary_continue() {
        test_group!("CSVパーサー");
        test_case!("重複boundary で continue をカバー", {
            let segments = vec![WorkSegment {
                start: dt(2026, 2, 25, 8, 0),
                end: dt(2026, 2, 26, 12, 0), // 28h
                labor_minutes: 600,
                drive_minutes: 400,
                cargo_minutes: 200,
            }];
            // 同じ boundary を2回渡す → 2回目で *boundary <= cur_start → continue
            let boundaries = vec![
                dt(2026, 2, 25, 20, 0),
                dt(2026, 2, 25, 20, 0), // duplicate → continue at line 389
            ];
            let result = split_segments_at_24h_with_workdays(segments, &boundaries);
            assert_eq!(result.len(), 2); // 08:00-20:00, 20:00-12:00
        });
    }

    #[test]
    fn test_split_at_24h_with_workdays_short_segment_passthrough() {
        test_group!("CSVパーサー");
        test_case!(
            "24h以内かつworkday境界なしのセグメントはそのまま通る",
            {
                let segments = vec![WorkSegment {
                    start: dt(2026, 2, 24, 8, 0),
                    end: dt(2026, 2, 24, 17, 0),
                    labor_minutes: 300,
                    drive_minutes: 200,
                    cargo_minutes: 100,
                }];
                let result = split_segments_at_24h_with_workdays(segments, &[]);
                assert_eq!(result.len(), 1);
                assert_eq!(result[0].start, dt(2026, 2, 24, 8, 0));
                assert_eq!(result[0].end, dt(2026, 2, 24, 17, 0));
                assert_eq!(result[0].labor_minutes, 300);
            }
        );
    }

    #[test]
    fn test_sum_events_cargo_class() {
        test_group!("CSVパーサー");
        test_case!("Cargo分類のイベントがcargoに集計される", {
            let events = vec![
                make_event("001", dt(2026, 2, 24, 10, 0), "202", Some(45)), // Cargo
                make_event("001", dt(2026, 2, 24, 11, 0), "110", Some(60)), // Drive
            ];
            let refs: Vec<&KudgivtRow> = events.iter().collect();
            let double_refs: Vec<&&KudgivtRow> = refs.iter().collect();
            let cls = make_classifications();
            let (drive, cargo) = sum_events_in_range(
                &double_refs,
                &cls,
                dt(2026, 2, 24, 9, 0),
                dt(2026, 2, 24, 12, 0),
            );
            assert_eq!(drive, 60);
            assert_eq!(cargo, 45);
        });
    }

    #[test]
    fn test_split_segments_by_day_zero_minute_tail_skipped() {
        test_group!("CSVパーサー");
        test_case!(
            "日跨ぎ直後数十秒の尻尾は分単位切り捨てで0分になりskipされる",
            {
                // 24日23:00〜25日00:00:30: 2日目分は day_start=day_end (分精度) で
                // work_mins=0 → 行を作らず continue する分岐を踏む
                let segments = vec![WorkSegment {
                    start: dt(2026, 2, 24, 23, 0),
                    end: NaiveDate::from_ymd_opt(2026, 2, 25)
                        .unwrap()
                        .and_hms_opt(0, 0, 30)
                        .unwrap(),
                    labor_minutes: 60,
                    drive_minutes: 60,
                    cargo_minutes: 0,
                }];
                let daily = split_segments_by_day(&segments);
                assert_eq!(daily.len(), 1);
                assert_eq!(daily[0].date, NaiveDate::from_ymd_opt(2026, 2, 24).unwrap());
                assert_eq!(daily[0].work_minutes, 60);
            }
        );
    }
}
