//! アップロードの取り込みが保存する、日別の労働時間とセグメントの計算 (Refs ippoan/rust-alc-api#725)。
//!
//! backend の `alc-dtako` (`calculate_daily_hours`) と分割 worker (ippoan/alc-dtako-worker) が、同じこの関数を呼ぶ。
//! **DB を呼ばない** (分類の読み込み・乗務員 id の引き当て・削除・保存は呼び手が持つ)。
//! 式は `calculate_daily_hours` に在ったものをそのまま移した — 数値を変えない。

use std::collections::HashMap;

use chrono::{NaiveDate, NaiveDateTime};

use alc_csv_parser::kudgivt::KudgivtRow;
use alc_csv_parser::kudguri::KudguriRow;
use alc_csv_parser::work_segments::EventClass;

use crate::{
    build_day_map, group_operations_into_work_days, post_process_day_map, DayKey, FerryInfo,
};

/// フェリーデータ（合計分 + 各エントリの開始時刻）
pub struct FerryData {
    pub total_minutes: i32,
    pub start_times: Vec<NaiveDateTime>,
    /// フェリー乗船期間(start, end)リスト
    pub periods: Vec<(NaiveDateTime, NaiveDateTime)>,
}

/// 運行 1 件ぶんの KUDGFRY の CSV テキスト (UTF-8 に decode 済み) から [`FerryData`] を作る。
///
/// 行の読み方は [`crate::parse_ferry_periods_from_text`] (1 行目は見出し・列 10 と 11 が開始と終了)。テキストは運行 1 件ぶんの
/// 前提なので、列 0 (運行NO) は見ない。分数は [`crate::ferry_period_minutes`] で、0 分以下の行は数えない。
/// 合計が 0 分以下なら `None`。`start_times`・`periods` は数えた行の順。
pub fn ferry_data_from_text(text: &str) -> Option<FerryData> {
    let mut total_minutes = 0i32;
    let mut start_times = Vec::new();
    let mut periods = Vec::new();
    for (_, start, end) in crate::parse_ferry_periods_from_text(text) {
        let mins = crate::ferry_period_minutes(start, end);
        if mins > 0 {
            total_minutes += mins;
            start_times.push(start);
            periods.push((start, end));
        }
    }
    (total_minutes > 0).then_some(FerryData {
        total_minutes,
        start_times,
        periods,
    })
}

/// 保存するセグメント 1 件 (`dtako_daily_work_segments` の 1 行ぶん)。
#[derive(Debug, Clone, PartialEq)]
pub struct DailySegment {
    pub unko_no: String,
    pub segment_index: i32,
    pub start_at: NaiveDateTime,
    pub end_at: NaiveDateTime,
    pub work_minutes: i32,
    pub labor_minutes: i32,
    pub late_night_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
}

/// 日エントリ 1 件 (`dtako_daily_work_hours` の 1 行ぶん + そのセグメント)。key は [`DayKey`]。
#[derive(Debug, Clone, PartialEq)]
pub struct DailyHours {
    pub total_work_minutes: i32,
    pub total_labor_minutes: i32,
    pub late_night_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub total_distance: f64,
    pub operation_count: i32,
    pub unko_nos: Vec<String>,
    pub segments: Vec<DailySegment>,
    pub rest_event_minutes: i32,
    pub overlap_drive_minutes: i32,
    pub overlap_cargo_minutes: i32,
    pub overlap_break_minutes: i32,
    pub overlap_restraint_minutes: i32,
    pub ot_late_night_minutes: i32,
}

impl DailyHours {
    /// 保存する `total_drive_minutes` の値 (= 労働の合計)。
    pub fn saved_total_drive_minutes(&self) -> i32 {
        self.total_labor_minutes
    }

    /// 保存する `late_night_minutes` の値 (時間外の深夜を引いたもの。負にはしない)。
    pub fn saved_late_night_minutes(&self) -> i32 {
        (self.late_night_minutes - self.ot_late_night_minutes).max(0)
    }
}

/// KUDGURI・KUDGIVT の行と分類・フェリーから、日エントリ ([`DayKey`] → [`DailyHours`]) を計算する。
pub fn compute_daily_hours(
    rows: &[KudguriRow],
    kudgivt_rows: &[KudgivtRow],
    classifications: &HashMap<String, EventClass>,
    ferry: &HashMap<String, FerryData>,
) -> HashMap<DayKey, DailyHours> {
    // 0. 始業ベースのワークデイグルーピング（unko_no → work_date）
    let unko_work_date = group_operations_into_work_days(rows);

    // 2. Group KUDGIVT rows by unko_no
    let mut kudgivt_by_unko: HashMap<String, Vec<&KudgivtRow>> = HashMap::new();
    for row in kudgivt_rows {
        kudgivt_by_unko
            .entry(row.unko_no.clone())
            .or_default()
            .push(row);
    }

    // 2.5. 302休息イベントを始業ベースのワークデイで集計
    let mut rest_event_map: HashMap<(String, NaiveDate), i32> = HashMap::new();
    for row in kudgivt_rows {
        if classifications.get(&row.event_cd) == Some(&EventClass::RestSplit) {
            let dur = row.duration_minutes.unwrap_or(0);
            if dur <= 0 {
                continue;
            }
            let work_date = unko_work_date
                .get(&row.unko_no)
                .copied()
                .unwrap_or(row.start_at.date());
            *rest_event_map
                .entry((row.driver_cd.clone(), work_date))
                .or_insert(0) += dur;
        }
    }

    // 3. 共通 build_day_map で日別集計を構築
    let build_result = build_day_map(rows, &kudgivt_by_unko, classifications);
    let mut compare_day_map = build_result.day_map;
    let mut workday_boundaries = build_result.workday_boundaries;
    let mut day_work_events = build_result.day_work_events;

    // 3.5. FerryInfoをuploadのFerryDataから構築
    let compare_ferry_info = {
        let mut fi_minutes: HashMap<String, i32> = HashMap::new();
        let mut fi_break_dur: HashMap<String, i32> = HashMap::new();
        let mut fi_period_map: HashMap<String, Vec<(NaiveDateTime, NaiveDateTime)>> =
            HashMap::new();
        for (unko_no, fd) in ferry.iter() {
            fi_minutes.insert(unko_no.clone(), fd.total_minutes);
            fi_period_map.insert(unko_no.clone(), fd.periods.clone());
            let Some(events) = kudgivt_by_unko.get(unko_no.as_str()) else {
                continue;
            };
            let mut break_total = 0i32;
            for ferry_start in &fd.start_times {
                let matched_301 = events
                    .iter()
                    .filter(|e| classifications.get(&e.event_cd) == Some(&EventClass::Break))
                    .filter(|e| e.duration_minutes.unwrap_or(0) > 0)
                    .min_by_key(|e| (e.start_at - *ferry_start).num_seconds().unsigned_abs());
                if let Some(evt) = matched_301 {
                    break_total += evt.duration_minutes.unwrap_or(0);
                }
            }
            if break_total > 0 {
                fi_break_dur.insert(unko_no.clone(), break_total);
            }
        }
        FerryInfo {
            ferry_minutes: fi_minutes,
            ferry_break_dur: fi_break_dur,
            ferry_period_map: fi_period_map,
        }
    };

    // 3.6. 共通 post_process_day_map で構内結合・overlap計算・フェリー控除を実行
    post_process_day_map(
        &mut compare_day_map,
        &mut workday_boundaries,
        &build_result.multi_wd_boundaries,
        &mut day_work_events,
        &kudgivt_by_unko,
        classifications,
        rows,
        &compare_ferry_info,
    );

    // 3.7. compare::DayAgg → upload用の enriched 構造体に変換
    // unko_no → (total_distance, driver_cd) マッピング
    let mut unko_meta: HashMap<String, (f64, String)> = HashMap::new();
    for row in rows {
        unko_meta.insert(
            row.unko_no.clone(),
            (row.total_distance.unwrap_or(0.0), row.driver_cd.clone()),
        );
    }

    let mut day_map: HashMap<DayKey, DailyHours> = HashMap::new();

    for (key, c_agg) in &compare_day_map {
        let (driver_cd, _work_date, _start_time) = key;

        // total_distance: 各unko_noの距離をwork_minutes比率で按分
        let total_distance: f64 = c_agg
            .unko_nos
            .iter()
            .map(|u| unko_meta.get(u).map(|(d, _)| *d).unwrap_or(0.0))
            .sum();

        // rest_event_minutes: rest_event_mapから取得
        let rest_minutes = rest_event_map
            .get(&(driver_cd.clone(), *_work_date))
            .copied()
            .unwrap_or(0);

        // SegmentRecord の構築: compare SegRec の start_at/end_at から詳細を再計算
        // unko_no の特定: セグメント時刻と operations の dep/ret を照合
        let mut segments: Vec<DailySegment> = Vec::new();
        // unko_no ごとのセグメントカウンター
        let mut seg_counters: HashMap<String, i32> = HashMap::new();

        for seg_rec in &c_agg.segments {
            let seg_duration = (seg_rec.end_at - seg_rec.start_at).num_minutes() as i32;
            let seg_late_night = alc_csv_parser::work_segments::calc_late_night_mins(
                seg_rec.start_at,
                seg_rec.end_at,
            );

            // unko_no を特定: どのoperationの dep..ret に含まれるか
            let unko_no = c_agg
                .unko_nos
                .iter()
                .find(|u| {
                    rows.iter().any(|r| {
                        &r.unko_no == *u
                            && r.departure_at
                                .map(|d| seg_rec.start_at >= d)
                                .unwrap_or(false)
                            && r.return_at
                                .map(|ret| seg_rec.end_at <= ret)
                                .unwrap_or(false)
                    })
                })
                .or_else(|| c_agg.unko_nos.first())
                .cloned()
                .unwrap_or_default();

            let seg_idx = seg_counters.entry(unko_no.clone()).or_insert(0);

            // drive/cargo はセグメント時間に対する日合計の比率で按分
            let day_total_seg_mins: i32 = c_agg
                .segments
                .iter()
                .map(|s| (s.end_at - s.start_at).num_minutes() as i32)
                .sum();
            let ratio = seg_duration as f64 / day_total_seg_mins.max(1) as f64;

            segments.push(DailySegment {
                unko_no,
                segment_index: *seg_idx,
                start_at: seg_rec.start_at,
                end_at: seg_rec.end_at,
                work_minutes: seg_duration,
                labor_minutes: ((c_agg.drive_minutes + c_agg.cargo_minutes) as f64 * ratio).round()
                    as i32,
                late_night_minutes: seg_late_night,
                drive_minutes: (c_agg.drive_minutes as f64 * ratio).round() as i32,
                cargo_minutes: (c_agg.cargo_minutes as f64 * ratio).round() as i32,
            });

            *seg_idx += 1;
        }

        day_map.insert(
            key.clone(),
            DailyHours {
                total_work_minutes: c_agg.total_work_minutes,
                total_labor_minutes: c_agg.drive_minutes + c_agg.cargo_minutes,
                late_night_minutes: c_agg.late_night_minutes,
                drive_minutes: c_agg.drive_minutes,
                cargo_minutes: c_agg.cargo_minutes,
                total_distance,
                operation_count: c_agg.unko_nos.len() as i32,
                unko_nos: c_agg.unko_nos.clone(),
                segments,
                rest_event_minutes: rest_minutes,
                overlap_drive_minutes: c_agg.overlap_drive_minutes,
                overlap_cargo_minutes: c_agg.overlap_cargo_minutes,
                overlap_break_minutes: c_agg.overlap_break_minutes,
                overlap_restraint_minutes: c_agg.overlap_restraint_minutes,
                ot_late_night_minutes: c_agg.ot_late_night_minutes,
            },
        );
    }

    day_map
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{dt, make_kudgivt, make_kudguri};
    use chrono::NaiveTime;

    /// 既定の分類 (201 運転 / 202〜204 荷役 / 302 休息 / 301 休憩)。
    fn cls() -> HashMap<String, EventClass> {
        ["201", "202", "203", "204", "302", "301"]
            .into_iter()
            .map(|cd| {
                let class = alc_csv_parser::work_segments::default_classification(cd).1;
                (cd.to_string(), class)
            })
            .collect()
    }

    /// 出力を key の順に並べる (HashMap の順に依らずに比べるため)。
    fn sorted(map: HashMap<DayKey, DailyHours>) -> Vec<(DayKey, DailyHours)> {
        let mut entries: Vec<_> = map.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    /// 乗務員CD を指定した KUDGIVT の 1 行 (`make_kudgivt` は乗務員CD が固定のため)。
    fn evt(unko: &str, driver: &str, start: NaiveDateTime, cd: &str, dur: i32) -> KudgivtRow {
        let mut e = make_kudgivt(unko, start, cd, dur);
        e.driver_cd = driver.into();
        e
    }

    /// 日エントリの key (乗務員CD, 2026-03-`day`, `hour`:00:00)。
    fn key(driver: &str, day: u32, hour: u32) -> DayKey {
        let date = NaiveDate::from_ymd_opt(2026, 3, day).unwrap();
        (
            driver.to_string(),
            date,
            NaiveTime::from_hms_opt(hour, 0, 0).unwrap(),
        )
    }

    /// 2026-03-`day` の `hour`:`min`:00。
    fn at(day: u32, hour: u32, min: u32) -> NaiveDateTime {
        dt(2026, 3, day, hour, min, 0)
    }

    /// フェリー 1 回 (開始〜終了) を 1 運行に付ける。
    fn ferry(unko: &str, start: NaiveDateTime, end: NaiveDateTime) -> HashMap<String, FerryData> {
        let data = FerryData {
            total_minutes: (end - start).num_minutes() as i32,
            start_times: vec![start],
            periods: vec![(start, end)],
        };
        HashMap::from([(unko.to_string(), data)])
    }

    fn no_ferry() -> HashMap<String, FerryData> {
        HashMap::new()
    }

    #[test]
    fn test_single_operation_single_day() {
        test_group!("日別の集計 (アップロード)");
        test_case!("1 運行 1 日: 運転・休憩・荷役・運転", {
            let mut op = make_kudguri("T1", "D01", at(2, 8, 0), at(2, 17, 0));
            op.total_distance = Some(123.5);
            let events = vec![
                evt("T1", "D01", at(2, 8, 0), "201", 240),
                evt("T1", "D01", at(2, 12, 0), "301", 60),
                evt("T1", "D01", at(2, 13, 0), "202", 60),
                evt("T1", "D01", at(2, 14, 0), "201", 180),
            ];
            let got = sorted(compute_daily_hours(&[op], &events, &cls(), &no_ferry()));
            let segment = DailySegment {
                unko_no: "T1".into(),
                segment_index: 0,
                start_at: at(2, 8, 0),
                end_at: at(2, 17, 0),
                work_minutes: 540,
                labor_minutes: 480,
                late_night_minutes: 0,
                drive_minutes: 420,
                cargo_minutes: 60,
            };
            let hours = DailyHours {
                total_work_minutes: 540,
                total_labor_minutes: 480,
                late_night_minutes: 0,
                drive_minutes: 420,
                cargo_minutes: 60,
                total_distance: 123.5,
                operation_count: 1,
                unko_nos: vec!["T1".into()],
                segments: vec![segment],
                rest_event_minutes: 0,
                overlap_drive_minutes: 0,
                overlap_cargo_minutes: 0,
                overlap_break_minutes: 0,
                overlap_restraint_minutes: 0,
                ot_late_night_minutes: 0,
            };
            assert_eq!(got, vec![(key("D01", 2, 8), hours)]);
        });
    }

    #[test]
    fn test_overnight_operation_splits_at_midnight() {
        test_group!("日別の集計 (アップロード)");
        test_case!(
            "日またぎ: 日エントリは 1 つ、セグメントは 0 時で 2 つ。運転は長さの比で按分",
            {
                let op = make_kudguri("T2", "D01", at(2, 20, 0), at(3, 6, 0));
                let events = vec![
                    evt("T2", "D01", at(2, 20, 0), "201", 300),
                    evt("T2", "D01", at(3, 1, 0), "301", 60),
                    evt("T2", "D01", at(3, 2, 0), "201", 240),
                ];
                let got = sorted(compute_daily_hours(&[op], &events, &cls(), &no_ferry()));
                let before_midnight = DailySegment {
                    unko_no: "T2".into(),
                    segment_index: 0,
                    start_at: at(2, 20, 0),
                    end_at: at(3, 0, 0),
                    work_minutes: 240,
                    labor_minutes: 216,
                    late_night_minutes: 120,
                    drive_minutes: 216,
                    cargo_minutes: 0,
                };
                let after_midnight = DailySegment {
                    unko_no: "T2".into(),
                    segment_index: 1,
                    start_at: at(3, 0, 0),
                    end_at: at(3, 6, 0),
                    work_minutes: 360,
                    labor_minutes: 324,
                    late_night_minutes: 300,
                    drive_minutes: 324,
                    cargo_minutes: 0,
                };
                let hours = DailyHours {
                    total_work_minutes: 600,
                    total_labor_minutes: 540,
                    late_night_minutes: 360,
                    drive_minutes: 540,
                    cargo_minutes: 0,
                    total_distance: 0.0,
                    operation_count: 1,
                    unko_nos: vec!["T2".into()],
                    segments: vec![before_midnight, after_midnight],
                    rest_event_minutes: 0,
                    overlap_drive_minutes: 0,
                    overlap_cargo_minutes: 0,
                    overlap_break_minutes: 0,
                    overlap_restraint_minutes: 0,
                    ot_late_night_minutes: 0,
                };
                assert_eq!(got, vec![(key("D01", 2, 20), hours)]);
            }
        );
    }

    #[test]
    fn test_two_operations_join_into_one_day() {
        test_group!("日別の集計 (アップロード)");
        test_case!(
            "同じ乗務員・同じ日の 2 運行は 1 つの日エントリ。距離は運行の合計",
            {
                let mut first = make_kudguri("T3", "D01", at(2, 8, 0), at(2, 12, 0));
                first.total_distance = Some(10.0);
                let mut second = make_kudguri("T4", "D01", at(2, 13, 0), at(2, 17, 0));
                second.total_distance = Some(20.25);
                let events = vec![
                    evt("T3", "D01", at(2, 8, 0), "201", 240),
                    evt("T4", "D01", at(2, 13, 0), "201", 200),
                    evt("T4", "D01", at(2, 16, 20), "203", 40),
                ];
                let got = sorted(compute_daily_hours(
                    &[first, second],
                    &events,
                    &cls(),
                    &no_ferry(),
                ));
                // セグメントの運転・荷役は、日の合計 (440・40) を長さの比 (240:240) で分ける
                let morning = DailySegment {
                    unko_no: "T3".into(),
                    segment_index: 0,
                    start_at: at(2, 8, 0),
                    end_at: at(2, 12, 0),
                    work_minutes: 240,
                    labor_minutes: 240,
                    late_night_minutes: 0,
                    drive_minutes: 220,
                    cargo_minutes: 20,
                };
                let afternoon = DailySegment {
                    unko_no: "T4".into(),
                    segment_index: 0,
                    start_at: at(2, 13, 0),
                    end_at: at(2, 17, 0),
                    work_minutes: 240,
                    labor_minutes: 240,
                    late_night_minutes: 0,
                    drive_minutes: 220,
                    cargo_minutes: 20,
                };
                let hours = DailyHours {
                    total_work_minutes: 540,
                    total_labor_minutes: 480,
                    late_night_minutes: 0,
                    drive_minutes: 440,
                    cargo_minutes: 40,
                    total_distance: 30.25,
                    operation_count: 2,
                    unko_nos: vec!["T3".into(), "T4".into()],
                    segments: vec![morning, afternoon],
                    rest_event_minutes: 0,
                    overlap_drive_minutes: 0,
                    overlap_cargo_minutes: 0,
                    overlap_break_minutes: 0,
                    overlap_restraint_minutes: 0,
                    ot_late_night_minutes: 0,
                };
                assert_eq!(got, vec![(key("D01", 2, 8), hours)]);
            }
        );
    }

    #[test]
    fn test_two_crew_members_get_separate_entries() {
        test_group!("日別の集計 (アップロード)");
        test_case!(
            "2 人乗務: 乗務員ごとに日エントリ (イベントは運行NO で束ねるので、同じ値になる)",
            {
                let driver = make_kudguri("T5", "D01", at(2, 8, 0), at(2, 16, 0));
                let mut helper = make_kudguri("T5", "D02", at(2, 8, 0), at(2, 16, 0));
                helper.crew_role = 2;
                let mut helper_event = evt("T5", "D02", at(2, 12, 0), "201", 240);
                helper_event.crew_role = 2;
                let events = vec![evt("T5", "D01", at(2, 8, 0), "201", 240), helper_event];
                let got = sorted(compute_daily_hours(
                    &[driver, helper],
                    &events,
                    &cls(),
                    &no_ferry(),
                ));
                let segment = DailySegment {
                    unko_no: "T5".into(),
                    segment_index: 0,
                    start_at: at(2, 8, 0),
                    end_at: at(2, 16, 0),
                    work_minutes: 480,
                    labor_minutes: 480,
                    late_night_minutes: 0,
                    drive_minutes: 480,
                    cargo_minutes: 0,
                };
                let hours = DailyHours {
                    total_work_minutes: 480,
                    total_labor_minutes: 480,
                    late_night_minutes: 0,
                    drive_minutes: 480,
                    cargo_minutes: 0,
                    total_distance: 0.0,
                    operation_count: 1,
                    unko_nos: vec!["T5".into()],
                    segments: vec![segment],
                    rest_event_minutes: 0,
                    overlap_drive_minutes: 0,
                    overlap_cargo_minutes: 0,
                    overlap_break_minutes: 0,
                    overlap_restraint_minutes: 0,
                    ot_late_night_minutes: 0,
                };
                let want = vec![(key("D01", 2, 8), hours.clone()), (key("D02", 2, 8), hours)];
                assert_eq!(got, want);
            }
        );
    }

    #[test]
    fn test_rest_split_makes_two_days_and_counts_rest_minutes() {
        test_group!("日別の集計 (アップロード)");
        test_case!("休息 (302) で日エントリが分かれる。休息の分数は始業の日に付き、長さ 0 の休息は数えない", {
            let op = make_kudguri("T6", "D01", at(2, 6, 0), at(3, 12, 0));
            let events = vec![
                evt("T6", "D01", at(2, 6, 0), "201", 480),
                evt("T6", "D01", at(2, 14, 0), "302", 600),
                evt("T6", "D01", at(3, 0, 0), "302", 0),
                evt("T6", "D01", at(3, 0, 0), "201", 720),
            ];
            let got = sorted(compute_daily_hours(&[op], &events, &cls(), &no_ferry()));
            let first_segment = DailySegment {
                unko_no: "T6".into(),
                segment_index: 0,
                start_at: at(2, 6, 0),
                end_at: at(2, 14, 0),
                work_minutes: 480,
                labor_minutes: 480,
                late_night_minutes: 0,
                drive_minutes: 480,
                cargo_minutes: 0,
            };
            let first = DailyHours {
                total_work_minutes: 480,
                total_labor_minutes: 480,
                late_night_minutes: 0,
                drive_minutes: 480,
                cargo_minutes: 0,
                total_distance: 0.0,
                operation_count: 1,
                unko_nos: vec!["T6".into()],
                segments: vec![first_segment],
                rest_event_minutes: 600,
                overlap_drive_minutes: 360,
                overlap_cargo_minutes: 0,
                overlap_break_minutes: 0,
                overlap_restraint_minutes: 360,
                ot_late_night_minutes: 0,
            };
            let second_segment = DailySegment {
                unko_no: "T6".into(),
                segment_index: 0,
                start_at: at(3, 0, 0),
                end_at: at(3, 12, 0),
                work_minutes: 720,
                labor_minutes: 720,
                late_night_minutes: 300,
                drive_minutes: 720,
                cargo_minutes: 0,
            };
            let second = DailyHours {
                total_work_minutes: 720,
                total_labor_minutes: 720,
                late_night_minutes: 300,
                drive_minutes: 720,
                cargo_minutes: 0,
                total_distance: 0.0,
                operation_count: 1,
                unko_nos: vec!["T6".into()],
                segments: vec![second_segment],
                rest_event_minutes: 0,
                overlap_drive_minutes: 0,
                overlap_cargo_minutes: 0,
                overlap_break_minutes: 0,
                overlap_restraint_minutes: 0,
                ot_late_night_minutes: 0,
            };
            assert_eq!(got, vec![(key("D01", 2, 6), first), (key("D01", 3, 0), second)]);
        });
    }

    /// フェリーの場面の共通の形: 8:00〜18:00 の 1 運行 (セグメントは 600 分のまま)。
    fn ferry_day(labor: i32) -> Vec<(DayKey, DailyHours)> {
        let segment = DailySegment {
            unko_no: "T7".into(),
            segment_index: 0,
            start_at: at(2, 8, 0),
            end_at: at(2, 18, 0),
            work_minutes: 600,
            labor_minutes: labor,
            late_night_minutes: 0,
            drive_minutes: labor,
            cargo_minutes: 0,
        };
        let hours = DailyHours {
            total_work_minutes: labor,
            total_labor_minutes: labor,
            late_night_minutes: 0,
            drive_minutes: labor,
            cargo_minutes: 0,
            total_distance: 0.0,
            operation_count: 1,
            unko_nos: vec!["T7".into()],
            segments: vec![segment],
            rest_event_minutes: 0,
            overlap_drive_minutes: 0,
            overlap_cargo_minutes: 0,
            overlap_break_minutes: 0,
            overlap_restraint_minutes: 0,
            ot_late_night_minutes: 0,
        };
        vec![(key("D01", 2, 8), hours)]
    }

    #[test]
    fn test_ferry_minutes_are_deducted() {
        test_group!("日別の集計 (アップロード)");
        let op = make_kudguri("T7", "D01", at(2, 8, 0), at(2, 18, 0));
        // フェリーは 11:05〜12:55 (110 分)
        let on_board = ferry("T7", at(2, 11, 5), at(2, 12, 55));
        test_case!(
            "フェリーの開始に最寄りの休憩 (301) が在る",
            {
                let events = vec![
                    evt("T7", "D01", at(2, 8, 0), "201", 180),
                    evt("T7", "D01", at(2, 11, 0), "301", 120),
                    evt("T7", "D01", at(2, 13, 0), "201", 300),
                ];
                let got = sorted(compute_daily_hours(
                    &[op.clone()],
                    &events,
                    &cls(),
                    &on_board,
                ));
                assert_eq!(got, ferry_day(480));
            }
        );
        test_case!(
            "休憩が無い (運転だけ) → フェリーの 110 分を引く",
            {
                let events = vec![evt("T7", "D01", at(2, 8, 0), "201", 600)];
                let got = sorted(compute_daily_hours(
                    &[op.clone()],
                    &events,
                    &cls(),
                    &on_board,
                ));
                assert_eq!(got, ferry_day(490));
            }
        );
        test_case!(
            "フェリーの運行に KUDGIVT が 1 行も無い → 0 分のまま (負にしない)",
            {
                let got = sorted(compute_daily_hours(&[op.clone()], &[], &cls(), &on_board));
                assert_eq!(got, ferry_day(0));
            }
        );
    }

    #[test]
    fn test_segment_outside_departure_return_falls_back_to_first_unko() {
        test_group!("日別の集計 (アップロード)");
        test_case!(
            "セグメントが運行の出発〜帰着に収まらない → 日エントリの最初の運行NO に付ける",
            {
                // 帰着は 12:00 だが、イベントは 7:00 から 480 分 (15:00 まで) 在る
                let op = make_kudguri("T8", "D01", at(2, 8, 0), at(2, 12, 0));
                let events = vec![evt("T8", "D01", at(2, 7, 0), "201", 480)];
                let got = sorted(compute_daily_hours(&[op], &events, &cls(), &no_ferry()));
                let inside = DailySegment {
                    unko_no: "T8".into(),
                    segment_index: 0,
                    start_at: at(2, 8, 0),
                    end_at: at(2, 12, 0),
                    work_minutes: 240,
                    labor_minutes: 274,
                    late_night_minutes: 0,
                    drive_minutes: 274,
                    cargo_minutes: 0,
                };
                // 12:00〜15:00 は帰着より後 = どの運行の出発〜帰着にも収まらない
                let outside = DailySegment {
                    unko_no: "T8".into(),
                    segment_index: 1,
                    start_at: at(2, 12, 0),
                    end_at: at(2, 15, 0),
                    work_minutes: 180,
                    labor_minutes: 206,
                    late_night_minutes: 0,
                    drive_minutes: 206,
                    cargo_minutes: 0,
                };
                let hours = DailyHours {
                    total_work_minutes: 480,
                    total_labor_minutes: 480,
                    late_night_minutes: 0,
                    drive_minutes: 480,
                    cargo_minutes: 0,
                    total_distance: 0.0,
                    operation_count: 1,
                    unko_nos: vec!["T8".into()],
                    segments: vec![inside, outside],
                    rest_event_minutes: 0,
                    overlap_drive_minutes: 0,
                    overlap_cargo_minutes: 0,
                    overlap_break_minutes: 0,
                    overlap_restraint_minutes: 0,
                    ot_late_night_minutes: 0,
                };
                assert_eq!(got, vec![(key("D01", 2, 8), hours)]);
            }
        );
        test_case!(
            "帰着の時刻が無い運行 → セグメントは無く、日エントリの開始は 0 時",
            {
                let mut op = make_kudguri("T9", "D01", at(2, 8, 0), at(2, 17, 0));
                op.return_at = None;
                let events = vec![evt("T9", "D01", at(2, 8, 0), "201", 300)];
                let got = sorted(compute_daily_hours(&[op], &events, &cls(), &no_ferry()));
                let hours = DailyHours {
                    total_work_minutes: 300,
                    total_labor_minutes: 300,
                    late_night_minutes: 0,
                    drive_minutes: 300,
                    cargo_minutes: 0,
                    total_distance: 0.0,
                    operation_count: 1,
                    unko_nos: vec!["T9".into()],
                    segments: vec![],
                    rest_event_minutes: 0,
                    overlap_drive_minutes: 0,
                    overlap_cargo_minutes: 0,
                    overlap_break_minutes: 0,
                    overlap_restraint_minutes: 0,
                    ot_late_night_minutes: 0,
                };
                assert_eq!(got, vec![(key("D01", 2, 0), hours)]);
            }
        );
    }

    #[test]
    fn test_no_operations_yields_no_entries() {
        test_group!("日別の集計 (アップロード)");
        test_case!("運行が無ければ日エントリも無い", {
            let got = compute_daily_hours(&[], &[], &cls(), &no_ferry());
            assert!(got.is_empty());
        });
    }

    #[test]
    fn test_saved_values() {
        test_group!("日別の集計 (アップロード)");
        let hours = |late_night: i32, ot_late_night: i32| DailyHours {
            total_work_minutes: 600,
            total_labor_minutes: 480,
            late_night_minutes: late_night,
            drive_minutes: 420,
            cargo_minutes: 60,
            total_distance: 0.0,
            operation_count: 1,
            unko_nos: vec![],
            segments: vec![],
            rest_event_minutes: 0,
            overlap_drive_minutes: 0,
            overlap_cargo_minutes: 0,
            overlap_break_minutes: 0,
            overlap_restraint_minutes: 0,
            ot_late_night_minutes: ot_late_night,
        };
        test_case!(
            "保存する total_drive_minutes は労働の合計 (運転 + 荷役)",
            {
                assert_eq!(hours(0, 0).saved_total_drive_minutes(), 480);
            }
        );
        test_case!(
            "保存する late_night_minutes は時間外の深夜を引いた値 (負にしない)",
            {
                assert_eq!(hours(100, 30).saved_late_night_minutes(), 70);
                assert_eq!(hours(30, 30).saved_late_night_minutes(), 0);
                assert_eq!(hours(10, 30).saved_late_night_minutes(), 0);
            }
        );
        test_case!(
            "型は Debug で出せる (テストの失敗時の表示用)",
            {
                let shown = format!("{:?}", hours(1, 2));
                assert!(shown.starts_with("DailyHours { total_work_minutes: 600"));
                let segment = ferry_day(1).remove(0).1.segments.remove(0);
                assert!(format!("{segment:?}").starts_with("DailySegment { unko_no: \"T7\""));
                assert_eq!(segment.clone(), segment);
            }
        );
    }

    #[test]
    fn test_ferry_data_from_text() {
        test_group!("日別の集計 (アップロード)");
        let head = "運行NO,1,2,3,4,5,6,7,8,9,開始,終了\n";
        let line = |start: &str, end: &str| format!("X,1,2,3,4,5,6,7,8,9,{start},{end}\n");
        test_case!(
            "KUDGFRY: 複数行の合計・開始時刻と期間は数えた行の順・列 0 は見ない",
            {
                let text = format!(
                    "{head}{}{}",
                    line("2026/03/02 10:00:00", "2026/03/02 11:30:00"),
                    line("2026/03/02 20:00:00", "2026/03/02 20:45:00")
                );
                let data = ferry_data_from_text(&text).unwrap();
                assert_eq!(data.total_minutes, 135);
                assert_eq!(data.start_times, vec![at(2, 10, 0), at(2, 20, 0)]);
                assert_eq!(
                    data.periods,
                    vec![(at(2, 10, 0), at(2, 11, 30)), (at(2, 20, 0), at(2, 20, 45))]
                );
            }
        );
        test_case!(
            "KUDGFRY: 秒は 30 秒で四捨五入 (29 秒は 0 分で数えない・30 秒は 1 分)",
            {
                let text = format!(
                    "{head}{}{}{}",
                    line("2026/03/02 10:00:00", "2026/03/02 10:00:29"),
                    line("2026/03/02 11:00:00", "2026/03/02 11:00:30"),
                    line("2026/03/02 12:00:00", "2026/03/02 12:01:29")
                );
                let data = ferry_data_from_text(&text).unwrap();
                assert_eq!(data.total_minutes, 2);
                assert_eq!(
                    data.start_times,
                    vec![dt(2026, 3, 2, 11, 0, 0), dt(2026, 3, 2, 12, 0, 0)]
                );
            }
        );
        test_case!(
            "KUDGFRY: 0 分・負・列が足りない行・日時の不正は数えず、合計が 0 なら None",
            {
                let text = format!(
                    "{head}{}{}X,1,2\n{}",
                    line("2026/03/02 10:00:00", "2026/03/02 10:00:00"),
                    line("2026/03/02 11:00:00", "2026/03/02 10:00:00"),
                    line("2026-03-02 10:00", "2026/03/02 11:00:00")
                );
                assert!(ferry_data_from_text(&text).is_none());
                assert!(ferry_data_from_text("").is_none());
                assert!(ferry_data_from_text(head).is_none());
            }
        );
        test_case!("KUDGFRY: 時が空白詰めの時刻 (%k) も読む", {
            let text = format!(
                "{head}{}",
                line("2026/03/02  9:00:00", "2026/03/02 10:30:00")
            );
            let data = ferry_data_from_text(&text).unwrap();
            assert_eq!(
                (data.total_minutes, data.periods.clone()),
                (90, vec![(at(2, 9, 0), at(2, 10, 30))])
            );
        });
    }
}
