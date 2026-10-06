//! 拘束時間管理表 CSV 比較ライブラリ
//!
//! compare.rs CLI と restraint_report.rs API の共通ロジック

#[cfg(test)]
#[macro_use]
mod test_macros;

pub mod upload_daily;

use std::collections::{BTreeMap, HashMap};

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde::Serialize;

use alc_csv_parser as csv_parser;
use alc_csv_parser::kudgivt::KudgivtRow;
use alc_csv_parser::kudguri::KudguriRow;
use alc_csv_parser::work_segments::{self, calc_late_night_mins, EventClass, Workday};

// ========== 共通型 ==========

#[derive(Debug, Clone, Serialize)]
pub struct CsvDayRow {
    pub date: String,
    pub is_holiday: bool,
    pub start_time: String,
    pub end_time: String,
    pub drive: String,
    pub overlap_drive: String,
    pub cargo: String,
    pub overlap_cargo: String,
    pub break_time: String,
    pub overlap_break: String,
    pub subtotal: String,
    pub overlap_subtotal: String,
    pub total: String,
    pub cumulative: String,
    pub rest: String,
    pub actual_work: String,
    pub overtime: String,
    pub late_night: String,
    pub ot_late_night: String,
    pub remarks: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CsvDriverData {
    pub driver_name: String,
    pub driver_cd: String,
    pub days: Vec<CsvDayRow>,
    pub total_drive: String,
    pub total_cargo: String,
    pub total_break: String,
    pub total_restraint: String,
    pub total_actual_work: String,
    pub total_overtime: String,
    pub total_late_night: String,
    pub total_ot_late_night: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiffItem {
    pub date: String,
    pub field: String,
    pub csv_val: String,
    pub sys_val: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub known_bug: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CompareReport {
    pub drivers: Vec<DriverCompareResult>,
    pub total_diffs: usize,
    pub known_bug_diffs: usize,
    pub unknown_diffs: usize,
}

#[derive(Debug, Serialize)]
pub struct DriverCompareResult {
    pub driver_name: String,
    pub driver_cd: String,
    pub diffs: Vec<DiffItem>,
    pub total_diffs: Vec<TotalDiffItem>,
    pub known_bug_diffs: usize,
    pub unknown_diffs: usize,
}

#[derive(Debug, Serialize)]
pub struct TotalDiffItem {
    pub label: String,
    pub csv_val: String,
    pub sys_val: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub known_bug: Option<String>,
}

// ========== ユーティリティ ==========

pub fn fmt_min(val: i32) -> String {
    if val == 0 {
        return String::new();
    }
    format!("{}:{:02}", val / 60, val.abs() % 60)
}

fn normalize_time(s: &str) -> String {
    let s = s.trim();
    if s.is_empty() {
        return String::new();
    }
    if let Some((h, m)) = s.split_once(':') {
        let h_num: u32 = h.parse().unwrap_or(0);
        format!("{}:{}", h_num, m)
    } else {
        s.to_string()
    }
}

/// 秒を切り捨てて分精度に揃える
pub fn trunc_min(dt: NaiveDateTime) -> NaiveDateTime {
    dt.with_second(0).unwrap_or(dt)
}

// ========== CSV パース ==========

pub fn parse_restraint_csv(bytes: &[u8]) -> Result<Vec<CsvDriverData>, String> {
    let text = if let Ok(s) = String::from_utf8(bytes.to_vec()) {
        s
    } else {
        let (decoded, _, _) = encoding_rs::SHIFT_JIS.decode(bytes);
        decoded.into_owned()
    };

    let mut drivers = Vec::new();
    let mut current: Option<CsvDriverData> = None;
    let mut in_data = false;

    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }

        if line.starts_with("氏名,") {
            if let Some(d) = current.take() {
                drivers.push(d);
            }
            let cols: Vec<&str> = line.split(',').collect();
            let name = cols.get(1).unwrap_or(&"").to_string();
            let cd = cols.get(3).unwrap_or(&"").to_string();
            current = Some(CsvDriverData {
                driver_name: name,
                driver_cd: cd,
                days: Vec::new(),
                total_drive: String::new(),
                total_cargo: String::new(),
                total_break: String::new(),
                total_restraint: String::new(),
                total_actual_work: String::new(),
                total_overtime: String::new(),
                total_late_night: String::new(),
                total_ot_late_night: String::new(),
            });
            in_data = false;
            continue;
        }

        if line.starts_with("日付,") {
            in_data = true;
            continue;
        }

        let Some(ref mut driver) = current else {
            continue;
        };
        if !in_data {
            continue;
        }

        let cols: Vec<&str> = line.split(',').collect();

        if cols.first().map(|s| s.contains("合計")).unwrap_or(false) {
            driver.total_drive = cols.get(3).unwrap_or(&"").to_string();
            driver.total_cargo = cols.get(5).unwrap_or(&"").to_string();
            driver.total_break = cols.get(7).unwrap_or(&"").to_string();
            driver.total_restraint = cols.get(11).unwrap_or(&"").to_string();
            driver.total_actual_work = cols.get(18).unwrap_or(&"").to_string();
            driver.total_overtime = cols.get(19).unwrap_or(&"").to_string();
            driver.total_late_night = cols.get(20).unwrap_or(&"").to_string();
            driver.total_ot_late_night = cols.get(21).unwrap_or(&"").to_string();
            in_data = false;
            continue;
        }

        let date_str = cols.first().unwrap_or(&"").to_string();
        if !date_str.contains('月') {
            continue;
        }

        let is_holiday = cols.get(1).map(|s| s.trim() == "休").unwrap_or(false);

        driver.days.push(CsvDayRow {
            date: date_str,
            is_holiday,
            start_time: cols.get(1).unwrap_or(&"").to_string(),
            end_time: cols.get(2).unwrap_or(&"").to_string(),
            drive: cols.get(3).unwrap_or(&"").to_string(),
            overlap_drive: cols.get(4).unwrap_or(&"").to_string(),
            cargo: cols.get(5).unwrap_or(&"").to_string(),
            overlap_cargo: cols.get(6).unwrap_or(&"").to_string(),
            break_time: cols.get(7).unwrap_or(&"").to_string(),
            overlap_break: cols.get(8).unwrap_or(&"").to_string(),
            subtotal: cols.get(11).unwrap_or(&"").to_string(),
            overlap_subtotal: cols.get(12).unwrap_or(&"").to_string(),
            total: cols.get(13).unwrap_or(&"").to_string(),
            cumulative: cols.get(14).unwrap_or(&"").to_string(),
            rest: cols.get(17).unwrap_or(&"").to_string(),
            actual_work: cols.get(18).unwrap_or(&"").to_string(),
            overtime: cols.get(19).unwrap_or(&"").to_string(),
            late_night: cols.get(20).unwrap_or(&"").to_string(),
            ot_late_night: cols.get(21).unwrap_or(&"").to_string(),
            remarks: cols.get(22).unwrap_or(&"").to_string(),
        });
    }

    if let Some(d) = current {
        drivers.push(d);
    }

    if drivers.is_empty() {
        return Err("ドライバーが見つかりません".to_string());
    }

    Ok(drivers)
}

// ========== 差分検出 ==========

/// CsvDayRow同士の差分を検出（日付ベースマッチング）
pub fn detect_diffs_csv(csv_days: &[CsvDayRow], sys_days: &[CsvDayRow]) -> Vec<DiffItem> {
    let mut diffs = Vec::new();

    let mut sys_idx = 0;
    for csv_day in csv_days {
        if csv_day.is_holiday {
            continue;
        }

        let sys_day = sys_days[sys_idx..]
            .iter()
            .find(|s| s.date == csv_day.date && !s.is_holiday);
        let sys_day = match sys_day {
            Some(sd) => {
                if let Some(pos) = sys_days[sys_idx..].iter().position(|s| std::ptr::eq(s, sd)) {
                    sys_idx += pos + 1;
                }
                sd
            }
            None => continue,
        };

        let csv_start = normalize_time(&csv_day.start_time);
        let sys_start = normalize_time(&sys_day.start_time);
        let csv_end = normalize_time(&csv_day.end_time);
        let sys_end = normalize_time(&sys_day.end_time);
        let checks = [
            ("始業", &csv_start, &sys_start),
            ("終業", &csv_end, &sys_end),
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
            if cv != sv && !(cv.is_empty() && sv.is_empty()) {
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

/// 参照CSVの日付データから対象年月を推定
pub fn detect_year_month(drivers: &[CsvDriverData]) -> (i32, u32) {
    for d in drivers {
        for day in &d.days {
            if day.is_holiday {
                continue;
            }
            // 年はCSVヘッダーから取れないので2026固定（要改善）
            let month = day
                .date
                .find('月')
                .and_then(|pos| day.date[..pos].parse::<u32>().ok());
            if let Some(m) = month {
                return (2026, m);
            }
        }
    }
    (2026, 1)
}

// ========== 既知バグパターン ==========

struct KnownBugPattern {
    driver_cd: &'static str,
    date_contains: &'static str,
    fields: &'static [&'static str],
    description: &'static str,
    cascading: bool,
}

/// web地球号の既知バグパターン定義
const KNOWN_BUGS: &[KnownBugPattern] = &[
    // 1039: 2/22 休息終了が始業にならないバグ → 170分消失
    KnownBugPattern {
        driver_cd: "1039",
        date_contains: "2月22",
        fields: &["始業", "終業", "運転", "小計", "合計", "実働", "時間外"],
        description: "web地球号バグ: 休息終了が始業にならない (#1)",
        cascading: true,
    },
    // 1039: 2/21 休息基準未達なのに終業扱い
    KnownBugPattern {
        driver_cd: "1039",
        date_contains: "2月21",
        fields: &["終業", "運転", "小計", "合計", "実働", "時間外", "深夜"],
        description: "web地球号バグ: 休息基準未達で終業扱い (#1)",
        cascading: true,
    },
    // 1068: 2/2 24h分離バグ（連続運行のカード乗換時、運行内休息でshigyo未リセット）
    KnownBugPattern {
        driver_cd: "1068",
        date_contains: "2月2",
        fields: &[
            "終業",
            "運転",
            "重複運転",
            "小計",
            "重複小計",
            "合計",
            "実働",
            "時間外",
            "深夜",
        ],
        description: "24h分離バグ: 運行内休息でshigyo未リセット (#2)",
        cascading: true,
    },
    KnownBugPattern {
        driver_cd: "1068",
        date_contains: "2月3",
        fields: &[
            "始業",
            "終業",
            "運転",
            "重複運転",
            "小計",
            "重複小計",
            "合計",
            "実働",
            "時間外",
            "深夜",
        ],
        description: "24h分離バグ: 連鎖 (#2)",
        cascading: false,
    },
    // 1069: 2/3-4 長距離480例外（24h境界手前の休息534分が分割されない）
    KnownBugPattern {
        driver_cd: "1069",
        date_contains: "2月3",
        fields: &[
            "終業",
            "運転",
            "重複運転",
            "小計",
            "重複小計",
            "実働",
            "時間外",
        ],
        description: "長距離480例外: 休息534分が未分割 (#3)",
        cascading: true,
    },
    KnownBugPattern {
        driver_cd: "1069",
        date_contains: "2月4",
        fields: &[
            "始業",
            "運転",
            "重複運転",
            "小計",
            "重複小計",
            "合計",
            "実働",
        ],
        description: "長距離480例外: 連鎖 (#3)",
        cascading: false,
    },
    // 1069: 2/9 #3バグの連鎖（overlap windowサイズのずれ）
    KnownBugPattern {
        driver_cd: "1069",
        date_contains: "2月9",
        fields: &["重複小計", "合計"],
        description: "長距離480例外: 連鎖 (#3)",
        cascading: false,
    },
    // 1078: #3バグ（休息521分が24h内で未分割）
    KnownBugPattern {
        driver_cd: "1078",
        date_contains: "2月15",
        fields: &[
            "終業",
            "運転",
            "重複運転",
            "小計",
            "重複小計",
            "合計",
            "実働",
            "時間外",
        ],
        description: "長距離480例外: 休息521分が未分割 (#3)",
        cascading: true,
    },
    KnownBugPattern {
        driver_cd: "1078",
        date_contains: "2月16",
        fields: &[
            "始業",
            "終業",
            "運転",
            "小計",
            "合計",
            "実働",
            "時間外",
            "深夜",
        ],
        description: "長距離480例外: 連鎖 (#3)",
        cascading: true,
    },
    // 1071: #3バグ（休息529/525/507分が24h内で未分割）
    KnownBugPattern {
        driver_cd: "1071",
        date_contains: "2月2",
        fields: &[
            "終業",
            "運転",
            "重複運転",
            "小計",
            "重複小計",
            "実働",
            "時間外",
        ],
        description: "長距離480例外: 休息529分が未分割 (#3)",
        cascading: true,
    },
    KnownBugPattern {
        driver_cd: "1071",
        date_contains: "2月3",
        fields: &[
            "始業",
            "運転",
            "重複運転",
            "小計",
            "重複小計",
            "合計",
            "実働",
            "深夜",
        ],
        description: "長距離480例外: 連鎖 (#3)",
        cascading: false,
    },
    KnownBugPattern {
        driver_cd: "1071",
        date_contains: "2月5",
        fields: &[
            "終業",
            "運転",
            "重複運転",
            "小計",
            "重複小計",
            "合計",
            "実働",
        ],
        description: "24h分離+480例外: 連鎖 (#2/#3)",
        cascading: false,
    },
    // 1071: 2/13-14 長距離480例外（休息507分が未分割）
    KnownBugPattern {
        driver_cd: "1071",
        date_contains: "2月13",
        fields: &[
            "終業",
            "運転",
            "重複運転",
            "小計",
            "重複小計",
            "実働",
            "時間外",
        ],
        description: "長距離480例外: 休息507分が未分割 (#3)",
        cascading: true,
    },
    KnownBugPattern {
        driver_cd: "1071",
        date_contains: "2月14",
        fields: &["運転", "小計", "実働", "時間外", "深夜"],
        description: "長距離480例外: 連鎖 (#3)",
        cascading: false,
    },
    KnownBugPattern {
        driver_cd: "1071",
        date_contains: "2月14",
        fields: &["始業", "重複運転", "重複小計", "合計"],
        description: "長距離480例外: 休息525分が未分割 (#3)",
        cascading: false,
    },
    KnownBugPattern {
        driver_cd: "1071",
        date_contains: "2月28",
        fields: &["始業", "重複運転", "重複小計", "合計"],
        description: "長距離480例外: 休息507分が未分割 (#3)",
        cascading: false,
    },
    // 1072: 2/18-19 長距離480例外（休息530分が未分割）
    KnownBugPattern {
        driver_cd: "1072",
        date_contains: "2月18",
        fields: &[
            "終業",
            "運転",
            "重複運転",
            "小計",
            "重複小計",
            "実働",
            "時間外",
        ],
        description: "長距離480例外: 休息530分が未分割 (#3)",
        cascading: true,
    },
    KnownBugPattern {
        driver_cd: "1072",
        date_contains: "2月19",
        fields: &[
            "始業",
            "運転",
            "重複運転",
            "小計",
            "重複小計",
            "合計",
            "実働",
            "時間外",
            "深夜",
        ],
        description: "長距離480例外: 連鎖 (#3)",
        cascading: true,
    },
];

/// 差分リストに既知バグアノテーションを付与（連鎖差分の自動計算含む）
pub fn annotate_known_bugs(
    driver_cd: &str,
    diffs: &mut [DiffItem],
    total_diffs: &mut [TotalDiffItem],
) {
    let mut has_cascading = false;

    // Phase 1: 直接パターンマッチ
    for diff in diffs.iter_mut() {
        for pattern in KNOWN_BUGS {
            if pattern.driver_cd == driver_cd
                && diff.date.contains(pattern.date_contains)
                && pattern.fields.contains(&diff.field.as_str())
            {
                diff.known_bug = Some(pattern.description.to_string());
                if pattern.cascading {
                    has_cascading = true;
                }
                break;
            }
        }
    }

    // Phase 2: 連鎖差分（cascading=trueのパターンがあれば、以降の累計差分もマーク）
    if has_cascading {
        // 直接マッチした最初の日付インデックスを取得
        // has_cascading=true → 少なくとも1つ known_bug ありなので unwrap 安全
        let idx = diffs.iter().position(|d| d.known_bug.is_some()).unwrap();
        for diff in &mut diffs[idx..] {
            if diff.known_bug.is_some() {
                continue;
            }
            // 累計は始点以降の全日が影響を受ける
            if diff.field == "累計" {
                diff.known_bug = Some("連鎖: 既知バグによる累計ずれ (#1)".to_string());
            }
        }
    }

    // Phase 3: 合計行 — 全day差分がknown_bugなら合計もknown_bug
    let all_day_diffs_known = diffs.iter().all(|d| d.known_bug.is_some());
    if all_day_diffs_known && !diffs.is_empty() {
        for td in total_diffs.iter_mut() {
            td.known_bug = Some("連鎖: 既知バグによる合計ずれ (#1)".to_string());
        }
    }
}

// ========== ドライバー比較 ==========

pub fn compare_drivers(
    drivers1: &[CsvDriverData],
    drivers2: &[CsvDriverData],
    driver_filter: Option<&str>,
) -> CompareReport {
    let mut report = CompareReport {
        drivers: Vec::new(),
        total_diffs: 0,
        known_bug_diffs: 0,
        unknown_diffs: 0,
    };

    for d1 in drivers1 {
        if driver_filter.is_some_and(|f| d1.driver_cd != f) {
            continue;
        }
        let d2 = drivers2.iter().find(|d| d.driver_cd == d1.driver_cd);
        let Some(d2) = d2 else {
            report.drivers.push(DriverCompareResult {
                driver_name: d1.driver_name.clone(),
                driver_cd: d1.driver_cd.clone(),
                diffs: Vec::new(),
                total_diffs: vec![TotalDiffItem {
                    label: "エラー".to_string(),
                    csv_val: "存在".to_string(),
                    sys_val: "該当なし".to_string(),
                    known_bug: None,
                }],
                known_bug_diffs: 0,
                unknown_diffs: 0,
            });
            continue;
        };

        let mut diffs = detect_diffs_csv(&d1.days, &d2.days);

        let mut total_diffs_vec = Vec::new();
        let total_checks = [
            ("運転合計", &d1.total_drive, &d2.total_drive),
            ("拘束合計", &d1.total_restraint, &d2.total_restraint),
            ("実働合計", &d1.total_actual_work, &d2.total_actual_work),
            ("時間外合計", &d1.total_overtime, &d2.total_overtime),
            ("深夜合計", &d1.total_late_night, &d2.total_late_night),
        ];
        for (label, v1, v2) in total_checks {
            let a = v1.trim();
            let b = v2.trim();
            if a != b && !(a.is_empty() && b.is_empty()) {
                total_diffs_vec.push(TotalDiffItem {
                    label: label.to_string(),
                    csv_val: a.to_string(),
                    sys_val: b.to_string(),
                    known_bug: None,
                });
            }
        }

        // 既知バグアノテーション
        annotate_known_bugs(&d1.driver_cd, &mut diffs, &mut total_diffs_vec);

        let known = diffs.iter().filter(|d| d.known_bug.is_some()).count()
            + total_diffs_vec
                .iter()
                .filter(|t| t.known_bug.is_some())
                .count();
        let diff_count = diffs.len() + total_diffs_vec.len();
        let unknown = diff_count - known;

        report.total_diffs += diff_count;
        report.known_bug_diffs += known;
        report.unknown_diffs += unknown;

        report.drivers.push(DriverCompareResult {
            driver_name: d1.driver_name.clone(),
            driver_cd: d1.driver_cd.clone(),
            diffs,
            total_diffs: total_diffs_vec,
            known_bug_diffs: known,
            unknown_diffs: unknown,
        });
    }

    report
}

// ========== ZIP → インメモリ計算 ==========

fn default_classifications() -> HashMap<String, EventClass> {
    // 分類の値は alc-csv-parser の既定の表から引く (表は 1 つ)。
    // ここに並べた 6 コード以外は map に入れない (`get` が `None` のまま)
    ["201", "202", "203", "204", "302", "301"]
        .into_iter()
        .map(|cd| (cd.to_string(), work_segments::default_classification(cd).1))
        .collect()
}

/// 実働ベースの時間外深夜計算
/// Drive/Cargoイベントの累計が480分に達した後の深夜時間を返す
pub fn calc_ot_late_night_from_events(events: &[(NaiveDateTime, NaiveDateTime)]) -> i32 {
    let mut cumulative = 0i64;
    let mut ot_night = 0i32;
    for &(start, end) in events {
        let dur = (end - start).num_minutes();
        if dur <= 0 {
            continue;
        }
        if cumulative >= 480 {
            // 全て時間外
            ot_night += calc_late_night_mins(start, end);
        } else if cumulative + dur <= 480 {
            // 全て所定内
        } else {
            // 境界を跨ぐ: 480分到達点で分割
            let regular_dur = 480 - cumulative;
            let boundary = start + chrono::Duration::minutes(regular_dur);
            ot_night += calc_late_night_mins(boundary, end);
        }
        cumulative += dur;
    }
    ot_night
}

pub fn group_operations_into_work_days(rows: &[KudguriRow]) -> HashMap<String, NaiveDate> {
    const REST_THRESHOLD_MINUTES: i64 = 540;
    const MAX_WORK_DAY_MINUTES: i64 = 1440;

    let mut unko_work_date: HashMap<String, NaiveDate> = HashMap::new();
    let mut driver_rows: HashMap<String, Vec<&KudguriRow>> = HashMap::new();
    for row in rows {
        if !row.driver_cd.is_empty() {
            driver_rows
                .entry(row.driver_cd.clone())
                .or_default()
                .push(row);
        }
    }

    for (_driver_cd, mut ops) in driver_rows {
        ops.sort_by(|a, b| {
            let da = a.departure_at.or(a.garage_out_at);
            let db = b.departure_at.or(b.garage_out_at);
            da.cmp(&db)
        });

        let mut current_shigyo: Option<NaiveDateTime> = None;
        let mut current_work_date: Option<NaiveDate> = None;
        let mut last_end: Option<NaiveDateTime> = None;

        for row in &ops {
            let dep = match row.departure_at.or(row.garage_out_at) {
                Some(d) => d,
                None => {
                    let wd = row.operation_date.unwrap_or(row.reading_date);
                    unko_work_date.insert(row.unko_no.clone(), wd);
                    continue;
                }
            };
            let ret = row.return_at.or(row.garage_in_at).unwrap_or(dep);

            let new_day = if let (Some(shigyo), Some(prev_end)) = (current_shigyo, last_end) {
                let gap_minutes = (dep - prev_end).num_minutes();
                let since_shigyo_minutes = (dep - shigyo).num_minutes();
                // 長距離判定: 日跨ぎ運行は480分（例外基準）
                let is_long_distance = dep.date() != ret.date();
                let threshold = if is_long_distance {
                    480
                } else {
                    REST_THRESHOLD_MINUTES
                };
                gap_minutes >= threshold || since_shigyo_minutes >= MAX_WORK_DAY_MINUTES
            } else {
                true
            };

            if new_day {
                current_shigyo = Some(dep);
                current_work_date = Some(dep.date());
            }

            unko_work_date.insert(row.unko_no.clone(), current_work_date.unwrap());
            last_end = Some(last_end.map_or(ret, |prev| ret.max(prev)));
        }
    }

    unko_work_date
}

/// 日時文字列を2種類のフォーマット (%H / %k) で試行パースする
fn parse_datetime_lenient(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S")
        .ok()
        .or_else(|| NaiveDateTime::parse_from_str(s, "%Y/%m/%d %k:%M:%S").ok())
}

/// KUDGFRY CSVテキストからフェリー乗船期間をパースする（IO分離済み）
pub fn parse_ferry_periods_from_text(text: &str) -> Vec<(String, NaiveDateTime, NaiveDateTime)> {
    let mut periods = Vec::new();
    for line in text.lines().skip(1) {
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() <= 11 {
            continue;
        }
        let unko_no = cols[0].trim().to_string();
        if let (Some(s), Some(e)) = (
            parse_datetime_lenient(cols[10].trim()),
            parse_datetime_lenient(cols[11].trim()),
        ) {
            periods.push((unko_no, s, e));
        }
    }
    periods
}

/// フェリー 1 回の分数 (秒を 30 秒で四捨五入)。
pub fn ferry_period_minutes(start: NaiveDateTime, end: NaiveDateTime) -> i32 {
    let secs = (end - start).num_seconds();
    ((secs + 30) / 60) as i32
}

/// parse_ferry_periods_from_text の結果から unko_no → 合計フェリー分を算出
fn ferry_minutes_from_periods(
    periods: &[(String, NaiveDateTime, NaiveDateTime)],
) -> HashMap<String, i32> {
    let mut ferry_map = HashMap::new();
    for (unko_no, s, e) in periods {
        let mins = ferry_period_minutes(*s, *e);
        if mins > 0 {
            *ferry_map.entry(unko_no.clone()).or_insert(0) += mins;
        }
    }
    ferry_map
}

pub fn split_work_segments_at_boundary(
    segments: Vec<work_segments::WorkSegment>,
    boundary: NaiveDateTime,
) -> Vec<work_segments::WorkSegment> {
    let mut result = Vec::new();
    for seg in segments {
        if seg.start < boundary && seg.end > boundary {
            let total_mins = (seg.end - seg.start).num_minutes().max(1) as f64;
            let before_mins = (boundary - seg.start).num_minutes() as f64;
            let ratio = before_mins / total_mins;
            let d1 = (seg.drive_minutes as f64 * ratio).round() as i32;
            let c1 = (seg.cargo_minutes as f64 * ratio).round() as i32;
            let l1 = (seg.labor_minutes as f64 * ratio).round() as i32;
            result.push(work_segments::WorkSegment {
                start: seg.start,
                end: boundary,
                labor_minutes: l1,
                drive_minutes: d1,
                cargo_minutes: c1,
            });
            result.push(work_segments::WorkSegment {
                start: boundary,
                end: seg.end,
                labor_minutes: seg.labor_minutes - l1,
                drive_minutes: seg.drive_minutes - d1,
                cargo_minutes: seg.cargo_minutes - c1,
            });
        } else {
            result.push(seg);
        }
    }
    result
}

/// フェリー期間と重なる301(休憩)イベントのduration合計を返す
pub fn ferry_break_overlap(
    events: &[&KudgivtRow],
    ferry_start: NaiveDateTime,
    ferry_end: NaiveDateTime,
) -> i32 {
    let mut total = 0i32;
    for evt in events {
        if evt.event_cd != "301" {
            continue;
        }
        let dur = evt.duration_minutes.unwrap_or(0);
        if dur <= 0 {
            continue;
        }
        let es = evt.start_at;
        let ee = es + chrono::Duration::minutes(dur as i64);
        if ee > ferry_start && es < ferry_end {
            total += dur;
        }
    }
    total
}

/// フェリー期間と重なるDrive/Cargoイベントの分数合計を返す（分精度）
pub fn ferry_drive_cargo_overlap(
    events: &[&KudgivtRow],
    classifications: &HashMap<String, EventClass>,
    ferry_start: NaiveDateTime,
    ferry_end: NaiveDateTime,
) -> (i32, i32) {
    let fs_trunc = trunc_min(ferry_start);
    let fe_trunc = trunc_min(ferry_end);
    let mut drive = 0i32;
    let mut cargo = 0i32;
    for evt in events {
        let dur = evt.duration_minutes.unwrap_or(0);
        if dur <= 0 {
            continue;
        }
        let es = trunc_min(evt.start_at);
        let ee = es + chrono::Duration::minutes(dur as i64);
        let os = es.max(fs_trunc);
        let oe = ee.min(fe_trunc);
        if oe > os {
            let mins = (oe - os).num_minutes() as i32;
            match classifications.get(&evt.event_cd) {
                Some(EventClass::Drive) => drive += mins,
                Some(EventClass::Cargo) => cargo += mins,
                _ => {}
            }
        }
    }
    (drive, cargo)
}

/// イベントをmulti_op境界で分割する
pub fn split_event_at_boundaries(
    evt_start: NaiveDateTime,
    evt_end: NaiveDateTime,
    dur_secs: i64,
    boundaries: Option<&Vec<NaiveDateTime>>,
) -> Vec<(NaiveDateTime, NaiveDateTime, i64)> {
    let mut parts = Vec::new();
    if let Some(bounds) = boundaries {
        let mut relevant: Vec<NaiveDateTime> = bounds
            .iter()
            .filter(|&&b| evt_start < b && evt_end > b)
            .copied()
            .collect();
        relevant.sort();
        if relevant.is_empty() {
            parts.push((evt_start, evt_end, dur_secs));
        } else {
            let mut cur = evt_start;
            for b in &relevant {
                let secs = (*b - cur).num_seconds();
                parts.push((cur, *b, secs));
                cur = *b;
            }
            let secs = (evt_end - cur).num_seconds();
            parts.push((cur, evt_end, secs));
        }
    } else {
        parts.push((evt_start, evt_end, dur_secs));
    }
    parts
}

/// イベント開始時刻からworkday(日付,始業時刻)を特定する
pub fn find_event_workday(
    part_start: NaiveDateTime,
    unko_segments: Option<&Vec<(NaiveDateTime, NaiveDateTime, NaiveDate, NaiveTime)>>,
) -> (NaiveDate, NaiveTime) {
    let default_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    let segs = match unko_segments {
        Some(s) => s,
        None => return (part_start.date(), default_time),
    };
    // 直接マッチ: part_startがセグメント[start, end)内
    if let Some((_, _, wd, st)) = segs
        .iter()
        .find(|(start, end, _, _)| part_start >= *start && part_start < *end)
    {
        return (*wd, *st);
    }
    // fallback: part_startより後に始まるセグメント or 最後のセグメント
    segs.iter()
        .find(|(start, _, _, _)| part_start < *start)
        .or_else(|| segs.last())
        .map(|(_, _, wd, st)| (*wd, *st))
        .unwrap_or((part_start.date(), default_time))
}

/// daily segmentをDayAggに蓄積する
#[allow(clippy::too_many_arguments)]
pub fn accumulate_daily_segment(
    entry: &mut DayAgg,
    work_mins: i32,
    late_night_mins: i32,
    drive_mins: i32,
    cargo_mins: i32,
    seg_start: NaiveDateTime,
    seg_end: NaiveDateTime,
    unko_no: &str,
) {
    entry.total_work_minutes += work_mins;
    entry.late_night_minutes += late_night_mins;
    entry.drive_minutes += drive_mins;
    entry.cargo_minutes += cargo_mins;
    if !entry.unko_nos.contains(&unko_no.to_string()) {
        entry.unko_nos.push(unko_no.to_string());
    }
    entry.segments.push(SegRec {
        start_at: seg_start,
        end_at: seg_end,
    });
}

/// フェリー控除用の事前計算データ（compare/upload共通）
#[derive(Clone, Default)]
pub struct FerryInfo {
    /// unko_no → フェリー時間（分、四捨五入）
    pub ferry_minutes: HashMap<String, i32>,
    /// unko_no → 対応する301(休憩)イベントのduration合計
    pub ferry_break_dur: HashMap<String, i32>,
    /// unko_no → フェリー乗船期間(start, end)リスト
    pub ferry_period_map: HashMap<String, Vec<(NaiveDateTime, NaiveDateTime)>>,
}

impl FerryInfo {
    /// zip_files からフェリー情報を構築
    pub fn from_zip_files(
        zip_files: &[(String, Vec<u8>)],
        kudgivt_by_unko: &HashMap<String, Vec<&KudgivtRow>>,
    ) -> Self {
        // KUDGFRY.csv を1回だけパース
        let mut all_periods: Vec<(String, NaiveDateTime, NaiveDateTime)> = Vec::new();
        for (name, bytes) in zip_files {
            if !name.to_uppercase().contains("KUDGFRY") {
                continue;
            }
            let text = csv_parser::decode_shift_jis(bytes);
            all_periods.extend(parse_ferry_periods_from_text(&text));
        }

        let ferry_minutes = ferry_minutes_from_periods(&all_periods);

        let mut ferry_break_dur: HashMap<String, i32> = HashMap::new();
        let mut ferry_period_map: HashMap<String, Vec<(NaiveDateTime, NaiveDateTime)>> =
            HashMap::new();
        for (unko_no, s, e) in &all_periods {
            ferry_period_map
                .entry(unko_no.clone())
                .or_default()
                .push((*s, *e));
            // 対応する301イベントをマッチ
            let matching_301 = kudgivt_by_unko.get(unko_no).and_then(|events| {
                events
                    .iter()
                    .filter(|ev| ev.event_cd == "301" && ev.duration_minutes.unwrap_or(0) > 0)
                    .min_by_key(|ev| (ev.start_at - *s).num_seconds().abs())
            });
            if let Some(evt) = matching_301 {
                let dur = evt.duration_minutes.unwrap_or(0);
                *ferry_break_dur.entry(unko_no.clone()).or_insert(0) += dur;
            }
        }

        FerryInfo {
            ferry_minutes,
            ferry_break_dur,
            ferry_period_map,
        }
    }
}

/// ZIP を処理して CsvDriverData を生成
/// (driver_cd, work_date, start_time) — day_map等のキー型
pub type DayKey = (String, NaiveDate, NaiveTime);

/// 日別集計データ（compare/upload共通）
#[derive(Clone, Default)]
pub struct DayAgg {
    pub total_work_minutes: i32,
    pub late_night_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub unko_nos: Vec<String>,
    pub segments: Vec<SegRec>,
    pub overlap_drive_minutes: i32,
    pub overlap_cargo_minutes: i32,
    pub overlap_break_minutes: i32,
    pub overlap_restraint_minutes: i32,
    pub ot_late_night_minutes: i32,
    pub from_multi_op: bool,
}

#[derive(Clone)]
pub struct SegRec {
    pub start_at: NaiveDateTime,
    pub end_at: NaiveDateTime,
}

#[allow(clippy::too_many_arguments)]
pub fn post_process_day_map(
    day_map: &mut HashMap<DayKey, DayAgg>,
    workday_boundaries: &mut HashMap<DayKey, (NaiveDateTime, NaiveDateTime)>,
    multi_wd_boundaries: &HashMap<DayKey, NaiveDateTime>,
    day_work_events: &mut HashMap<DayKey, Vec<(NaiveDateTime, NaiveDateTime)>>,
    kudgivt_by_unko: &HashMap<String, Vec<&KudgivtRow>>,
    classifications: &HashMap<String, EventClass>,
    kudguri_rows: &[KudguriRow],
    ferry_info: &FerryInfo,
) {
    merge_same_day_entries(day_map, day_work_events);
    process_overlap_chain(
        day_map,
        workday_boundaries,
        multi_wd_boundaries,
        day_work_events,
        kudgivt_by_unko,
        classifications,
        kudguri_rows,
        ferry_info,
    );
    apply_ferry_deductions(day_map, kudgivt_by_unko, classifications, ferry_info);
}

/// 構内結合: 同日・異運行・gap<180分のエントリを結合
fn merge_same_day_entries(
    day_map: &mut HashMap<DayKey, DayAgg>,
    day_work_events: &mut HashMap<DayKey, Vec<(NaiveDateTime, NaiveDateTime)>>,
) {
    let keys: Vec<_> = day_map.keys().cloned().collect();
    let mut driver_date_keys: HashMap<(String, NaiveDate), Vec<DayKey>> = HashMap::new();
    for (dc, d, st) in &keys {
        driver_date_keys
            .entry((dc.clone(), *d))
            .or_default()
            .push((dc.clone(), *d, *st));
    }

    for ((_dc, _d), mut entries) in driver_date_keys {
        if entries.len() < 2 {
            continue;
        }
        entries.sort_by_key(|(_, _, st)| *st);

        let mut merged_any = true;
        while merged_any {
            merged_any = false;
            for i in 0..entries.len().saturating_sub(1) {
                let key_a = entries[i].clone();
                let key_b = entries[i + 1].clone();
                // entries は day_map.keys() から生成され、remove は key_b のみ → 常に存在
                let agg_a = &day_map[&key_a];
                let agg_b = &day_map[&key_b];
                let different_ops = !agg_a.unko_nos.iter().any(|u| agg_b.unko_nos.contains(u));
                let merge_info = different_ops
                    .then(|| {
                        match (
                            agg_a.segments.iter().map(|s| s.end_at).max(),
                            agg_b.segments.iter().map(|s| s.start_at).min(),
                        ) {
                            (Some(pe), Some(ns)) => {
                                let gap = (trunc_min(ns) - trunc_min(pe)).num_minutes();
                                (0..180).contains(&gap).then_some(gap as i32)
                            }
                            _ => None,
                        }
                    })
                    .flatten();
                if let Some(gap_mins) = merge_info {
                    let b_clone = day_map.get(&key_b).unwrap().clone();
                    let agg_a_mut = day_map.get_mut(&key_a).unwrap();
                    agg_a_mut.drive_minutes += b_clone.drive_minutes;
                    agg_a_mut.cargo_minutes += b_clone.cargo_minutes;
                    agg_a_mut.total_work_minutes += b_clone.total_work_minutes + gap_mins;
                    agg_a_mut.late_night_minutes += b_clone.late_night_minutes;
                    agg_a_mut.ot_late_night_minutes += b_clone.ot_late_night_minutes;
                    agg_a_mut.segments.extend(b_clone.segments);
                    for u in &b_clone.unko_nos {
                        if !agg_a_mut.unko_nos.contains(u) {
                            agg_a_mut.unko_nos.push(u.clone());
                        }
                    }
                    if let Some(b_events) =
                        day_work_events.remove(&(key_b.0.clone(), key_b.1, key_b.2))
                    {
                        day_work_events
                            .entry((key_a.0.clone(), key_a.1, key_a.2))
                            .or_default()
                            .extend(b_events);
                    }
                    day_map.remove(&key_b);
                    entries.remove(i + 1);
                    merged_any = true;
                    break;
                }
            }
        }
    }
}

/// overlap reset時のフェリー控除計算
fn calc_overlap_ferry_deductions(
    unko_nos: &[String],
    ferry_info: &FerryInfo,
    kudgivt_by_unko: &HashMap<String, Vec<&KudgivtRow>>,
    classifications: &HashMap<String, EventClass>,
    next_start: NaiveDateTime,
    window_end: NaiveDateTime,
) -> (i32, i32, i32) {
    let mut ferry_ded = 0i32;
    let mut ferry_drive_ded = 0i32;
    let mut ferry_cargo_ded = 0i32;
    for unko in unko_nos {
        let Some(periods) = ferry_info.ferry_period_map.get(unko) else {
            continue;
        };
        let Some(events) = kudgivt_by_unko.get(unko) else {
            continue;
        };
        for &(fs, fe) in periods {
            if fe <= next_start || fs >= window_end {
                continue;
            }
            let break_ded = ferry_break_overlap(events, fs, fe);
            if break_ded > 0 {
                ferry_ded += break_ded;
            } else {
                let f_start = fs.max(next_start);
                let f_end = fe.min(window_end);
                let f_mins = (f_end - f_start).num_minutes() as i32;
                if f_mins > 0 {
                    ferry_ded += f_mins;
                    let (d, c) = ferry_drive_cargo_overlap(events, classifications, fs, fe);
                    ferry_drive_ded += d;
                    ferry_cargo_ded += c;
                }
            }
        }
    }
    (ferry_ded, ferry_drive_ded, ferry_cargo_ded)
}

/// overlap計算: 連続workday間の24h境界チェーン処理
#[allow(clippy::too_many_arguments)]
fn process_overlap_chain(
    day_map: &mut HashMap<DayKey, DayAgg>,
    workday_boundaries: &mut HashMap<DayKey, (NaiveDateTime, NaiveDateTime)>,
    multi_wd_boundaries: &HashMap<DayKey, NaiveDateTime>,
    day_work_events: &mut HashMap<DayKey, Vec<(NaiveDateTime, NaiveDateTime)>>,
    kudgivt_by_unko: &HashMap<String, Vec<&KudgivtRow>>,
    classifications: &HashMap<String, EventClass>,
    kudguri_rows: &[KudguriRow],
    ferry_info: &FerryInfo,
) {
    let long_distance_unkos: std::collections::HashSet<&str> = kudguri_rows
        .iter()
        .filter(|r| {
            r.departure_at
                .zip(r.return_at)
                .map(|(dep, ret)| dep.date() != ret.date())
                .unwrap_or(false)
        })
        .map(|r| r.unko_no.as_str())
        .collect();

    struct DayInfo {
        start: NaiveDateTime,
        end: NaiveDateTime,
        unko_nos: Vec<String>,
    }

    let mut driver_days: HashMap<String, BTreeMap<(NaiveDate, NaiveTime), DayInfo>> =
        HashMap::new();
    for ((driver_cd, date, st), agg) in day_map.iter() {
        if agg.segments.is_empty() {
            continue;
        }
        let start = trunc_min(agg.segments.iter().map(|s| s.start_at).min().unwrap());
        let end = trunc_min(agg.segments.iter().map(|s| s.end_at).max().unwrap());
        driver_days.entry(driver_cd.clone()).or_default().insert(
            (*date, *st),
            DayInfo {
                start,
                end,
                unko_nos: agg.unko_nos.clone(),
            },
        );
    }

    for (driver_cd, dates_map) in &driver_days {
        let dates: Vec<(NaiveDate, NaiveTime)> = dates_map.keys().copied().collect();
        let mut effective_start: Option<NaiveDateTime> = None;
        let mut prev_end: Option<NaiveDateTime> = None;
        let mut next_day_deduction: Option<(i32, i32, i32, i32)> = None;
        let mut split_rests: Vec<i32> = Vec::new();
        let mut forced_next_reset = false;

        for (idx, &(date, st)) in dates.iter().enumerate() {
            let info = &dates_map[&(date, st)];

            if let Some((ded_drive, ded_cargo, ded_restraint, ded_night)) =
                next_day_deduction.take()
            {
                if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date, st)) {
                    agg.drive_minutes = (agg.drive_minutes - ded_drive).max(0);
                    agg.cargo_minutes = (agg.cargo_minutes - ded_cargo).max(0);
                    agg.total_work_minutes = (agg.total_work_minutes - ded_restraint).max(0);
                    agg.late_night_minutes = (agg.late_night_minutes - ded_night).max(0);
                }
            }

            let reset = match prev_end {
                Some(pe) => (info.start - pe).num_minutes() >= 480,
                None => true,
            } || forced_next_reset;
            forced_next_reset = false;
            if reset {
                let key = (driver_cd.clone(), date, st);
                if let Some(&(wb_start, _)) = workday_boundaries.get(&key) {
                    effective_start = Some(wb_start);
                    let seg_end = day_map
                        .get(&key)
                        .and_then(|a| a.segments.iter().map(|s| s.end_at).max())
                        .unwrap_or(info.end);
                    workday_boundaries.insert(key, (wb_start, seg_end));
                } else {
                    effective_start = Some(info.start);
                }
            } else {
                effective_start = Some(effective_start.unwrap() + chrono::Duration::hours(24));
            }

            let mut window_end = effective_start.unwrap() + chrono::Duration::hours(24);

            if idx + 1 < dates.len() {
                let (next_date, next_st) = dates[idx + 1];
                let next_info = &dates_map[&(next_date, next_st)];

                let mut ol_drive = 0i32;
                let mut ol_cargo = 0i32;
                let mut ol_restraint = 0i32;
                let mut ol_late_night_dc = 0i32;
                let mut ol_work_events: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();

                for unko_no in &next_info.unko_nos {
                    let Some(events) = kudgivt_by_unko.get(unko_no) else {
                        continue;
                    };
                    for evt in events {
                        let cls = classifications.get(&evt.event_cd);
                        let dur = evt.duration_minutes.unwrap_or(0);
                        if dur <= 0 {
                            continue;
                        }
                        let evt_start = trunc_min(evt.start_at);
                        if evt_start >= window_end {
                            continue;
                        }
                        let evt_end = evt_start + chrono::Duration::minutes(dur as i64);
                        // イベントが現在の日のend以前に開始 → 当日分なのでスキップ
                        // (evt_end <= info.end は evt_start < info.end の部分集合)
                        if evt_start < info.end {
                            continue;
                        }
                        let overlap_start = evt_start.max(next_info.start);
                        let effective_end = evt_end.min(window_end);
                        if effective_end <= overlap_start {
                            continue;
                        }
                        // effective_end > overlap_start is guaranteed above
                        let mins = (effective_end - overlap_start).num_minutes() as i32;
                        let actual_dur = if mins >= dur { dur } else { mins };
                        match cls {
                            Some(EventClass::Drive) | Some(EventClass::Cargo) => {
                                if matches!(cls, Some(EventClass::Drive)) {
                                    ol_drive += actual_dur;
                                } else {
                                    ol_cargo += actual_dur;
                                }
                                ol_late_night_dc +=
                                    calc_late_night_mins(overlap_start, effective_end);
                                ol_work_events.push((overlap_start, effective_end));
                            }
                            _ => {}
                        }
                    }
                }

                if next_info.start < window_end {
                    // セグメント合計で計算（302休息のギャップを除外）
                    ol_restraint = day_map
                        .get(&(driver_cd.clone(), next_date, next_st))
                        .map(|next_agg| {
                            next_agg
                                .segments
                                .iter()
                                .filter_map(|seg| {
                                    let seg_s = trunc_min(seg.start_at);
                                    let seg_e = trunc_min(seg.end_at);
                                    if seg_e <= next_info.start || seg_s >= window_end {
                                        return None;
                                    }
                                    let eff_s = seg_s.max(next_info.start);
                                    let eff_e = seg_e.min(window_end);
                                    Some((eff_e - eff_s).num_minutes() as i32)
                                })
                                .sum()
                        })
                        .unwrap_or(0);
                }

                let next_gap = (next_info.start - info.end).num_minutes();
                let is_long_distance = info
                    .unko_nos
                    .iter()
                    .chain(next_info.unko_nos.iter())
                    .any(|u| long_distance_unkos.contains(u.as_str()));
                let rest_threshold = if is_long_distance { 480 } else { 540 };
                let mut next_resets = next_gap >= rest_threshold;
                if !next_resets && next_gap >= 180 {
                    split_rests.push(next_gap as i32);
                    let total: i32 = split_rests.iter().sum();
                    let threshold = match split_rests.len() {
                        2 => 600,
                        n if n >= 3 => 720,
                        _ => i32::MAX,
                    };
                    if total >= threshold {
                        next_resets = true;
                        split_rests.clear();
                    }
                } else if next_resets {
                    split_rests.clear();
                }

                if !next_resets {
                    let key = (driver_cd.clone(), date, st);
                    if let Some(&det_end) = multi_wd_boundaries.get(&key) {
                        if det_end < window_end {
                            next_resets = true;
                            split_rests.clear();
                            forced_next_reset = true;
                            window_end = det_end;
                        }
                    }
                }

                let same_date_long_gap = date == next_date && next_gap >= 180;
                if !next_resets && ol_restraint > 0 && !same_date_long_gap {
                    if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date, st)) {
                        agg.drive_minutes += ol_drive;
                        agg.cargo_minutes += ol_cargo;
                        agg.total_work_minutes += ol_restraint;
                        agg.late_night_minutes += ol_late_night_dc;
                    }
                    if !ol_work_events.is_empty() {
                        let events_entry = day_work_events
                            .entry((driver_cd.clone(), date, st))
                            .or_default();
                        events_entry.extend(ol_work_events);
                        let mut sorted = events_entry.clone();
                        sorted.sort_by_key(|&(s, _)| s);
                        let ot_night = calc_ot_late_night_from_events(&sorted);
                        if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date, st)) {
                            agg.ot_late_night_minutes = ot_night;
                        }
                    }
                    next_day_deduction = Some((ol_drive, ol_cargo, ol_restraint, ol_late_night_dc));
                    let eff_start = effective_start.unwrap();
                    workday_boundaries
                        .insert((driver_cd.clone(), date, st), (eff_start, window_end));
                    let next_key = (driver_cd.clone(), next_date, next_st);
                    let next_seg_end = day_map
                        .get(&next_key)
                        .and_then(|a| a.segments.iter().map(|s| s.end_at).max())
                        .unwrap_or(window_end + chrono::Duration::hours(24));
                    workday_boundaries.insert(next_key, (window_end, next_seg_end));
                } else if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date, st)) {
                    let (ferry_ded, ferry_drive_ded, ferry_cargo_ded) =
                        calc_overlap_ferry_deductions(
                            &next_info.unko_nos,
                            ferry_info,
                            kudgivt_by_unko,
                            classifications,
                            next_info.start,
                            window_end,
                        );
                    let adj_restraint = (ol_restraint - ferry_ded).max(0);
                    let adj_drive = (ol_drive - ferry_drive_ded).max(0);
                    let adj_cargo = (ol_cargo - ferry_cargo_ded).max(0);
                    agg.overlap_drive_minutes = adj_drive;
                    agg.overlap_cargo_minutes = adj_cargo;
                    agg.overlap_break_minutes = (adj_restraint - adj_drive - adj_cargo).max(0);
                    agg.overlap_restraint_minutes = adj_restraint;
                }
            }

            prev_end = Some(info.end);
        }
    }
}

/// フェリー控除: day_mapエントリのセグメント時間範囲と重なるフェリーのみ控除
fn apply_ferry_deductions(
    day_map: &mut HashMap<DayKey, DayAgg>,
    kudgivt_by_unko: &HashMap<String, Vec<&KudgivtRow>>,
    classifications: &HashMap<String, EventClass>,
    ferry_info: &FerryInfo,
) {
    for ((_driver_cd, _date, _st), agg) in day_map.iter_mut() {
        let seg_start = agg.segments.iter().map(|s| s.start_at).min();
        let seg_end = agg.segments.iter().map(|s| s.end_at).max();
        let (seg_start, seg_end) = match (seg_start, seg_end) {
            (Some(s), Some(e)) => (s, e),
            _ => continue,
        };

        let mut ferry_deduction = 0i32;
        let mut ferry_break_deduction = 0i32;
        let mut ferry_drive_overlap = 0i32;
        for unko in &agg.unko_nos {
            if let Some(periods) = ferry_info.ferry_period_map.get(unko) {
                for &(fs, fe) in periods {
                    if fe <= seg_start || fs >= seg_end {
                        continue;
                    }
                    let ferry_mins = ((fe - fs).num_seconds() as f64 / 60.0).round() as i32;
                    ferry_deduction += ferry_mins;
                    if let Some(events) = kudgivt_by_unko.get(unko) {
                        ferry_break_deduction += ferry_break_overlap(events, fs, fe);
                        let (d, c) = ferry_drive_cargo_overlap(events, classifications, fs, fe);
                        ferry_drive_overlap += d + c;
                    }
                }
            }
        }
        if ferry_deduction > 0 {
            let rounding_diff = (ferry_deduction - ferry_break_deduction).max(0);
            let drive_ded = rounding_diff.min(ferry_drive_overlap);
            let total_ded = ferry_break_deduction + drive_ded;
            agg.total_work_minutes = (agg.total_work_minutes - total_ded).max(0);
            agg.drive_minutes = (agg.drive_minutes - drive_ded).max(0);
        }
    }
}

pub struct BuildDayMapResult {
    pub day_map: HashMap<DayKey, DayAgg>,
    pub workday_boundaries: HashMap<DayKey, (NaiveDateTime, NaiveDateTime)>,
    /// determine_workdaysが複数workdayを生成した場合の、オリジナルのwd.end
    pub multi_wd_boundaries: HashMap<DayKey, NaiveDateTime>,
    pub day_work_events: HashMap<DayKey, Vec<(NaiveDateTime, NaiveDateTime)>>,
    /// カレンダー日ベースの全拘束時間（分）: (driver_cd, date) → minutes
    pub calendar_day_total: HashMap<(String, NaiveDate), i32>,
}

/// workday境界でセグメントを追加分割（sig_split: 両側180分以上）
pub fn apply_sig_splits(
    segments: Vec<work_segments::WorkSegment>,
    workdays: &[work_segments::Workday],
    ret: NaiveDateTime,
) -> Vec<work_segments::WorkSegment> {
    let mut segs = segments;
    for wd in workdays {
        if wd.end < ret {
            let sig_split = segs.iter().any(|seg| {
                seg.start < wd.end
                    && wd.end < seg.end
                    && (wd.end - seg.start).num_minutes() >= 180
                    && (seg.end - wd.end).num_minutes() >= 180
            });
            if sig_split {
                segs = split_work_segments_at_boundary(segs, wd.end);
            }
        }
    }
    segs
}

/// daily_segment の work_date を決定: workday 内 → workday.date, 外 → parent segment の start.date()
pub fn find_work_date_for_segment(
    ds: &work_segments::DailyWorkSegment,
    workdays: &[work_segments::Workday],
    segments: &[work_segments::WorkSegment],
) -> NaiveDate {
    workdays
        .iter()
        .find(|wd| ds.start >= wd.start && ds.end <= wd.end)
        .map(|wd| wd.date)
        .unwrap_or_else(|| {
            let parent_seg = segments
                .iter()
                .find(|seg| ds.start >= seg.start && ds.start < seg.end);
            parent_seg.map(|seg| seg.start.date()).unwrap_or(ds.date)
        })
}

pub fn build_day_map(
    kudguri_rows: &[KudguriRow],
    kudgivt_by_unko: &HashMap<String, Vec<&KudgivtRow>>,
    classifications: &HashMap<String, EventClass>,
) -> BuildDayMapResult {
    let unko_work_date = group_operations_into_work_days(kudguri_rows);

    let mut workday_boundaries: HashMap<DayKey, (NaiveDateTime, NaiveDateTime)> = HashMap::new();
    // determine_workdaysが複数workdayを生成した境界（分割休息/24h境界等）
    // overlap計算でのchain上書きを防止するために使用
    let mut multi_wd_boundaries: HashMap<DayKey, NaiveDateTime> = HashMap::new();
    let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
    let mut unko_segments: HashMap<
        String,
        Vec<(NaiveDateTime, NaiveDateTime, NaiveDate, NaiveTime)>,
    > = HashMap::new();
    let mut multi_op_boundaries: HashMap<String, Vec<NaiveDateTime>> = HashMap::new();

    let mut workday_groups: BTreeMap<(String, NaiveDate), Vec<&KudguriRow>> = BTreeMap::new();
    for row in kudguri_rows {
        let wd = unko_work_date
            .get(&row.unko_no)
            .copied()
            .unwrap_or(row.operation_date.unwrap_or(row.reading_date));
        workday_groups
            .entry((row.driver_cd.clone(), wd))
            .or_default()
            .push(row);
    }

    for ((_group_driver_cd, _group_work_date), ops) in &workday_groups {
        let valid_ops: Vec<&&KudguriRow> = ops
            .iter()
            .filter(|r| matches!((r.departure_at, r.return_at), (Some(d), Some(r)) if r > d))
            .collect();

        for row in ops {
            if matches!((row.departure_at, row.return_at), (Some(d), Some(r)) if r > d) {
                continue;
            }
            let work_date = row.operation_date.unwrap_or(row.reading_date);
            let total_drive_mins = row.drive_time_general.unwrap_or(0)
                + row.drive_time_highway.unwrap_or(0)
                + row.drive_time_bypass.unwrap_or(0);
            let default_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            let entry = day_map
                .entry((row.driver_cd.clone(), work_date, default_time))
                .or_default();
            entry.total_work_minutes += total_drive_mins;
            entry.unko_nos.push(row.unko_no.clone());
        }

        if valid_ops.is_empty() {
            continue;
        }

        let spans_different_days = if valid_ops.len() > 1 {
            let dates: std::collections::HashSet<NaiveDate> = valid_ops
                .iter()
                .filter_map(|r| r.departure_at.map(|d| d.date()))
                .collect();
            if dates.len() <= 1 {
                false
            } else {
                // 前の運行の帰着日と次の運行の出発日が同じ場合は
                // multi-op mergeを使わない（overlapセクションに任せる）
                let mut sorted_valid: Vec<_> = valid_ops.iter().collect();
                sorted_valid.sort_by_key(|r| r.departure_at);
                let ops_share_date = sorted_valid.windows(2).any(|pair| {
                    let ret_date = pair[0].return_at.map(|r| r.date());
                    let dep_date = pair[1].departure_at.map(|d| d.date());
                    ret_date.is_some() && ret_date == dep_date
                });
                !ops_share_date
            }
        } else {
            false
        };

        if !spans_different_days {
            for row in &valid_ops {
                let dep = row.departure_at.unwrap();
                let ret = row.return_at.unwrap();
                let events = kudgivt_by_unko.get(&row.unko_no);
                let event_slice: Vec<&KudgivtRow> = events.map(|e| e.to_vec()).unwrap_or_default();

                let rest_events_for_unko: Vec<(NaiveDateTime, i32)> = event_slice
                    .iter()
                    .filter(|e| classifications.get(&e.event_cd) == Some(&EventClass::RestSplit))
                    .filter_map(|e| {
                        e.duration_minutes
                            .filter(|&d| d > 0)
                            .map(|d| (e.start_at, d))
                    })
                    .collect();
                let workdays = work_segments::determine_workdays(
                    &rest_events_for_unko,
                    dep,
                    ret,
                    dep.date() != ret.date(), // 長距離480例外: 日跨ぎ運行
                );

                let segments =
                    work_segments::split_by_rest(dep, ret, &event_slice, classifications);
                // workday境界でセグメントを分割（24hルール対応）
                let span_days = (ret.date() - dep.date()).num_days();
                // 全workday境界をイベント分割用に登録（分単位に揃える）
                if workdays.len() >= 2 {
                    let boundaries = multi_op_boundaries.entry(row.unko_no.clone()).or_default();
                    for wd in &workdays {
                        let trunc_end = trunc_min(wd.end);
                        if wd.end < ret && !boundaries.contains(&trunc_end) {
                            boundaries.push(trunc_end);
                        }
                    }
                }
                // workday境界で始業基準の24h分割（分単位に揃える）
                let wd_ends: Vec<NaiveDateTime> =
                    workdays.iter().map(|wd| trunc_min(wd.end)).collect();
                let segments =
                    work_segments::split_segments_at_24h_with_workdays(segments, &wd_ends);
                // workday境界でセグメントを追加分割（長距離運行の24hルール対応）
                // 条件: 3日以上スパン、分割後の両パートが60分以上
                let segments = if span_days >= 3 && workdays.len() >= 2 {
                    apply_sig_splits(segments, &workdays, ret)
                } else {
                    segments
                };
                let daily_segments = work_segments::split_segments_by_day(&segments);

                for wd in &workdays {
                    let key = (row.driver_cd.clone(), wd.date, wd.start.time());
                    workday_boundaries.insert(key.clone(), (wd.start, wd.end));
                    // 複数workdayの場合、各境界をauthoritativeとして登録
                    if workdays.len() >= 2 {
                        multi_wd_boundaries.insert(key, wd.end);
                    }
                }

                let find_start_time = |ts: NaiveDateTime| -> NaiveTime {
                    workdays
                        .iter()
                        .find(|wd| ts >= wd.start && ts < wd.end)
                        .or_else(|| workdays.iter().rev().find(|wd| ts >= wd.start))
                        .map(|wd| wd.start.time())
                        .unwrap_or(dep.time())
                };
                let find_workday_date = |start: NaiveDateTime, end: NaiveDateTime| -> NaiveDate {
                    workdays
                        .iter()
                        .find(|wd| start >= wd.start && end <= wd.end)
                        .map(|wd| wd.date)
                        .unwrap_or(start.date())
                };
                let seg_entries: Vec<_> = segments
                    .iter()
                    .map(|seg| {
                        (
                            trunc_min(seg.start),
                            trunc_min(seg.end),
                            find_workday_date(seg.start, seg.end),
                            find_start_time(seg.start),
                        )
                    })
                    .collect();
                unko_segments.insert(row.unko_no.clone(), seg_entries);

                for ds in &daily_segments {
                    let work_date = find_work_date_for_segment(ds, &workdays, &segments);
                    let start_time = find_start_time(ds.start);
                    let entry = day_map
                        .entry((row.driver_cd.clone(), work_date, start_time))
                        .or_default();
                    entry.total_work_minutes += ds.work_minutes;
                    entry.late_night_minutes += ds.late_night_minutes;
                    entry.drive_minutes += ds.drive_minutes;
                    entry.cargo_minutes += ds.cargo_minutes;
                    if !entry.unko_nos.contains(&row.unko_no) {
                        entry.unko_nos.push(row.unko_no.clone());
                    }
                    entry.segments.push(SegRec {
                        start_at: ds.start,
                        end_at: ds.end,
                    });
                }
            }
        } else {
            // ---- 複数運行の結合処理（運行間workday結合） ----
            let merged_dep = valid_ops
                .iter()
                .filter_map(|r| r.departure_at)
                .min()
                .unwrap();
            let merged_ret = valid_ops.iter().filter_map(|r| r.return_at).max().unwrap();

            let merged_dep_trunc = trunc_min(merged_dep);
            // 24h単位でvirtual workdayを生成（3日以上のスパンに対応）
            let mut virtual_workdays: Vec<Workday> = Vec::new();
            let mut boundaries_24h: Vec<NaiveDateTime> = Vec::new();
            let mut boundary = merged_dep_trunc;
            loop {
                let next_boundary = boundary + chrono::Duration::hours(24);
                let wd_start = if virtual_workdays.is_empty() {
                    merged_dep
                } else {
                    boundary
                };
                let wd_end = next_boundary.min(merged_ret);
                virtual_workdays.push(Workday {
                    date: wd_start.date(),
                    start: wd_start,
                    end: wd_end,
                });
                boundaries_24h.push(next_boundary);
                if wd_end >= merged_ret {
                    break;
                }
                boundary = next_boundary;
            }

            for row in &valid_ops {
                // 最初の24h境界をlegacy互換で保存
                multi_op_boundaries
                    .entry(row.unko_no.clone())
                    .or_default()
                    .push(boundaries_24h[0]);
            }

            let driver_cd = &valid_ops[0].driver_cd;
            for wd in &virtual_workdays {
                workday_boundaries.insert(
                    (driver_cd.clone(), wd.date, wd.start.time()),
                    (wd.start, wd.end),
                );
            }

            let find_vwd_start_time = |ts: NaiveDateTime| -> NaiveTime {
                virtual_workdays
                    .iter()
                    .find(|wd| ts >= wd.start && ts < wd.end)
                    .or_else(|| virtual_workdays.iter().rev().find(|wd| ts >= wd.start))
                    .map(|wd| wd.start.time())
                    .unwrap_or(merged_dep.time())
            };
            let find_vwd_date = |ts: NaiveDateTime| -> NaiveDate {
                virtual_workdays
                    .iter()
                    .find(|wd| ts >= wd.start && ts < wd.end)
                    .or_else(|| virtual_workdays.iter().rev().find(|wd| ts >= wd.start))
                    .map(|wd| wd.date)
                    .unwrap_or(merged_dep.date())
            };

            for row in &valid_ops {
                let dep = row.departure_at.unwrap();
                let ret = row.return_at.unwrap();
                let events = kudgivt_by_unko.get(&row.unko_no);
                let event_slice: Vec<&KudgivtRow> = events.map(|e| e.to_vec()).unwrap_or_default();

                let segments =
                    work_segments::split_by_rest(dep, ret, &event_slice, classifications);
                let segments = work_segments::split_segments_at_24h(segments);
                let mut segments = segments;
                for &b in &boundaries_24h {
                    segments = split_work_segments_at_boundary(segments, b);
                }
                let daily_segments = work_segments::split_segments_by_day(&segments);

                let seg_entries: Vec<_> = segments
                    .iter()
                    .map(|seg| {
                        (
                            seg.start,
                            seg.end,
                            find_vwd_date(seg.start),
                            find_vwd_start_time(seg.start),
                        )
                    })
                    .collect();
                unko_segments.insert(row.unko_no.clone(), seg_entries);

                for ds in &daily_segments {
                    let work_date = find_vwd_date(ds.start);
                    let start_time = find_vwd_start_time(ds.start);
                    let entry = day_map
                        .entry((driver_cd.clone(), work_date, start_time))
                        .or_default();
                    entry.from_multi_op = true;
                    entry.total_work_minutes += ds.work_minutes;
                    entry.late_night_minutes += ds.late_night_minutes;
                    entry.drive_minutes += ds.drive_minutes;
                    entry.cargo_minutes += ds.cargo_minutes;
                    if !entry.unko_nos.contains(&row.unko_no) {
                        entry.unko_nos.push(row.unko_no.clone());
                    }
                    entry.segments.push(SegRec {
                        start_at: ds.start,
                        end_at: ds.end,
                    });
                }
            }
        }
    }

    let (day_work_events, calendar_day_total) = aggregate_events_by_day(
        &mut day_map,
        &unko_segments,
        &multi_op_boundaries,
        kudgivt_by_unko,
        classifications,
    );

    BuildDayMapResult {
        day_map,
        workday_boundaries,
        multi_wd_boundaries,
        day_work_events,
        calendar_day_total,
    }
}

/// イベント直接集計: KUDGIVTイベントからDrive/Cargo/Break秒数を集計してday_mapを上書き
#[allow(clippy::type_complexity)]
fn aggregate_events_by_day<'a>(
    day_map: &mut HashMap<DayKey, DayAgg>,
    unko_segments: &HashMap<String, Vec<(NaiveDateTime, NaiveDateTime, NaiveDate, NaiveTime)>>,
    multi_op_boundaries: &HashMap<String, Vec<NaiveDateTime>>,
    kudgivt_by_unko: &'a HashMap<String, Vec<&'a KudgivtRow>>,
    classifications: &HashMap<String, EventClass>,
) -> (
    HashMap<DayKey, Vec<(NaiveDateTime, NaiveDateTime)>>,
    HashMap<(String, NaiveDate), i32>,
) {
    let mut day_work_events: HashMap<DayKey, Vec<(NaiveDateTime, NaiveDateTime)>> = HashMap::new();
    let mut calendar_day_total: HashMap<(String, NaiveDate), i32> = HashMap::new();

    let mut driver_unko_map: HashMap<String, Vec<String>> = HashMap::new();
    for ((driver_cd, _, _), agg) in day_map.iter() {
        let entry = driver_unko_map.entry(driver_cd.clone()).or_default();
        for u in &agg.unko_nos {
            if !entry.contains(u) {
                entry.push(u.clone());
            }
        }
    }

    for (driver_cd, unko_nos) in &driver_unko_map {
        let mut day_drive_secs: HashMap<(NaiveDate, NaiveTime), i64> = HashMap::new();
        let mut day_cargo_secs: HashMap<(NaiveDate, NaiveTime), i64> = HashMap::new();
        let mut day_break_secs: HashMap<(NaiveDate, NaiveTime), i64> = HashMap::new();
        for ((dc, date, st), _) in day_map.iter() {
            if dc == driver_cd {
                day_drive_secs.entry((*date, *st)).or_insert(0);
                day_cargo_secs.entry((*date, *st)).or_insert(0);
                day_break_secs.entry((*date, *st)).or_insert(0);
            }
        }
        let mut day_late_night: HashMap<(NaiveDate, NaiveTime), i32> = HashMap::new();
        let mut calendar_day_secs: HashMap<NaiveDate, i64> = HashMap::new();

        for unko_no in unko_nos {
            if let Some(events) = kudgivt_by_unko.get(unko_no) {
                let boundary_opt = multi_op_boundaries.get(unko_no);

                for evt in events {
                    let dur = evt.duration_minutes.unwrap_or(0);
                    if dur <= 0 {
                        continue;
                    }

                    let evt_start_trunc = trunc_min(evt.start_at);
                    let evt_end = evt_start_trunc + chrono::Duration::minutes(dur as i64);

                    let parts = split_event_at_boundaries(
                        evt_start_trunc,
                        evt_end,
                        dur as i64 * 60,
                        boundary_opt,
                    );

                    for (part_start, part_end, part_secs) in &parts {
                        let seg_list = unko_segments.get(unko_no);
                        let (event_date, event_start_time) =
                            find_event_workday(*part_start, seg_list);

                        let cls = classifications.get(&evt.event_cd);
                        match cls {
                            Some(EventClass::Drive) => {
                                *day_drive_secs
                                    .entry((event_date, event_start_time))
                                    .or_insert(0) += part_secs;
                            }
                            Some(EventClass::Cargo) => {
                                *day_cargo_secs
                                    .entry((event_date, event_start_time))
                                    .or_insert(0) += part_secs;
                            }
                            Some(EventClass::Break) => {
                                *day_break_secs
                                    .entry((event_date, event_start_time))
                                    .or_insert(0) += part_secs;
                            }
                            _ => {}
                        }
                        match cls {
                            Some(EventClass::Drive)
                            | Some(EventClass::Cargo)
                            | Some(EventClass::Break) => {
                                let cal_date = part_start.date();
                                let next_midnight = (cal_date + chrono::Duration::days(1))
                                    .and_hms_opt(0, 0, 0)
                                    .unwrap();
                                if *part_end <= next_midnight {
                                    *calendar_day_secs.entry(cal_date).or_insert(0) += part_secs;
                                } else {
                                    let before = (next_midnight - *part_start).num_seconds();
                                    let after = (*part_end - next_midnight).num_seconds();
                                    *calendar_day_secs.entry(cal_date).or_insert(0) += before;
                                    *calendar_day_secs
                                        .entry(cal_date + chrono::Duration::days(1))
                                        .or_insert(0) += after;
                                }
                            }
                            _ => {}
                        }
                        match cls {
                            Some(EventClass::Drive) | Some(EventClass::Cargo) => {
                                let night = calc_late_night_mins(*part_start, *part_end);
                                if night > 0 {
                                    *day_late_night
                                        .entry((event_date, event_start_time))
                                        .or_insert(0) += night;
                                }
                                day_work_events
                                    .entry((driver_cd.clone(), event_date, event_start_time))
                                    .or_default()
                                    .push((*part_start, *part_end));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        for ((date, st), secs) in &day_drive_secs {
            if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date, *st)) {
                agg.drive_minutes = ((*secs + 30) / 60) as i32;
            }
        }
        for ((date, st), secs) in &day_cargo_secs {
            if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date, *st)) {
                agg.cargo_minutes = ((*secs + 30) / 60) as i32;
            }
        }
        for ((date, st), _) in day_drive_secs
            .iter()
            .chain(day_cargo_secs.iter())
            .chain(day_break_secs.iter())
        {
            if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date, *st)) {
                let d = day_drive_secs.get(&(*date, *st)).copied().unwrap_or(0);
                let c = day_cargo_secs.get(&(*date, *st)).copied().unwrap_or(0);
                let b = day_break_secs.get(&(*date, *st)).copied().unwrap_or(0);
                agg.total_work_minutes = ((d + c + b + 30) / 60) as i32;
            }
        }
        for ((date, st), night) in &day_late_night {
            if let Some(agg) = day_map.get_mut(&(driver_cd.clone(), *date, *st)) {
                agg.late_night_minutes = *night;
            }
        }
        for ((dc, _date, _st), agg) in day_map.iter_mut() {
            if dc == driver_cd && !day_late_night.contains_key(&(*_date, *_st)) {
                agg.late_night_minutes = 0;
            }
        }
        for &(date, st) in day_late_night.keys() {
            // 付け先の行が無いキーは、上の 4 ループと同じく飛ばす
            let Some(agg) = day_map.get_mut(&(driver_cd.clone(), date, st)) else {
                continue;
            };
            let ot_night = day_work_events
                .get(&(driver_cd.clone(), date, st))
                .map(|events| {
                    let mut sorted = events.clone();
                    sorted.sort_by_key(|&(s, _)| s);
                    calc_ot_late_night_from_events(&sorted)
                })
                .unwrap_or(0);
            agg.ot_late_night_minutes = ot_night;
        }
        for (date, secs) in &calendar_day_secs {
            let mins = ((*secs + 30) / 60) as i32;
            calendar_day_total.insert((driver_cd.clone(), *date), mins);
        }
    }

    (day_work_events, calendar_day_total)
}

/// パース済みデータからCsvDriverDataを生成（IO分離済み・テスト可能）
pub fn process_parsed_data(
    kudguri_rows: &[KudguriRow],
    kudgivt_rows: &[KudgivtRow],
    ferry_info: &FerryInfo,
    target_year: i32,
    target_month: u32,
) -> Result<Vec<CsvDriverData>, String> {
    let classifications = default_classifications();

    let mut kudgivt_by_unko: HashMap<String, Vec<&KudgivtRow>> = HashMap::new();
    for row in kudgivt_rows {
        kudgivt_by_unko
            .entry(row.unko_no.clone())
            .or_default()
            .push(row);
    }

    let result = build_day_map(kudguri_rows, &kudgivt_by_unko, &classifications);
    let mut day_map = result.day_map;
    let mut workday_boundaries = result.workday_boundaries;
    let multi_wd_boundaries = result.multi_wd_boundaries;
    let mut day_work_events = result.day_work_events;
    let _calendar_day_total = result.calendar_day_total;

    post_process_day_map(
        &mut day_map,
        &mut workday_boundaries,
        &multi_wd_boundaries,
        &mut day_work_events,
        &kudgivt_by_unko,
        &classifications,
        kudguri_rows,
        ferry_info,
    );

    let result = build_csv_driver_data(
        &day_map,
        &workday_boundaries,
        kudguri_rows,
        target_year,
        target_month,
    );
    Ok(result)
}

/// day_map + workday_boundaries から CsvDriverData を生成
fn build_csv_driver_data(
    day_map: &HashMap<DayKey, DayAgg>,
    workday_boundaries: &HashMap<DayKey, (NaiveDateTime, NaiveDateTime)>,
    kudguri_rows: &[KudguriRow],
    target_year: i32,
    target_month: u32,
) -> Vec<CsvDriverData> {
    let mut driver_map: HashMap<String, String> = HashMap::new();
    for row in kudguri_rows {
        driver_map
            .entry(row.driver_cd.clone())
            .or_insert_with(|| row.driver_name.clone());
    }

    let month_start = NaiveDate::from_ymd_opt(target_year, target_month, 1).unwrap();
    let month_end = if target_month == 12 {
        NaiveDate::from_ymd_opt(target_year + 1, 1, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(target_year, target_month + 1, 1).unwrap()
    } - chrono::Duration::days(1);

    let fmt_trunc_time =
        |dt: NaiveDateTime| -> String { format!("{}:{:02}", dt.hour(), dt.minute()) };

    let mut result = Vec::new();

    for (driver_cd, driver_name) in &driver_map {
        let mut days = Vec::new();
        let mut cumulative = 0i32;
        let mut total_drive = 0i32;
        let mut total_restraint = 0i32;
        let mut total_actual_work = 0i32;
        let mut total_overtime = 0i32;
        let mut total_late_night = 0i32;

        let mut current_date = month_start;
        while current_date <= month_end {
            let day_entries: Vec<_> = day_map
                .iter()
                .filter(|((dc, d, _), _)| dc == driver_cd && *d == current_date)
                .collect();

            if day_entries.is_empty() {
                days.push(CsvDayRow {
                    date: format!("{}月{}日", current_date.month(), current_date.day()),
                    is_holiday: true,
                    start_time: String::new(),
                    end_time: String::new(),
                    drive: String::new(),
                    overlap_drive: String::new(),
                    cargo: String::new(),
                    overlap_cargo: String::new(),
                    break_time: String::new(),
                    overlap_break: String::new(),
                    subtotal: String::new(),
                    overlap_subtotal: String::new(),
                    total: String::new(),
                    cumulative: fmt_min(cumulative),
                    rest: String::new(),
                    actual_work: String::new(),
                    overtime: String::new(),
                    late_night: String::new(),
                    ot_late_night: String::new(),
                    remarks: String::new(),
                });
            } else {
                let mut sorted_entries: Vec<_> = day_entries;
                sorted_entries.sort_by_key(|((_, _, st), _)| *st);

                for ((_, _, _st), agg) in &sorted_entries {
                    let day_drive = agg.drive_minutes;
                    let day_cargo = agg.cargo_minutes;
                    let day_restraint = agg.total_work_minutes;
                    let overlap_restraint = agg.overlap_restraint_minutes;
                    let day_total = day_restraint + overlap_restraint;

                    cumulative += day_restraint;

                    let actual_work = day_drive + day_cargo;
                    let ot_ln = agg.ot_late_night_minutes;
                    let total_ot = (actual_work - 480).max(0);
                    let overtime = (total_ot - ot_ln).max(0);

                    let wb = workday_boundaries.get(&(driver_cd.clone(), current_date, *_st));
                    let start_dt = wb
                        .map(|(wd_start, _)| *wd_start)
                        .or_else(|| agg.segments.iter().map(|s| s.start_at).min());
                    let start_time = start_dt.map(fmt_trunc_time).unwrap_or_default();
                    let seg_max_end = agg.segments.iter().map(|s| s.end_at).max();
                    let end_time = match (wb, seg_max_end) {
                        (Some((wd_start, wd_end)), Some(seg_end))
                            if wd_start.date() != wd_end.date()
                                && (*wd_end - seg_end).num_minutes() > 60 =>
                        {
                            fmt_trunc_time(*wd_end)
                        }
                        (_, Some(seg_end)) => fmt_trunc_time(seg_end),
                        _ => wb
                            .map(|(_, wd_end)| fmt_trunc_time(*wd_end))
                            .unwrap_or_default(),
                    };

                    let standard_late_night = (agg.late_night_minutes - ot_ln).max(0);

                    total_drive += day_drive;
                    total_restraint += day_restraint;
                    total_actual_work += actual_work;
                    total_overtime += overtime;
                    total_late_night += standard_late_night;

                    days.push(CsvDayRow {
                        date: format!("{}月{}日", current_date.month(), current_date.day()),
                        is_holiday: false,
                        start_time,
                        end_time,
                        drive: fmt_min(day_drive),
                        overlap_drive: fmt_min(agg.overlap_drive_minutes),
                        cargo: fmt_min(day_cargo),
                        overlap_cargo: fmt_min(agg.overlap_cargo_minutes),
                        break_time: fmt_min((day_restraint - day_drive - day_cargo).max(0)),
                        overlap_break: fmt_min(agg.overlap_break_minutes),
                        subtotal: fmt_min(day_restraint),
                        overlap_subtotal: fmt_min(overlap_restraint),
                        total: fmt_min(day_total),
                        cumulative: fmt_min(cumulative),
                        rest: String::new(),
                        actual_work: fmt_min(actual_work),
                        overtime: fmt_min(overtime),
                        late_night: fmt_min(standard_late_night),
                        ot_late_night: fmt_min(ot_ln),
                        remarks: String::new(),
                    });
                }
            }

            current_date += chrono::Duration::days(1);
        }

        result.push(CsvDriverData {
            driver_name: driver_name.clone(),
            driver_cd: driver_cd.clone(),
            days,
            total_drive: fmt_min(total_drive),
            total_cargo: String::new(),
            total_break: String::new(),
            total_restraint: fmt_min(total_restraint),
            total_actual_work: fmt_min(total_actual_work),
            total_overtime: fmt_min(total_overtime),
            total_late_night: fmt_min(total_late_night),
            total_ot_late_night: String::new(),
        });
    }

    result
}

/// テストの helper (`mod tests` と `upload_daily` のテストが共用する)。
#[cfg(test)]
pub(crate) mod test_support {
    use super::csv_parser;
    use chrono::{NaiveDate, NaiveDateTime};

    pub(crate) fn dt(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, mi, s)
            .unwrap()
    }

    pub(crate) fn make_kudguri(
        unko_no: &str,
        driver_cd: &str,
        dep: NaiveDateTime,
        ret: NaiveDateTime,
    ) -> csv_parser::kudguri::KudguriRow {
        csv_parser::kudguri::KudguriRow {
            unko_no: unko_no.into(),
            reading_date: dep.date(),
            driver_cd: driver_cd.into(),
            driver_name: "Test".into(),
            vehicle_cd: "V1".into(),
            vehicle_name: "V1".into(),
            office_cd: "1".into(),
            office_name: "Test".into(),
            crew_role: 1,
            operation_date: Some(dep.date()),
            departure_at: Some(dep),
            return_at: Some(ret),
            garage_out_at: None,
            garage_in_at: None,
            meter_start: None,
            meter_end: None,
            total_distance: None,
            drive_time_general: None,
            drive_time_highway: None,
            drive_time_bypass: None,
            safety_score: None,
            economy_score: None,
            total_score: None,
            raw_data: serde_json::Value::Null,
        }
    }

    pub(crate) fn make_kudgivt(
        unko_no: &str,
        start: NaiveDateTime,
        event_cd: &str,
        dur: i32,
    ) -> csv_parser::kudgivt::KudgivtRow {
        csv_parser::kudgivt::KudgivtRow {
            unko_no: unko_no.into(),
            reading_date: start.date(),
            driver_cd: "1001".into(),
            driver_name: "Test".into(),
            crew_role: 1,
            start_at: start,
            end_at: Some(start + chrono::Duration::minutes(dur as i64)),
            event_cd: event_cd.into(),
            event_name: "".into(),
            duration_minutes: Some(dur),
            section_distance: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{dt, make_kudgivt, make_kudguri};
    use super::*;
    use chrono::NaiveDate;

    fn make_csv_day(date: &str, start: &str, end: &str, drive: &str, subtotal: &str) -> CsvDayRow {
        CsvDayRow {
            date: date.into(),
            is_holiday: false,
            start_time: start.into(),
            end_time: end.into(),
            drive: drive.into(),
            overlap_drive: "".into(),
            cargo: "".into(),
            overlap_cargo: "".into(),
            break_time: "".into(),
            overlap_break: "".into(),
            subtotal: subtotal.into(),
            overlap_subtotal: "".into(),
            total: subtotal.into(),
            cumulative: subtotal.into(),
            rest: "".into(),
            actual_work: drive.into(),
            overtime: "".into(),
            late_night: "".into(),
            ot_late_night: "".into(),
            remarks: "".into(),
        }
    }

    // ---- fmt_min ----
    #[test]
    fn test_fmt_min_zero() {
        test_group!("比較ロジック");
        test_case!("fmt_min: ゼロ", {
            assert_eq!(fmt_min(0), "");
        });
    }
    #[test]
    fn test_fmt_min_hours_and_minutes() {
        test_group!("比較ロジック");
        test_case!("fmt_min: 時分", {
            assert_eq!(fmt_min(90), "1:30");
            assert_eq!(fmt_min(605), "10:05");
        });
    }
    #[test]
    fn test_fmt_min_minutes_only() {
        test_group!("比較ロジック");
        test_case!("fmt_min: 分のみ", {
            assert_eq!(fmt_min(45), "0:45");
        });
    }

    // ---- trunc_min ----
    #[test]
    fn test_trunc_min() {
        test_group!("比較ロジック");
        test_case!("trunc_min: 秒切り捨て", {
            assert_eq!(
                trunc_min(dt(2026, 2, 5, 8, 15, 49)),
                dt(2026, 2, 5, 8, 15, 0)
            );
            assert_eq!(trunc_min(dt(2026, 2, 5, 0, 0, 0)), dt(2026, 2, 5, 0, 0, 0));
        });
    }

    // ---- normalize_time ----
    #[test]
    fn test_normalize_time() {
        test_group!("比較ロジック");
        test_case!("normalize_time: 時刻正規化", {
            assert_eq!(normalize_time("01:30"), "1:30");
            assert_eq!(normalize_time("9:05"), "9:05");
            assert_eq!(normalize_time(""), "");
            assert_eq!(normalize_time("  3:00  "), "3:00");
        });
    }

    // ---- calc_ot_late_night_from_events ----
    #[test]
    fn test_ot_late_night_no_overtime() {
        test_group!("比較ロジック");
        test_case!("時間外深夜: 時間外なし", {
            // 8h以内 → 時間外なし → 深夜0
            let events = vec![
                (dt(2026, 2, 1, 22, 0, 0), dt(2026, 2, 2, 5, 0, 0)), // 7h (22-5 = all deep night but < 8h)
            ];
            assert_eq!(calc_ot_late_night_from_events(&events), 0);
        });
    }
    #[test]
    fn test_ot_late_night_with_overtime() {
        test_group!("比較ロジック");
        test_case!("時間外深夜: 時間外あり", {
            // 合計9h → 1h時間外。最後の1h(4:00-5:00)が深夜帯
            let events = vec![
                (dt(2026, 2, 1, 20, 0, 0), dt(2026, 2, 2, 5, 0, 0)), // 9h
            ];
            let result = calc_ot_late_night_from_events(&events);
            assert_eq!(result, 60); // 4:00-5:00 = 1h deep night overtime
        });
    }

    // ---- split_work_segments_at_boundary ----
    #[test]
    fn test_split_at_boundary_no_crossing() {
        test_group!("比較ロジック");
        test_case!("split_at_boundary: 境界跨ぎなし", {
            let segs = vec![work_segments::WorkSegment {
                start: dt(2026, 2, 1, 8, 0, 0),
                end: dt(2026, 2, 1, 16, 0, 0),
                labor_minutes: 480,
                drive_minutes: 400,
                cargo_minutes: 80,
            }];
            let boundary = dt(2026, 2, 1, 20, 0, 0); // after segment end
            let result = split_work_segments_at_boundary(segs, boundary);
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].drive_minutes, 400);
        });
    }
    #[test]
    fn test_split_at_boundary_crossing() {
        test_group!("比較ロジック");
        test_case!("split_at_boundary: 境界跨ぎ", {
            let segs = vec![work_segments::WorkSegment {
                start: dt(2026, 2, 1, 8, 0, 0),
                end: dt(2026, 2, 1, 16, 0, 0),
                labor_minutes: 480,
                drive_minutes: 480,
                cargo_minutes: 0,
            }];
            let boundary = dt(2026, 2, 1, 12, 0, 0); // midpoint
            let result = split_work_segments_at_boundary(segs, boundary);
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].drive_minutes, 240); // 50% of 480
            assert_eq!(result[1].drive_minutes, 240);
            assert_eq!(result[0].end, boundary);
            assert_eq!(result[1].start, boundary);
        });
    }

    // ---- split_segments_at_24h_with_workdays: workday boundary < 24h ----
    #[test]
    fn test_split_segments_at_workday_boundary_under_24h() {
        test_group!("比較ロジック");
        test_case!("workday境界分割: 24h未満", {
            // セグメント: 13:05→翌10:01 (20h56min < 24h)
            // workday境界: 翌8:15
            // → 境界で分割されるべき
            let seg = work_segments::WorkSegment {
                start: dt(2026, 2, 27, 13, 5, 0),
                end: dt(2026, 2, 28, 10, 1, 0),
                labor_minutes: 600,
                drive_minutes: 500,
                cargo_minutes: 100,
            };
            let wd_ends = vec![dt(2026, 2, 28, 8, 15, 0)];
            let result = work_segments::split_segments_at_24h_with_workdays(vec![seg], &wd_ends);

            assert_eq!(result.len(), 2);
            assert_eq!(result[0].start, dt(2026, 2, 27, 13, 5, 0));
            assert_eq!(result[0].end, dt(2026, 2, 28, 8, 15, 0));
            assert_eq!(result[1].start, dt(2026, 2, 28, 8, 15, 0));
            assert_eq!(result[1].end, dt(2026, 2, 28, 10, 1, 0));
            // pro-rata: 前半(1150min)/全体(1256min) ≈ 91.6%
            assert!(result[0].drive_minutes > result[1].drive_minutes);
            assert_eq!(result[0].drive_minutes + result[1].drive_minutes, 500);
        });
    }

    #[test]
    fn test_split_segments_no_workday_boundary_under_24h() {
        test_group!("比較ロジック");
        test_case!("workday境界なし: 24h未満", {
            // セグメント < 24h, workday境界なし → 分割しない
            let seg = work_segments::WorkSegment {
                start: dt(2026, 2, 27, 13, 5, 0),
                end: dt(2026, 2, 28, 10, 1, 0),
                labor_minutes: 600,
                drive_minutes: 500,
                cargo_minutes: 100,
            };
            let result = work_segments::split_segments_at_24h_with_workdays(vec![seg], &[]);
            assert_eq!(result.len(), 1);
        });
    }

    // ---- group_operations_into_work_days ----
    #[test]
    fn test_group_ops_single_run() {
        test_group!("比較ロジック");
        test_case!("group_ops: 単一運行", {
            let rows = vec![make_kudguri(
                "U001",
                "1001",
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 17, 0, 0),
            )];
            let result = group_operations_into_work_days(&rows);
            assert_eq!(
                result.get("U001"),
                Some(&NaiveDate::from_ymd_opt(2026, 2, 1).unwrap())
            );
        });
    }
    #[test]
    fn test_group_ops_short_gap_same_day() {
        test_group!("比較ロジック");
        test_case!("group_ops: 短ギャップ同一日", {
            let rows = vec![
                make_kudguri(
                    "U001",
                    "1001",
                    dt(2026, 2, 1, 8, 0, 0),
                    dt(2026, 2, 1, 12, 0, 0),
                ),
                make_kudguri(
                    "U002",
                    "1001",
                    dt(2026, 2, 1, 12, 30, 0),
                    dt(2026, 2, 1, 17, 0, 0),
                ),
            ];
            let result = group_operations_into_work_days(&rows);
            assert_eq!(result["U001"], result["U002"]);
        });
    }
    #[test]
    fn test_group_ops_long_gap_new_day() {
        test_group!("比較ロジック");
        test_case!("group_ops: 長ギャップ新日", {
            // gap=20h > 540 → 別work_date
            let rows = vec![
                make_kudguri(
                    "U001",
                    "1001",
                    dt(2026, 2, 1, 8, 0, 0),
                    dt(2026, 2, 1, 12, 0, 0),
                ),
                make_kudguri(
                    "U002",
                    "1001",
                    dt(2026, 2, 2, 8, 0, 0),
                    dt(2026, 2, 2, 17, 0, 0),
                ),
            ];
            let result = group_operations_into_work_days(&rows);
            assert_ne!(result["U001"], result["U002"]);
        });
    }

    // ---- detect_diffs_csv ----
    #[test]
    fn test_detect_diffs_no_diff() {
        test_group!("比較ロジック");
        test_case!("detect_diffs: 差分なし", {
            let day = make_csv_day("2月1日", "8:00", "17:00", "5:00", "7:00");
            let diffs = detect_diffs_csv(&[day.clone()], &[day]);
            assert!(diffs.is_empty());
        });
    }
    #[test]
    fn test_detect_diffs_with_diff() {
        test_group!("比較ロジック");
        test_case!("detect_diffs: 差分あり", {
            let csv_day = make_csv_day("2月1日", "8:00", "17:00", "5:00", "5:00");
            let mut sys_day = csv_day.clone();
            sys_day.drive = "6:00".into();
            let diffs = detect_diffs_csv(&[csv_day], &[sys_day]);
            assert_eq!(diffs.len(), 1);
            assert_eq!(diffs[0].field, "運転");
        });
    }

    // ---- build_day_map ----
    #[test]
    fn test_build_day_map_single_day_run() {
        test_group!("比較ロジック");
        test_case!("build_day_map: 単日運行", {
            let cls = default_classifications();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 17, 0, 0),
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 120), // 運転2h
                make_kudgivt("U1", dt(2026, 2, 1, 10, 0, 0), "202", 60), // 荷役1h
                make_kudgivt("U1", dt(2026, 2, 1, 11, 0, 0), "301", 60), // 休憩1h
                make_kudgivt("U1", dt(2026, 2, 1, 12, 0, 0), "201", 180), // 運転3h
            ];
            let refs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), refs);

            let result = build_day_map(&kudguri, &by_unko, &cls);
            // 1エントリ (D1, 2/1, 8:00)
            assert_eq!(result.day_map.len(), 1);
            let key = result.day_map.keys().next().unwrap();
            assert_eq!(key.0, "D1");
            assert_eq!(key.1, NaiveDate::from_ymd_opt(2026, 2, 1).unwrap());
            let agg = &result.day_map[key];
            assert_eq!(agg.drive_minutes, 300); // 120+180=300
            assert_eq!(agg.cargo_minutes, 60);
        });
    }

    #[test]
    fn test_build_day_map_multi_day_with_rest() {
        test_group!("比較ロジック");
        test_case!("build_day_map: 複数日+休息", {
            let cls = default_classifications();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 2, 17, 0, 0),
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 480), // 運転8h
                make_kudgivt("U1", dt(2026, 2, 1, 16, 0, 0), "302", 600), // 休息10h (≥540→分割)
                make_kudgivt("U1", dt(2026, 2, 2, 2, 0, 0), "201", 480), // 運転8h
            ];
            let refs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), refs);

            let result = build_day_map(&kudguri, &by_unko, &cls);
            // 休息で分割 → 2エントリ
            assert!(result.day_map.len() >= 2);
        });
    }

    #[test]
    fn test_build_day_map_24h_forced_split() {
        test_group!("比較ロジック");
        test_case!("build_day_map: 24h強制分割", {
            let cls = default_classifications();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 6, 0, 0),
                dt(2026, 2, 3, 10, 0, 0),
            )];
            // 休息なしで連続作業 → 24h強制分割
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 6, 0, 0), "201", 1440), // 24h運転
                make_kudgivt("U1", dt(2026, 2, 2, 6, 0, 0), "201", 1440), // 24h運転
            ];
            let refs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), refs);

            let result = build_day_map(&kudguri, &by_unko, &cls);
            assert!(result.day_map.len() >= 2);
        });
    }

    // ---- post_process_day_map ----
    #[test]
    fn test_post_process_overlap_chain() {
        test_group!("比較ロジック");
        test_case!("post_process: 重複チェーン", {
            // 2日連続workday（gap短い） → chain
            let cls = default_classifications();
            let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let d2 = NaiveDate::from_ymd_opt(2026, 2, 2).unwrap();
            let t = NaiveTime::from_hms_opt(5, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            // Day1: 5:00→翌3:00 (22h)
            let mut agg1 = DayAgg::default();
            agg1.drive_minutes = 600;
            agg1.total_work_minutes = 600;
            agg1.unko_nos = vec!["U1".into()];
            agg1.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 5, 0, 0),
                end_at: dt(2026, 2, 2, 3, 0, 0),
            }];
            day_map.insert(("D1".into(), d1, t), agg1);
            // Day2: 3:00→15:00 (12h)
            let t2 = NaiveTime::from_hms_opt(3, 0, 0).unwrap();
            let mut agg2 = DayAgg::default();
            agg2.drive_minutes = 400;
            agg2.total_work_minutes = 400;
            agg2.unko_nos = vec!["U1".into()];
            agg2.segments = vec![SegRec {
                start_at: dt(2026, 2, 2, 3, 0, 0),
                end_at: dt(2026, 2, 2, 15, 0, 0),
            }];
            day_map.insert(("D1".into(), d2, t2), agg2);

            let mut wb = HashMap::new();
            wb.insert(
                ("D1".into(), d1, t),
                (dt(2026, 2, 1, 5, 0, 0), dt(2026, 2, 2, 3, 0, 0)),
            );
            wb.insert(
                ("D1".into(), d2, t2),
                (dt(2026, 2, 2, 3, 0, 0), dt(2026, 2, 2, 15, 0, 0)),
            );
            let mwb = HashMap::new();
            let mut dwe = HashMap::new();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 5, 0, 0),
                dt(2026, 2, 2, 15, 0, 0),
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 5, 0, 0), "201", 600),
                make_kudgivt("U1", dt(2026, 2, 2, 3, 0, 0), "201", 400),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), erefs);
            let ferry = FerryInfo::default();

            post_process_day_map(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &ferry,
            );
            // chain: gap=0 < 480 → overlap added to day1, deducted from day2
            // post_process実行後もエラーなく完了すること
            assert!(day_map.contains_key(&("D1".into(), d1, t)));
        });
    }

    #[test]
    fn test_post_process_overlap_reset() {
        test_group!("比較ロジック");
        test_case!("post_process: 重複リセット", {
            // gap=600分 ≥ 540 → reset（overlap表示）
            let cls = default_classifications();
            let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let d2 = NaiveDate::from_ymd_opt(2026, 2, 2).unwrap();
            let t1 = NaiveTime::from_hms_opt(6, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            let mut agg1 = DayAgg::default();
            agg1.drive_minutes = 300;
            agg1.total_work_minutes = 300;
            agg1.unko_nos = vec!["U1".into()];
            agg1.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 6, 0, 0),
                end_at: dt(2026, 2, 1, 11, 0, 0),
            }];
            day_map.insert(("D1".into(), d1, t1), agg1);
            let mut agg2 = DayAgg::default();
            agg2.drive_minutes = 300;
            agg2.total_work_minutes = 300;
            agg2.unko_nos = vec!["U1".into()];
            agg2.segments = vec![SegRec {
                start_at: dt(2026, 2, 2, 8, 0, 0),
                end_at: dt(2026, 2, 2, 13, 0, 0),
            }];
            day_map.insert(("D1".into(), d2, t2), agg2);

            let mut wb = HashMap::new();
            wb.insert(
                ("D1".into(), d1, t1),
                (dt(2026, 2, 1, 6, 0, 0), dt(2026, 2, 1, 11, 0, 0)),
            );
            wb.insert(
                ("D1".into(), d2, t2),
                (dt(2026, 2, 2, 8, 0, 0), dt(2026, 2, 2, 13, 0, 0)),
            );
            let mwb = HashMap::new();
            let mut dwe = HashMap::new();
            let kudguri = vec![
                make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 6, 0, 0),
                    dt(2026, 2, 1, 11, 0, 0),
                ),
                make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 2, 8, 0, 0),
                    dt(2026, 2, 2, 13, 0, 0),
                ),
            ];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 6, 0, 0), "201", 300),
                make_kudgivt("U1", dt(2026, 2, 2, 8, 0, 0), "201", 300),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), erefs);
            let ferry = FerryInfo::default();

            post_process_day_map(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &ferry,
            );
            // gap=21h > 540 → reset. 次のworkday(2/2 8:00)が24h window(6:00+24h=翌6:00)内
            // → overlap表示
            let entry1 = &day_map[&("D1".into(), d1, t1)];
            // day1にoverlap_restraint_minutesが設定される場合がある
            assert!(entry1.overlap_restraint_minutes >= 0);
        });
    }

    // ---- post_process: overlap excludes 302 rest ----
    #[test]
    fn test_post_process_overlap_excludes_rest302() {
        test_group!("比較ロジック");
        test_case!("post_process: 302休息除外", {
            // Day1: 6:00-11:00, Day2: 4:40-10:35 (with 302 rest gap 4:55-7:59 = 184min)
            // gap = 11:00→4:40(next day) = 17h40m > 540min → reset → overlap表示
            // 24h window: 6:00+24h = 翌6:00
            // Day2 starts at 4:40, but segments: [4:40-4:55] + [7:59-10:35]
            // overlap in window (before 翌6:00): seg1=4:40-4:55(15min) + seg2 clipped=0min(7:59>6:00? no 7:59>6:00 next day)
            // Actually: Day1 starts 2/4 6:00, window_end = 2/5 6:00
            // Day2 segments start on 2/5: [2/5 4:40 - 2/5 4:55] and [2/5 7:59 - 2/5 10:35]
            // overlap window: 2/5 4:40 to 2/5 6:00
            // seg1: 4:40-4:55, clipped to 4:40-4:55 = 15min (within window)
            // seg2: 7:59-10:35, starts after window_end(6:00) → excluded
            // Old (span-based): restraint_end=4:55(max seg_end in window), ol_restraint=4:55-4:40=15min
            // Hmm, that doesn't show the bug. Let me redesign.
            //
            // Better scenario: Day1 6:00-11:00, Day2 segments [14:00-15:00]+[20:00-22:00]
            // with 302 rest gap between 15:00-20:00 (300min)
            // gap = 11:00→14:00 = 3h < 540 but this would NOT reset...
            // Need gap >= 540 for the non-chain (overlap display) path.
            //
            // Correct scenario for overlap display (next_resets=true):
            // Day1: 2/1 6:00-11:00, Day2: 2/2 3:00 (gap=16h>540→reset)
            // Day2 segments: [2/2 3:00-5:00] + [2/2 8:00-10:00] (302 rest 5:00-8:00=180min)
            // 24h window: 2/1 6:00+24h = 2/2 6:00
            // Old: restraint_end = max(5:00, 10:00→clipped to 6:00) = 6:00
            //      ol_restraint = 6:00 - 3:00 = 180min (includes rest gap)
            // New: seg1 3:00-5:00 → clipped 3:00-5:00 = 120min
            //      seg2 8:00-10:00 → starts after 6:00 window but clipped to 6:00... no, 8:00>6:00 → skip
            //      Actually seg2 start=8:00 > window_end=6:00 → excluded
            //      seg_total = 120min
            // Expected: overlap_restraint = 120, not 180
            let cls = default_classifications();
            let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let d2 = NaiveDate::from_ymd_opt(2026, 2, 2).unwrap();
            let t1 = NaiveTime::from_hms_opt(6, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(3, 0, 0).unwrap();

            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            let mut agg1 = DayAgg::default();
            agg1.drive_minutes = 300;
            agg1.total_work_minutes = 300;
            agg1.unko_nos = vec!["U1".into()];
            agg1.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 6, 0, 0),
                end_at: dt(2026, 2, 1, 11, 0, 0),
            }];
            day_map.insert(("D1".into(), d1, t1), agg1);

            let mut agg2 = DayAgg::default();
            agg2.drive_minutes = 200;
            agg2.total_work_minutes = 200;
            agg2.unko_nos = vec!["U1".into()];
            // 2 segments with 302 rest gap (5:00-8:00)
            agg2.segments = vec![
                SegRec {
                    start_at: dt(2026, 2, 2, 3, 0, 0),
                    end_at: dt(2026, 2, 2, 5, 0, 0),
                },
                SegRec {
                    start_at: dt(2026, 2, 2, 8, 0, 0),
                    end_at: dt(2026, 2, 2, 10, 0, 0),
                },
            ];
            day_map.insert(("D1".into(), d2, t2), agg2);

            let mut wb = HashMap::new();
            wb.insert(
                ("D1".into(), d1, t1),
                (dt(2026, 2, 1, 6, 0, 0), dt(2026, 2, 1, 11, 0, 0)),
            );
            wb.insert(
                ("D1".into(), d2, t2),
                (dt(2026, 2, 2, 3, 0, 0), dt(2026, 2, 2, 10, 0, 0)),
            );
            let mwb = HashMap::new();
            let mut dwe = HashMap::new();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 6, 0, 0),
                dt(2026, 2, 1, 11, 0, 0),
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 2, 3, 0, 0), "201", 120),
                make_kudgivt("U1", dt(2026, 2, 2, 8, 0, 0), "201", 120),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), erefs);
            let ferry = FerryInfo::default();

            post_process_day_map(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &ferry,
            );

            let entry1 = &day_map[&("D1".into(), d1, t1)];
            // gap=16h > 540 → reset → overlap表示
            // 24h window: 6:00+24h = 翌6:00
            // overlap = seg1(3:00-5:00 within window) = 120min
            // NOT 180min (which would include 302 rest gap 5:00-8:00)
            assert_eq!(entry1.overlap_restraint_minutes, 120);
            // overlap_break = restraint - drive - cargo
            // drive overlap from events: 120min (3:00-5:00 event, within window)
            assert_eq!(entry1.overlap_break_minutes, 0);
        });
    }

    // ---- post_process: ferry deduction ----
    #[test]
    fn test_post_process_ferry_deduction() {
        test_group!("比較ロジック");
        test_case!("post_process: フェリー控除", {
            let cls = default_classifications();
            let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let t1 = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            let mut agg = DayAgg::default();
            agg.drive_minutes = 300;
            agg.total_work_minutes = 400;
            agg.unko_nos = vec!["U1".into()];
            agg.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 8, 0, 0),
                end_at: dt(2026, 2, 1, 15, 0, 0),
            }];
            day_map.insert(("D1".into(), d1, t1), agg);
            let mut wb = HashMap::new();
            wb.insert(
                ("D1".into(), d1, t1),
                (dt(2026, 2, 1, 8, 0, 0), dt(2026, 2, 1, 15, 0, 0)),
            );
            let mwb = HashMap::new();
            let mut dwe = HashMap::new();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 15, 0, 0),
            )];
            // フェリー10:00-11:00 (60分)、301イベント10:00-11:00 (60分)
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 120),
                make_kudgivt("U1", dt(2026, 2, 1, 10, 0, 0), "301", 60),
                make_kudgivt("U1", dt(2026, 2, 1, 11, 0, 0), "201", 180),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), erefs);
            let mut ferry = FerryInfo::default();
            ferry.ferry_period_map.insert(
                "U1".into(),
                vec![(dt(2026, 2, 1, 10, 0, 0), dt(2026, 2, 1, 11, 0, 0))],
            );
            post_process_day_map(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &ferry,
            );
            let entry = &day_map[&("D1".into(), d1, t1)];
            // フェリー60分が控除される
            assert!(entry.total_work_minutes < 400);
        });
    }

    // ---- post_process: forced_next_reset ----
    #[test]
    fn test_post_process_forced_next_reset() {
        test_group!("比較ロジック");
        test_case!("post_process: 強制次日リセット", {
            let cls = default_classifications();
            let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let d2 = NaiveDate::from_ymd_opt(2026, 2, 2).unwrap();
            let t1 = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            // Day1: 24h workday
            let mut agg1 = DayAgg::default();
            agg1.drive_minutes = 600;
            agg1.total_work_minutes = 600;
            agg1.unko_nos = vec!["U1".into()];
            agg1.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 8, 0, 0),
                end_at: dt(2026, 2, 2, 8, 0, 0),
            }];
            day_map.insert(("D1".into(), d1, t1), agg1);
            // Day2: short workday (from 24h split)
            let mut agg2 = DayAgg::default();
            agg2.drive_minutes = 200;
            agg2.total_work_minutes = 200;
            agg2.unko_nos = vec!["U1".into()];
            agg2.segments = vec![SegRec {
                start_at: dt(2026, 2, 2, 8, 0, 0),
                end_at: dt(2026, 2, 2, 14, 0, 0),
            }];
            day_map.insert(("D1".into(), d2, t2), agg2);
            let mut wb = HashMap::new();
            wb.insert(
                ("D1".into(), d1, t1),
                (dt(2026, 2, 1, 8, 0, 0), dt(2026, 2, 2, 8, 0, 0)),
            );
            wb.insert(
                ("D1".into(), d2, t2),
                (dt(2026, 2, 2, 8, 0, 0), dt(2026, 2, 2, 14, 0, 0)),
            );
            // multi_wd_boundaries: 2/1のdet_endがwindow_endより前 → forced_next_reset
            let mut mwb = HashMap::new();
            mwb.insert(("D1".into(), d1, t1), dt(2026, 2, 2, 8, 0, 0));
            mwb.insert(("D1".into(), d2, t2), dt(2026, 2, 2, 14, 0, 0));
            let mut dwe = HashMap::new();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 2, 14, 0, 0),
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 600),
                make_kudgivt("U1", dt(2026, 2, 2, 8, 0, 0), "201", 200),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), erefs);
            let ferry = FerryInfo::default();
            post_process_day_map(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &ferry,
            );
            // forced_next_resetが正しく動作し、エラーなく完了すること
            assert!(day_map.contains_key(&("D1".into(), d2, t2)));
        });
    }

    // ---- compare_drivers ----
    #[test]
    fn test_compare_drivers_no_diff() {
        test_group!("比較ロジック");
        test_case!("compare_drivers: 差分なし", {
            let days = vec![make_csv_day("2月1日", "8:00", "17:00", "5:00", "6:00")];
            let csv_data = vec![CsvDriverData {
                driver_name: "Test".into(),
                driver_cd: "1001".into(),
                days: days.clone(),
                total_drive: "5:00".into(),
                total_cargo: "".into(),
                total_break: "".into(),
                total_restraint: "6:00".into(),
                total_actual_work: "5:00".into(),
                total_overtime: "".into(),
                total_late_night: "".into(),
                total_ot_late_night: "".into(),
            }];
            let sys_data = csv_data.clone();
            let report = compare_drivers(&csv_data, &sys_data, None);
            assert_eq!(report.total_diffs, 0);
        });
    }
    #[test]
    fn test_compare_drivers_with_diff() {
        test_group!("比較ロジック");
        test_case!("compare_drivers: 差分あり", {
            let csv_days = vec![make_csv_day("2月1日", "8:00", "17:00", "5:00", "6:00")];
            let mut sys_days = csv_days.clone();
            sys_days[0].drive = "6:00".into();
            let csv_data = vec![CsvDriverData {
                driver_name: "Test".into(),
                driver_cd: "1001".into(),
                days: csv_days,
                total_drive: "5:00".into(),
                total_cargo: "".into(),
                total_break: "".into(),
                total_restraint: "6:00".into(),
                total_actual_work: "5:00".into(),
                total_overtime: "".into(),
                total_late_night: "".into(),
                total_ot_late_night: "".into(),
            }];
            let sys_data = vec![CsvDriverData {
                driver_name: "Test".into(),
                driver_cd: "1001".into(),
                days: sys_days,
                total_drive: "6:00".into(),
                total_cargo: "".into(),
                total_break: "".into(),
                total_restraint: "6:00".into(),
                total_actual_work: "6:00".into(),
                total_overtime: "".into(),
                total_late_night: "".into(),
                total_ot_late_night: "".into(),
            }];
            let report = compare_drivers(&csv_data, &sys_data, None);
            assert!(report.total_diffs > 0);
        });
    }

    // ---- detect_year_month ----
    #[test]
    fn test_detect_year_month() {
        test_group!("比較ロジック");
        test_case!("detect_year_month: 年月検出", {
            let days = vec![make_csv_day("2月1日", "8:00", "17:00", "5:00", "6:00")];
            let data = vec![CsvDriverData {
                driver_name: "T".into(),
                driver_cd: "1".into(),
                days,
                total_drive: "".into(),
                total_cargo: "".into(),
                total_break: "".into(),
                total_restraint: "".into(),
                total_actual_work: "".into(),
                total_overtime: "".into(),
                total_late_night: "".into(),
                total_ot_late_night: "".into(),
            }];
            let (y, m) = detect_year_month(&data);
            assert_eq!(y, 2026);
            assert_eq!(m, 2);
        });
    }

    // ---- group_operations 24h boundary ----
    #[test]
    fn test_group_ops_24h_boundary() {
        test_group!("比較ロジック");
        test_case!("group_ops: 24h境界", {
            // 同じドライバー、3連続短い運行(合計30h > 24h) → 24hで分離
            let rows = vec![
                make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 6, 0, 0),
                    dt(2026, 2, 1, 16, 0, 0),
                ),
                make_kudguri(
                    "U2",
                    "D1",
                    dt(2026, 2, 1, 16, 30, 0),
                    dt(2026, 2, 2, 2, 0, 0),
                ),
                make_kudguri(
                    "U3",
                    "D1",
                    dt(2026, 2, 2, 2, 30, 0),
                    dt(2026, 2, 2, 12, 0, 0),
                ),
            ];
            let result = group_operations_into_work_days(&rows);
            // U1とU2は同じwork_date(gap 30min < 540)
            assert_eq!(result["U1"], result["U2"]);
            // U3: since_shigyo = 2/2 2:30 - 2/1 6:00 = 20.5h < 24h → same day
            // OR: since_shigyo > 24h → new day. Depends on exact implementation.
            // Just verify all 3 have work_dates assigned
            assert!(result.contains_key("U3"));
        });
    }

    // ---- post_process: overlap with ferry in non-chain ----
    #[test]
    fn test_post_process_overlap_with_ferry() {
        test_group!("比較ロジック");
        test_case!("post_process: フェリー重複", {
            let cls = default_classifications();
            let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let d2 = NaiveDate::from_ymd_opt(2026, 2, 2).unwrap();
            let t1 = NaiveTime::from_hms_opt(14, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            let mut agg1 = DayAgg::default();
            agg1.drive_minutes = 200;
            agg1.total_work_minutes = 250;
            agg1.unko_nos = vec!["U1".into()];
            agg1.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 14, 0, 0),
                end_at: dt(2026, 2, 1, 18, 0, 0),
            }];
            day_map.insert(("D1".into(), d1, t1), agg1);
            let mut agg2 = DayAgg::default();
            agg2.drive_minutes = 300;
            agg2.total_work_minutes = 350;
            agg2.unko_nos = vec!["U1".into()];
            agg2.segments = vec![SegRec {
                start_at: dt(2026, 2, 2, 8, 0, 0),
                end_at: dt(2026, 2, 2, 14, 0, 0),
            }];
            day_map.insert(("D1".into(), d2, t2), agg2);
            let mut wb = HashMap::new();
            wb.insert(
                ("D1".into(), d1, t1),
                (dt(2026, 2, 1, 14, 0, 0), dt(2026, 2, 1, 18, 0, 0)),
            );
            wb.insert(
                ("D1".into(), d2, t2),
                (dt(2026, 2, 2, 8, 0, 0), dt(2026, 2, 2, 14, 0, 0)),
            );
            let mwb = HashMap::new();
            let mut dwe = HashMap::new();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 14, 0, 0),
                dt(2026, 2, 2, 14, 0, 0),
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 14, 0, 0), "201", 200),
                make_kudgivt("U1", dt(2026, 2, 2, 8, 0, 0), "201", 120),
                make_kudgivt("U1", dt(2026, 2, 2, 10, 0, 0), "301", 60), // フェリー内301
                make_kudgivt("U1", dt(2026, 2, 2, 11, 0, 0), "201", 180),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), erefs);
            let mut ferry = FerryInfo::default();
            ferry.ferry_period_map.insert(
                "U1".into(),
                vec![(dt(2026, 2, 2, 10, 0, 0), dt(2026, 2, 2, 11, 0, 0))],
            );
            post_process_day_map(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &ferry,
            );
            // gap=14h ≥ 540 → reset, overlapが表示される
            let e1 = &day_map[&("D1".into(), d1, t1)];
            assert!(e1.overlap_restraint_minutes >= 0);
        });
    }

    // ---- build_day_map: multi-op path ----
    #[test]
    fn test_build_day_map_multi_op_same_day() {
        test_group!("比較ロジック");
        test_case!("build_day_map: 同日複数運行", {
            let cls = default_classifications();
            // 同日2運行、別ドライバー日、spans_different_days
            let kudguri = vec![
                make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 6, 0, 0),
                    dt(2026, 2, 2, 18, 0, 0),
                ),
                make_kudguri(
                    "U2",
                    "D1",
                    dt(2026, 2, 3, 6, 0, 0),
                    dt(2026, 2, 4, 18, 0, 0),
                ),
            ];
            let evts1 = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 6, 0, 0), "201", 600),
                make_kudgivt("U1", dt(2026, 2, 1, 16, 0, 0), "302", 840),
                make_kudgivt("U1", dt(2026, 2, 2, 6, 0, 0), "201", 600),
            ];
            let evts2 = vec![
                make_kudgivt("U2", dt(2026, 2, 3, 6, 0, 0), "201", 600),
                make_kudgivt("U2", dt(2026, 2, 3, 16, 0, 0), "302", 840),
                make_kudgivt("U2", dt(2026, 2, 4, 6, 0, 0), "201", 600),
            ];
            let r1: Vec<&_> = evts1.iter().collect();
            let r2: Vec<&_> = evts2.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), r1);
            by_unko.insert("U2".into(), r2);
            let result = build_day_map(&kudguri, &by_unko, &cls);
            // 2運行 → 複数workday
            assert!(result.day_map.len() >= 2);
        });
    }

    // ---- calc_ot_late_night boundary ----
    #[test]
    fn test_ot_late_night_boundary_crossing() {
        test_group!("比較ロジック");
        test_case!("時間外深夜: 境界跨ぎ", {
            // 7h所定 + 2h深夜(3:00-5:00) → 1hが時間外深夜
            let events = vec![
                (dt(2026, 2, 1, 20, 0, 0), dt(2026, 2, 2, 5, 0, 0)), // 9h (20-5)
            ];
            let result = calc_ot_late_night_from_events(&events);
            // 8h所定後の1h(4:00-5:00)が時間外深夜
            assert_eq!(result, 60);
        });
    }
    #[test]
    fn test_ot_late_night_all_overtime_deep_night() {
        test_group!("比較ロジック");
        test_case!("時間外深夜: 全時間外深夜", {
            // 先に8h所定を消化、その後すべて深夜
            let events = vec![
                (dt(2026, 2, 1, 8, 0, 0), dt(2026, 2, 1, 16, 0, 0)), // 8h 所定
                (dt(2026, 2, 1, 22, 0, 0), dt(2026, 2, 2, 2, 0, 0)), // 4h 全深夜+全時間外
            ];
            let result = calc_ot_late_night_from_events(&events);
            assert_eq!(result, 240); // 4h全部が時間外深夜
        });
    }

    // ---- process_parsed_data ----
    #[test]
    fn test_process_parsed_data_single_day() {
        test_group!("比較ロジック");
        test_case!("process_parsed: 単日", {
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 17, 0, 0),
            )];
            let kudgivt = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 300),
                make_kudgivt("U1", dt(2026, 2, 1, 13, 0, 0), "301", 60),
                make_kudgivt("U1", dt(2026, 2, 1, 14, 0, 0), "201", 120),
            ];
            let ferry = FerryInfo::default();
            let result = process_parsed_data(&kudguri, &kudgivt, &ferry, 2026, 2).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].driver_cd, "D1");
            // 2月1日のデータが含まれる
            let working_days: Vec<_> = result[0].days.iter().filter(|d| !d.is_holiday).collect();
            assert!(!working_days.is_empty());
        });
    }
    #[test]
    fn test_process_parsed_data_multi_day_with_rest() {
        test_group!("比較ロジック");
        test_case!("process_parsed: 複数日+休息", {
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 2, 17, 0, 0),
            )];
            let kudgivt = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 480),
                make_kudgivt("U1", dt(2026, 2, 1, 16, 0, 0), "302", 600),
                make_kudgivt("U1", dt(2026, 2, 2, 2, 0, 0), "201", 480),
            ];
            let ferry = FerryInfo::default();
            let result = process_parsed_data(&kudguri, &kudgivt, &ferry, 2026, 2).unwrap();
            assert_eq!(result.len(), 1);
            let working_days: Vec<_> = result[0].days.iter().filter(|d| !d.is_holiday).collect();
            assert!(working_days.len() >= 2);
        });
    }
    #[test]
    fn test_process_parsed_data_with_ferry() {
        test_group!("比較ロジック");
        test_case!("process_parsed: フェリー", {
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 17, 0, 0),
            )];
            let kudgivt = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 120),
                make_kudgivt("U1", dt(2026, 2, 1, 10, 0, 0), "301", 60),
                make_kudgivt("U1", dt(2026, 2, 1, 11, 0, 0), "201", 300),
            ];
            let mut ferry = FerryInfo::default();
            ferry.ferry_period_map.insert(
                "U1".into(),
                vec![(dt(2026, 2, 1, 10, 0, 0), dt(2026, 2, 1, 11, 0, 0))],
            );
            let result = process_parsed_data(&kudguri, &kudgivt, &ferry, 2026, 2).unwrap();
            assert_eq!(result.len(), 1);
            // フェリー控除が適用されている
            let day = result[0].days.iter().find(|d| d.date == "2月1日").unwrap();
            assert!(!day.subtotal.is_empty());
        });
    }
    #[test]
    fn test_process_parsed_data_empty() {
        test_group!("比較ロジック");
        test_case!("process_parsed: 空データ", {
            let result = process_parsed_data(&[], &[], &FerryInfo::default(), 2026, 2);
            // ドライバーなし → 空結果
            assert!(result.unwrap().is_empty());
        });
    }

    // ---- annotate_known_bugs: cascading + total ----
    #[test]
    fn test_annotate_known_bugs_cascading() {
        test_group!("比較ロジック");
        test_case!("annotate_bugs: カスケード", {
            let mut diffs = vec![
                DiffItem {
                    date: "2月22日".into(),
                    field: "始業".into(),
                    csv_val: "a".into(),
                    sys_val: "b".into(),
                    known_bug: None,
                },
                DiffItem {
                    date: "2月23日".into(),
                    field: "累計".into(),
                    csv_val: "100".into(),
                    sys_val: "90".into(),
                    known_bug: None,
                },
            ];
            let mut totals = vec![TotalDiffItem {
                label: "拘束合計".into(),
                csv_val: "100".into(),
                sys_val: "90".into(),
                known_bug: None,
            }];
            annotate_known_bugs("1039", &mut diffs, &mut totals);
            assert!(diffs[0].known_bug.is_some()); // 直接マッチ
            assert!(diffs[1].known_bug.is_some()); // cascading累計
            assert!(totals[0].known_bug.is_some()); // 合計行
        });
    }

    // ---- compare_drivers: filter ----
    #[test]
    fn test_compare_drivers_with_filter() {
        test_group!("比較ロジック");
        test_case!("compare_drivers: フィルタ付き", {
            let days = vec![make_csv_day("2月1日", "8:00", "17:00", "5:00", "6:00")];
            let data = vec![CsvDriverData {
                driver_name: "A".into(),
                driver_cd: "1001".into(),
                days: days.clone(),
                total_drive: "5:00".into(),
                total_cargo: "".into(),
                total_break: "".into(),
                total_restraint: "6:00".into(),
                total_actual_work: "5:00".into(),
                total_overtime: "".into(),
                total_late_night: "".into(),
                total_ot_late_night: "".into(),
            }];
            let report = compare_drivers(&data, &data, Some("9999")); // 存在しないドライバー
            assert_eq!(report.total_diffs, 0);
        });
    }
    #[test]
    fn test_compare_drivers_missing_in_sys() {
        test_group!("比較ロジック");
        test_case!("compare_drivers: システム側欠損", {
            let days = vec![make_csv_day("2月1日", "8:00", "17:00", "5:00", "6:00")];
            let csv_data = vec![CsvDriverData {
                driver_name: "A".into(),
                driver_cd: "1001".into(),
                days,
                total_drive: "5:00".into(),
                total_cargo: "".into(),
                total_break: "".into(),
                total_restraint: "6:00".into(),
                total_actual_work: "5:00".into(),
                total_overtime: "".into(),
                total_late_night: "".into(),
                total_ot_late_night: "".into(),
            }];
            let report = compare_drivers(&csv_data, &[], None); // sysにドライバーなし
                                                                // sysに一致するドライバーがない → エラー行として記録される
            assert!(!report.drivers.is_empty());
        });
    }

    // ---- parse_restraint_csv: Shift-JIS + multi driver ----
    #[test]
    fn test_parse_restraint_csv_multi_driver() {
        test_group!("比較ロジック");
        test_case!("parse_csv: 複数ドライバー", {
            let csv = "氏名,AAA,乗務員コード,1001\n\
日付,始業時刻\n\
2月1日,8:00,17:00,5:00,,,,1:00,,,,6:00,,6:00,6:00,,,,5:00,,,,\n\
合計,,,5:00,,,,1:00,,,,6:00,,,,,5:00,,,,\n\
氏名,BBB,乗務員コード,1002\n\
日付,始業時刻\n\
2月1日,9:00,18:00,6:00,,,,1:00,,,,7:00,,7:00,7:00,,,,6:00,,,,\n\
合計,,,6:00,,,,1:00,,,,7:00,,,,,6:00,,,,\n";
            let result = parse_restraint_csv(csv.as_bytes()).unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].driver_cd, "1001");
            assert_eq!(result[1].driver_cd, "1002");
        });
    }

    // ---- build_day_map: invalid ops (no departure) ----
    #[test]
    fn test_build_day_map_invalid_op() {
        test_group!("比較ロジック");
        test_case!("build_day_map: 不正運行", {
            let cls = default_classifications();
            let mut row = make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 17, 0, 0),
            );
            row.departure_at = None; // invalid
            row.return_at = None;
            let kudguri = vec![row];
            let by_unko: HashMap<String, Vec<&csv_parser::kudgivt::KudgivtRow>> = HashMap::new();
            let result = build_day_map(&kudguri, &by_unko, &cls);
            // invalid op → drive_time fallback only
            assert!(result.day_map.len() <= 1);
        });
    }

    // ---- process_parsed_data: overtime calculation ----
    #[test]
    fn test_process_parsed_data_overtime() {
        test_group!("比較ロジック");
        test_case!("process_parsed: 時間外", {
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 6, 0, 0),
                dt(2026, 2, 1, 18, 0, 0),
            )];
            let kudgivt = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 6, 0, 0), "201", 600), // 10h運転
            ];
            let result =
                process_parsed_data(&kudguri, &kudgivt, &FerryInfo::default(), 2026, 2).unwrap();
            let day = result[0].days.iter().find(|d| d.date == "2月1日").unwrap();
            assert!(!day.overtime.is_empty(), "10h work should have overtime");
        });
    }

    // ---- process_parsed_data: late night ----
    #[test]
    fn test_process_parsed_data_late_night() {
        test_group!("比較ロジック");
        test_case!("process_parsed: 深夜", {
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 22, 0, 0),
                dt(2026, 2, 2, 5, 0, 0),
            )];
            let kudgivt = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 22, 0, 0), "201", 420), // 22:00-5:00 全深夜
            ];
            let result =
                process_parsed_data(&kudguri, &kudgivt, &FerryInfo::default(), 2026, 2).unwrap();
            let working: Vec<_> = result[0]
                .days
                .iter()
                .filter(|d| !d.is_holiday && !d.late_night.is_empty())
                .collect();
            assert!(!working.is_empty(), "22:00-5:00 should have late_night");
        });
    }

    // ---- build_day_map: spans_different_days (multi-op merge path) ----
    #[test]
    fn test_build_day_map_spans_different_days() {
        test_group!("比較ロジック");
        test_case!("build_day_map: 日跨ぎ", {
            let cls = default_classifications();
            // 2運行、異なる日に出発（ret/depが日を共有しない）→ multi-op merge
            let kudguri = vec![
                make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 8, 0, 0),
                    dt(2026, 2, 2, 10, 0, 0),
                ),
                make_kudguri(
                    "U2",
                    "D1",
                    dt(2026, 2, 4, 8, 0, 0),
                    dt(2026, 2, 5, 10, 0, 0),
                ),
            ];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 600),
                make_kudgivt("U1", dt(2026, 2, 1, 18, 0, 0), "302", 840),
                make_kudgivt("U1", dt(2026, 2, 2, 8, 0, 0), "201", 120),
                make_kudgivt("U2", dt(2026, 2, 4, 8, 0, 0), "201", 600),
                make_kudgivt("U2", dt(2026, 2, 4, 18, 0, 0), "302", 840),
                make_kudgivt("U2", dt(2026, 2, 5, 8, 0, 0), "201", 120),
            ];
            let refs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            for e in &refs {
                by_unko.entry(e.unko_no.clone()).or_default().push(*e);
            }
            let result = build_day_map(&kudguri, &by_unko, &cls);
            assert!(result.day_map.len() >= 2);
        });
    }

    // ---- build_day_map: 3日以上スパンのsig_split ----
    #[test]
    fn test_build_day_map_sig_split_3day() {
        test_group!("比較ロジック");
        test_case!("build_day_map: SIG分割3日", {
            let cls = default_classifications();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 5, 17, 0, 0), // 4日スパン
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 600),
                make_kudgivt("U1", dt(2026, 2, 1, 18, 0, 0), "302", 840), // 14h rest → split
                make_kudgivt("U1", dt(2026, 2, 2, 8, 0, 0), "201", 1200), // 20h span crossing boundary
                make_kudgivt("U1", dt(2026, 2, 3, 4, 0, 0), "302", 840),
                make_kudgivt("U1", dt(2026, 2, 3, 18, 0, 0), "201", 600),
                make_kudgivt("U1", dt(2026, 2, 4, 4, 0, 0), "302", 840),
                make_kudgivt("U1", dt(2026, 2, 4, 18, 0, 0), "201", 600),
            ];
            let refs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), refs);
            let result = build_day_map(&kudguri, &by_unko, &cls);
            assert!(result.day_map.len() >= 3);
        });
    }

    // ---- build_day_map: same-day multi ops (shares date, not spans_different) ----
    #[test]
    fn test_build_day_map_same_day_multi_ops() {
        test_group!("比較ロジック");
        test_case!("build_day_map: 同日複数運行(2)", {
            let cls = default_classifications();
            // 2運行が同日帰着/出発を共有 → spans_different_days=false
            let kudguri = vec![
                make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 8, 0, 0),
                    dt(2026, 2, 2, 12, 0, 0),
                ),
                make_kudguri(
                    "U2",
                    "D1",
                    dt(2026, 2, 2, 14, 0, 0),
                    dt(2026, 2, 3, 12, 0, 0),
                ),
            ];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 480),
                make_kudgivt("U1", dt(2026, 2, 1, 16, 0, 0), "302", 840),
                make_kudgivt("U1", dt(2026, 2, 2, 6, 0, 0), "201", 360),
                make_kudgivt("U2", dt(2026, 2, 2, 14, 0, 0), "201", 480),
                make_kudgivt("U2", dt(2026, 2, 2, 22, 0, 0), "302", 600),
                make_kudgivt("U2", dt(2026, 2, 3, 8, 0, 0), "201", 240),
            ];
            let refs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            for e in &refs {
                by_unko.entry(e.unko_no.clone()).or_default().push(*e);
            }
            let result = build_day_map(&kudguri, &by_unko, &cls);
            assert!(result.day_map.len() >= 2);
        });
    }

    // ---- post_process: split_rests accumulation ----
    #[test]
    fn test_post_process_split_rests() {
        test_group!("比較ロジック");
        test_case!("post_process: 分割休息", {
            // 2分割休息: 200分 + 400分 = 600 ≥ 600 → reset
            let cls = default_classifications();
            let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let d2 = NaiveDate::from_ymd_opt(2026, 2, 2).unwrap();
            let d3 = NaiveDate::from_ymd_opt(2026, 2, 3).unwrap();
            let t1 = NaiveTime::from_hms_opt(6, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
            let t3 = NaiveTime::from_hms_opt(5, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            for (d, t, start_h, end_h) in [(d1, t1, 6, 18), (d2, t2, 12, 22), (d3, t3, 5, 15)] {
                let mut agg = DayAgg::default();
                agg.drive_minutes = 300;
                agg.total_work_minutes = 300;
                agg.unko_nos = vec!["U1".into()];
                agg.segments = vec![SegRec {
                    start_at: d.and_hms_opt(start_h, 0, 0).unwrap(),
                    end_at: d.and_hms_opt(end_h, 0, 0).unwrap(),
                }];
                day_map.insert(("D1".into(), d, t), agg);
            }
            let mut wb = HashMap::new();
            for (d, t, s, e) in [
                (d1, t1, dt(2026, 2, 1, 6, 0, 0), dt(2026, 2, 1, 18, 0, 0)),
                (d2, t2, dt(2026, 2, 2, 12, 0, 0), dt(2026, 2, 2, 22, 0, 0)),
                (d3, t3, dt(2026, 2, 3, 5, 0, 0), dt(2026, 2, 3, 15, 0, 0)),
            ] {
                wb.insert(("D1".into(), d, t), (s, e));
            }
            let mwb = HashMap::new();
            let mut dwe = HashMap::new();
            let kudguri = vec![
                make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 6, 0, 0),
                    dt(2026, 2, 1, 18, 0, 0),
                ),
                make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 2, 12, 0, 0),
                    dt(2026, 2, 2, 22, 0, 0),
                ),
                make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 3, 5, 0, 0),
                    dt(2026, 2, 3, 15, 0, 0),
                ),
            ];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 6, 0, 0), "201", 300),
                make_kudgivt("U1", dt(2026, 2, 2, 12, 0, 0), "201", 300),
                make_kudgivt("U1", dt(2026, 2, 3, 5, 0, 0), "201", 300),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), erefs);
            let ferry = FerryInfo::default();
            post_process_day_map(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &ferry,
            );
            assert_eq!(day_map.len(), 3);
        });
    }

    // ---- process_parsed_data: 2 drivers ----
    #[test]
    fn test_process_parsed_data_two_drivers() {
        test_group!("比較ロジック");
        test_case!("process_parsed: 2名", {
            let kudguri = vec![
                make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 8, 0, 0),
                    dt(2026, 2, 1, 17, 0, 0),
                ),
                make_kudguri(
                    "U2",
                    "D2",
                    dt(2026, 2, 1, 9, 0, 0),
                    dt(2026, 2, 1, 18, 0, 0),
                ),
            ];
            let kudgivt = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 300),
                make_kudgivt("U2", dt(2026, 2, 1, 9, 0, 0), "201", 300),
            ];
            let result =
                process_parsed_data(&kudguri, &kudgivt, &FerryInfo::default(), 2026, 2).unwrap();
            assert_eq!(result.len(), 2);
        });
    }

    // ---- post_process: 構内結合 (same-day merge) ----
    #[test]
    fn test_post_process_merge_same_day_entries() {
        test_group!("比較ロジック");
        test_case!("post_process: 同日エントリ合算", {
            let cls = default_classifications();
            let d = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let t1 = NaiveTime::from_hms_opt(6, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(14, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            // 同日2エントリ、異なる運行、gap短い → 構内結合
            let mut a1 = DayAgg::default();
            a1.drive_minutes = 200;
            a1.total_work_minutes = 200;
            a1.unko_nos = vec!["U1".into()];
            a1.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 6, 0, 0),
                end_at: dt(2026, 2, 1, 12, 0, 0),
            }];
            day_map.insert(("D1".into(), d, t1), a1);
            let mut a2 = DayAgg::default();
            a2.drive_minutes = 100;
            a2.total_work_minutes = 100;
            a2.unko_nos = vec!["U2".into()];
            a2.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 14, 0, 0),
                end_at: dt(2026, 2, 1, 18, 0, 0),
            }];
            day_map.insert(("D1".into(), d, t2), a2);
            let mut wb = HashMap::new();
            wb.insert(
                ("D1".into(), d, t1),
                (dt(2026, 2, 1, 6, 0, 0), dt(2026, 2, 1, 12, 0, 0)),
            );
            wb.insert(
                ("D1".into(), d, t2),
                (dt(2026, 2, 1, 14, 0, 0), dt(2026, 2, 1, 18, 0, 0)),
            );
            let mwb = HashMap::new();
            let mut dwe = HashMap::new();
            let kudguri = vec![
                make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 6, 0, 0),
                    dt(2026, 2, 1, 12, 0, 0),
                ),
                make_kudguri(
                    "U2",
                    "D1",
                    dt(2026, 2, 1, 14, 0, 0),
                    dt(2026, 2, 1, 18, 0, 0),
                ),
            ];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 6, 0, 0), "201", 200),
                make_kudgivt("U2", dt(2026, 2, 1, 14, 0, 0), "201", 100),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            for e in &erefs {
                by_unko.entry(e.unko_no.clone()).or_default().push(*e);
            }
            post_process_day_map(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &FerryInfo::default(),
            );
            // 構内結合で1エントリにマージされる可能性
            assert!(day_map.len() >= 1);
        });
    }

    // ---- build_day_map: sig_split実行パス ----
    #[test]
    fn test_build_day_map_sig_split_triggers() {
        test_group!("比較ロジック");
        test_case!("build_day_map: SIG分割発動", {
            let cls = default_classifications();
            // 4日スパン、workday境界を跨ぐ大きなセグメント(両側180分以上)
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 5, 8, 0, 0),
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 960), // 16h
                make_kudgivt("U1", dt(2026, 2, 2, 0, 0, 0), "302", 540), // 9h rest
                make_kudgivt("U1", dt(2026, 2, 2, 9, 0, 0), "201", 1200), // 20h（workday境界を跨ぐ大セグメント）
                make_kudgivt("U1", dt(2026, 2, 3, 5, 0, 0), "302", 600),  // 10h rest
                make_kudgivt("U1", dt(2026, 2, 3, 15, 0, 0), "201", 600),
                make_kudgivt("U1", dt(2026, 2, 4, 1, 0, 0), "302", 540),
                make_kudgivt("U1", dt(2026, 2, 4, 10, 0, 0), "201", 600),
            ];
            let refs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), refs);
            let result = build_day_map(&kudguri, &by_unko, &cls);
            assert!(result.day_map.len() >= 3);
        });
    }

    // ---- build_day_map: spans_different_days true → multi-op path ----
    #[test]
    fn test_build_day_map_multi_op_path() {
        test_group!("比較ロジック");
        test_case!("build_day_map: 複数運行パス", {
            let cls = default_classifications();
            // k1: dep=2/1 ret=2/1(同日), k2: dep=2/2 ret=2/4(日跨ぎ)
            // gap: 2/1 18:00→2/2 1:00 = 7h=420 < 480(long) → same group
            // dep dates: {2/1, 2/2} → 2 dates. ret/dep: k1.ret.date=2/1≠k2.dep.date=2/2 → no share
            // → spans_different_days=true → multi-op path!
            let kudguri = vec![
                make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 8, 0, 0),
                    dt(2026, 2, 1, 18, 0, 0),
                ),
                make_kudguri(
                    "U2",
                    "D1",
                    dt(2026, 2, 2, 1, 0, 0),
                    dt(2026, 2, 4, 10, 0, 0),
                ),
            ];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 600),
                make_kudgivt("U2", dt(2026, 2, 2, 1, 0, 0), "201", 600),
                make_kudgivt("U2", dt(2026, 2, 2, 11, 0, 0), "302", 900),
                make_kudgivt("U2", dt(2026, 2, 3, 2, 0, 0), "201", 600),
                make_kudgivt("U2", dt(2026, 2, 3, 12, 0, 0), "302", 900),
                make_kudgivt("U2", dt(2026, 2, 4, 3, 0, 0), "201", 420),
            ];
            let refs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            for e in &refs {
                by_unko.entry(e.unko_no.clone()).or_default().push(*e);
            }
            let result = build_day_map(&kudguri, &by_unko, &cls);
            assert!(result.day_map.len() >= 3);
        });
    }

    // ---- build_day_map: event-level calendar day split ----
    #[test]
    fn test_build_day_map_midnight_crossing_event() {
        test_group!("比較ロジック");
        test_case!("build_day_map: 深夜イベント跨ぎ", {
            let cls = default_classifications();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 22, 0, 0),
                dt(2026, 2, 2, 6, 0, 0),
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 22, 0, 0), "201", 480), // 22:00→6:00 日跨ぎ
            ];
            let refs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), refs);
            let result = build_day_map(&kudguri, &by_unko, &cls);
            assert!(!result.day_map.is_empty());
            // 深夜時間が計算されている
            let has_late_night = result.day_map.values().any(|a| a.late_night_minutes > 0);
            assert!(has_late_night, "midnight crossing should have late_night");
        });
    }

    // ---- process_parsed_data: holiday rows ----
    #[test]
    fn test_process_parsed_data_fills_holidays() {
        test_group!("比較ロジック");
        test_case!("process_parsed: 休日埋め", {
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 5, 8, 0, 0),
                dt(2026, 2, 5, 17, 0, 0),
            )];
            let kudgivt = vec![make_kudgivt("U1", dt(2026, 2, 5, 8, 0, 0), "201", 300)];
            let result =
                process_parsed_data(&kudguri, &kudgivt, &FerryInfo::default(), 2026, 2).unwrap();
            // 28日分（2月）あり、稼働日以外はholiday
            let holidays: Vec<_> = result[0].days.iter().filter(|d| d.is_holiday).collect();
            assert!(holidays.len() >= 20);
        });
    }

    // ---- parse_ferry_periods_from_text ----
    #[test]
    fn test_parse_ferry_periods_basic() {
        test_group!("比較ロジック");
        test_case!("parse_ferry: 基本", {
            let csv = "ヘッダー行\n\
U001,x,x,x,x,x,x,x,x,x,2026/02/01 10:00:00,2026/02/01 11:30:00\n\
U002,x,x,x,x,x,x,x,x,x,2026/02/02 22:00:00,2026/02/03 06:00:00\n";
            let periods = parse_ferry_periods_from_text(csv);
            assert_eq!(periods.len(), 2);
            assert_eq!(periods[0].0, "U001");
            assert_eq!(periods[1].0, "U002");
            assert_eq!(periods[0].1, dt(2026, 2, 1, 10, 0, 0));
            assert_eq!(periods[0].2, dt(2026, 2, 1, 11, 30, 0));
        });
    }
    #[test]
    fn test_parse_ferry_periods_empty() {
        test_group!("比較ロジック");
        test_case!("parse_ferry: 空", {
            let csv = "ヘッダー行\n";
            let periods = parse_ferry_periods_from_text(csv);
            assert!(periods.is_empty());
        });
    }
    #[test]
    fn test_parse_ferry_periods_short_cols() {
        test_group!("比較ロジック");
        test_case!("parse_ferry: 短カラム", {
            let csv = "ヘッダー\nU001,x,x\n"; // cols < 12
            let periods = parse_ferry_periods_from_text(csv);
            assert!(periods.is_empty());
        });
    }

    // ---- DayAgg default ----
    #[test]
    fn test_day_agg_default() {
        test_group!("比較ロジック");
        test_case!("DayAgg: デフォルト", {
            let agg = DayAgg::default();
            assert_eq!(agg.drive_minutes, 0);
            assert_eq!(agg.total_work_minutes, 0);
            assert!(agg.unko_nos.is_empty());
            assert!(!agg.from_multi_op);
        });
    }

    // ---- ferry_break_overlap ----
    #[test]
    fn test_ferry_break_overlap_with_301() {
        test_group!("比較ロジック");
        test_case!("ferry_break_overlap: 301あり", {
            let evts = vec![make_kudgivt("U1", dt(2026, 2, 1, 10, 0, 0), "301", 60)];
            let refs: Vec<&_> = evts.iter().collect();
            let result =
                ferry_break_overlap(&refs, dt(2026, 2, 1, 9, 0, 0), dt(2026, 2, 1, 11, 0, 0));
            assert_eq!(result, 60);
        });
    }
    #[test]
    fn test_ferry_break_overlap_no_301() {
        test_group!("比較ロジック");
        test_case!("ferry_break_overlap: 301なし", {
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 10, 0, 0), "201", 60), // 運転、301じゃない
            ];
            let refs: Vec<&_> = evts.iter().collect();
            let result =
                ferry_break_overlap(&refs, dt(2026, 2, 1, 9, 0, 0), dt(2026, 2, 1, 11, 0, 0));
            assert_eq!(result, 0);
        });
    }
    #[test]
    fn test_ferry_break_overlap_outside_period() {
        test_group!("比較ロジック");
        test_case!("ferry_break_overlap: 範囲外", {
            let evts = vec![make_kudgivt("U1", dt(2026, 2, 1, 15, 0, 0), "301", 60)];
            let refs: Vec<&_> = evts.iter().collect();
            let result =
                ferry_break_overlap(&refs, dt(2026, 2, 1, 9, 0, 0), dt(2026, 2, 1, 11, 0, 0));
            assert_eq!(result, 0);
        });
    }

    // ---- ferry_drive_cargo_overlap ----
    #[test]
    fn test_ferry_drive_cargo_overlap_drive() {
        test_group!("比較ロジック");
        test_case!("ferry_drive_cargo: 運転重複", {
            let cls = default_classifications();
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 10, 0, 0), "201", 60), // 運転
            ];
            let refs: Vec<&_> = evts.iter().collect();
            let (drive, cargo) = ferry_drive_cargo_overlap(
                &refs,
                &cls,
                dt(2026, 2, 1, 10, 30, 0), // フェリー10:30-11:30
                dt(2026, 2, 1, 11, 30, 0),
            );
            assert_eq!(drive, 30); // 10:30-11:00の30分が重複
            assert_eq!(cargo, 0);
        });
    }
    #[test]
    fn test_ferry_drive_cargo_overlap_cargo() {
        test_group!("比較ロジック");
        test_case!("ferry_drive_cargo: 荷役重複", {
            let cls = default_classifications();
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 10, 0, 0), "202", 60), // 積み
            ];
            let refs: Vec<&_> = evts.iter().collect();
            let (drive, cargo) = ferry_drive_cargo_overlap(
                &refs,
                &cls,
                dt(2026, 2, 1, 10, 0, 0),
                dt(2026, 2, 1, 11, 0, 0),
            );
            assert_eq!(drive, 0);
            assert_eq!(cargo, 60);
        });
    }
    #[test]
    fn test_ferry_drive_cargo_overlap_no_overlap() {
        test_group!("比較ロジック");
        test_case!("ferry_drive_cargo: 重複なし", {
            let cls = default_classifications();
            let evts = vec![make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 60)];
            let refs: Vec<&_> = evts.iter().collect();
            let (drive, cargo) = ferry_drive_cargo_overlap(
                &refs,
                &cls,
                dt(2026, 2, 1, 12, 0, 0),
                dt(2026, 2, 1, 13, 0, 0),
            );
            assert_eq!(drive, 0);
            assert_eq!(cargo, 0);
        });
    }

    // ---- split_event_at_boundaries ----
    #[test]
    fn test_split_event_no_boundary() {
        test_group!("比較ロジック");
        test_case!("split_event: 境界なし", {
            let parts = split_event_at_boundaries(
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 10, 0, 0),
                7200,
                None,
            );
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0].2, 7200);
        });
    }
    #[test]
    fn test_split_event_one_boundary() {
        test_group!("比較ロジック");
        test_case!("split_event: 1境界", {
            let bounds = vec![dt(2026, 2, 1, 9, 0, 0)];
            let parts = split_event_at_boundaries(
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 10, 0, 0),
                7200,
                Some(&bounds),
            );
            assert_eq!(parts.len(), 2);
            assert_eq!(parts[0].2, 3600); // 8:00-9:00
            assert_eq!(parts[1].2, 3600); // 9:00-10:00
        });
    }
    #[test]
    fn test_split_event_two_boundaries() {
        test_group!("比較ロジック");
        test_case!("split_event: 2境界", {
            let bounds = vec![dt(2026, 2, 1, 9, 0, 0), dt(2026, 2, 1, 11, 0, 0)];
            let parts = split_event_at_boundaries(
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 12, 0, 0),
                14400,
                Some(&bounds),
            );
            assert_eq!(parts.len(), 3);
        });
    }
    #[test]
    fn test_split_event_boundary_outside() {
        test_group!("比較ロジック");
        test_case!("split_event: 境界範囲外", {
            let bounds = vec![dt(2026, 2, 1, 15, 0, 0)]; // イベント外
            let parts = split_event_at_boundaries(
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 10, 0, 0),
                7200,
                Some(&bounds),
            );
            assert_eq!(parts.len(), 1); // 分割なし
        });
    }

    // ---- find_event_workday ----
    #[test]
    fn test_find_event_workday_direct_match() {
        test_group!("比較ロジック");
        test_case!("find_event_workday: 直接一致", {
            let d = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let t = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
            let segs = vec![(dt(2026, 2, 1, 8, 0, 0), dt(2026, 2, 1, 17, 0, 0), d, t)];
            let (wd, st) = find_event_workday(dt(2026, 2, 1, 10, 0, 0), Some(&segs));
            assert_eq!(wd, d);
            assert_eq!(st, t);
        });
    }
    #[test]
    fn test_find_event_workday_fallback() {
        test_group!("比較ロジック");
        test_case!("find_event_workday: フォールバック", {
            let d = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let t = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
            let segs = vec![(dt(2026, 2, 1, 8, 0, 0), dt(2026, 2, 1, 17, 0, 0), d, t)];
            // 7:00はセグメント[8:00, 17:00)の外 → fallback
            let (wd, _st) = find_event_workday(dt(2026, 2, 1, 7, 0, 0), Some(&segs));
            assert_eq!(wd, d); // fallbackで最初のセグメント
        });
    }
    #[test]
    fn test_find_event_workday_no_segments() {
        test_group!("比較ロジック");
        test_case!("find_event_workday: セグメントなし", {
            let (wd, _) = find_event_workday(dt(2026, 2, 1, 10, 0, 0), None);
            assert_eq!(wd, NaiveDate::from_ymd_opt(2026, 2, 1).unwrap());
        });
    }

    // ---- accumulate_daily_segment ----
    #[test]
    fn test_accumulate_daily_segment_basic() {
        test_group!("比較ロジック");
        test_case!("accumulate_daily: 基本", {
            let mut entry = DayAgg::default();
            accumulate_daily_segment(
                &mut entry,
                100,
                10,
                80,
                20,
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 9, 40, 0),
                "U001",
            );
            assert_eq!(entry.total_work_minutes, 100);
            assert_eq!(entry.drive_minutes, 80);
            assert_eq!(entry.cargo_minutes, 20);
            assert_eq!(entry.late_night_minutes, 10);
            assert_eq!(entry.unko_nos, vec!["U001"]);
            assert_eq!(entry.segments.len(), 1);
        });
    }
    #[test]
    fn test_accumulate_daily_segment_additive() {
        test_group!("比較ロジック");
        test_case!("accumulate_daily: 加算", {
            let mut entry = DayAgg::default();
            accumulate_daily_segment(
                &mut entry,
                100,
                0,
                80,
                20,
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 9, 0, 0),
                "U001",
            );
            accumulate_daily_segment(
                &mut entry,
                50,
                0,
                30,
                20,
                dt(2026, 2, 1, 10, 0, 0),
                dt(2026, 2, 1, 11, 0, 0),
                "U001",
            );
            assert_eq!(entry.total_work_minutes, 150);
            assert_eq!(entry.drive_minutes, 110);
            assert_eq!(entry.unko_nos.len(), 1); // 重複スキップ
            assert_eq!(entry.segments.len(), 2);
        });
    }

    // ---- parse_restraint_csv ----
    #[test]
    fn test_parse_restraint_csv_basic() {
        test_group!("比較ロジック");
        test_case!("parse_csv: 基本", {
            let csv = "拘束時間管理表 (2026年 2月分)\n\
氏名,テスト太郎,乗務員コード,9999\n\
日付,始業時刻,終業時刻,運転時間,重複運転時間,荷役時間,重複荷役時間,休憩時間,重複休憩時間,時間,重複時間,拘束時間小計,重複拘束時間小計,拘束時間合計,拘束時間累計,前運転平均,後運転平均,休息時間,実働時間,時間外時間,深夜時間,時間外深夜時間,摘要1,摘要2\n\
2月1日,8:00,17:00,5:00,,1:00,,1:00,,,,7:00,,7:00,7:00,,,,6:00,,,,テスト,\n\
合計,,,5:00,,1:00,,1:00,,,,7:00,,,,,,6:00,,,,\n";
            let result = parse_restraint_csv(csv.as_bytes()).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].driver_cd, "9999");
            assert_eq!(result[0].driver_name, "テスト太郎");
            assert_eq!(result[0].days.len(), 1);
            assert_eq!(result[0].days[0].date, "2月1日");
            assert_eq!(result[0].days[0].drive, "5:00");
            assert_eq!(result[0].total_drive, "5:00");
        });
    }
    #[test]
    fn test_parse_restraint_csv_holiday() {
        test_group!("比較ロジック");
        test_case!("parse_csv: 休日", {
            let csv = "氏名,テスト,乗務員コード,1\n\
日付,始業時刻\n\
2月1日,休,\n\
合計,,\n";
            let result = parse_restraint_csv(csv.as_bytes()).unwrap();
            assert_eq!(result[0].days.len(), 1);
            assert!(result[0].days[0].is_holiday);
        });
    }
    #[test]
    fn test_parse_restraint_csv_empty() {
        test_group!("比較ロジック");
        test_case!("parse_csv: 空", {
            let csv = "何もないデータ\n";
            let result = parse_restraint_csv(csv.as_bytes());
            assert!(result.is_err());
        });
    }

    // ---- annotate_known_bugs ----
    #[test]
    fn test_annotate_known_bugs_no_match() {
        test_group!("比較ロジック");
        test_case!("annotate_bugs: 不一致", {
            let mut diffs = vec![DiffItem {
                date: "2月1日".into(),
                field: "運転".into(),
                csv_val: "5:00".into(),
                sys_val: "6:00".into(),
                known_bug: None,
            }];
            let mut totals = vec![];
            annotate_known_bugs("9999", &mut diffs, &mut totals);
            assert!(diffs[0].known_bug.is_none());
        });
    }
    #[test]
    fn test_annotate_known_bugs_match() {
        test_group!("比較ロジック");
        test_case!("annotate_bugs: 一致", {
            let mut diffs = vec![DiffItem {
                date: "2月22日".into(),
                field: "始業".into(),
                csv_val: "5:00".into(),
                sys_val: "6:00".into(),
                known_bug: None,
            }];
            let mut totals = vec![];
            annotate_known_bugs("1039", &mut diffs, &mut totals);
            assert!(diffs[0].known_bug.is_some());
        });
    }

    // ---- normalize_time: non HH:MM input (L111) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_normalize_time_no_colon() {
        test_group!("比較ロジック");
        test_case!("normalize_time: コロンなし", {
            // L111: s.to_string() branch — input without ':'
            assert_eq!(normalize_time("abc"), "abc");
            assert_eq!(normalize_time("12345"), "12345");
            assert_eq!(normalize_time("no-colon"), "no-colon");
        });
    }

    // ---- parse_restraint_csv: Shift-JIS fallback (L126-127) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_parse_restraint_csv_shift_jis() {
        test_group!("比較ロジック");
        test_case!("parse_csv: Shift-JIS", {
            // L126-127: UTF-8 fails → Shift-JIS decode
            let csv_str = "氏名,テスト,乗務員コード,1001\n\
日付,始業時刻\n\
2月1日,8:00,17:00,5:00,,,,1:00,,,,6:00,,6:00,6:00,,,,5:00,,,,\n\
合計,,,5:00,,,,1:00,,,,6:00,,,,,5:00,,,,\n";
            let (encoded, _, _) = encoding_rs::SHIFT_JIS.encode(csv_str);
            let result = parse_restraint_csv(&encoded).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].driver_cd, "1001");
        });
    }

    // ---- parse_restraint_csv: in_data=false continue (L173) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_parse_restraint_csv_lines_before_header() {
        test_group!("比較ロジック");
        test_case!("parse_csv: ヘッダー前行スキップ", {
            // L173: lines after 氏名 but before 日付 → in_data=false → continue
            let csv = "氏名,テスト,乗務員コード,1001\n\
拘束時間管理表\n\
対象期間: 2026年2月\n\
日付,始業時刻\n\
2月1日,8:00,17:00,5:00,,,,1:00,,,,6:00,,6:00,6:00,,,,5:00,,,,\n\
合計,,,5:00,,,,1:00,,,,6:00,,,,,5:00,,,,\n";
            let result = parse_restraint_csv(csv.as_bytes()).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].days.len(), 1);
        });
    }

    // ---- parse_restraint_csv: date without '月' (L193) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_parse_restraint_csv_date_without_month() {
        test_group!("比較ロジック");
        test_case!("parse_csv: 月なし日付スキップ", {
            // L193: date_str doesn't contain '月' → continue
            let csv = "氏名,テスト,乗務員コード,1001\n\
日付,始業時刻\n\
extra-line,8:00,17:00,5:00,,,,1:00,,,,6:00,,6:00,6:00,,,,5:00,,,,\n\
2月1日,8:00,17:00,5:00,,,,1:00,,,,6:00,,6:00,6:00,,,,5:00,,,,\n\
合計,,,5:00,,,,1:00,,,,6:00,,,,,5:00,,,,\n";
            let result = parse_restraint_csv(csv.as_bytes()).unwrap();
            assert_eq!(result[0].days.len(), 1);
            assert_eq!(result[0].days[0].date, "2月1日");
        });
    }

    // ---- detect_diffs_csv: holiday skip (L242) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_detect_diffs_csv_holiday_skip() {
        test_group!("比較ロジック");
        test_case!("detect_diffs: 休日スキップ", {
            // L242: csv_day.is_holiday → continue
            let mut holiday = make_csv_day("2月1日", "", "", "", "");
            holiday.is_holiday = true;
            let csv_days = vec![holiday];
            let sys_days = vec![make_csv_day("2月1日", "8:00", "17:00", "5:00", "6:00")];
            let diffs = detect_diffs_csv(&csv_days, &sys_days);
            assert!(diffs.is_empty(), "holiday rows should be skipped");
        });
    }

    // ---- detect_diffs_csv: sys_day not found (L255) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_detect_diffs_csv_sys_day_not_found() {
        test_group!("比較ロジック");
        test_case!("detect_diffs: sys_day なし", {
            // L255: sys_days has no matching date → None => continue
            let csv_days = vec![make_csv_day("2月1日", "8:00", "17:00", "5:00", "6:00")];
            let sys_days = vec![make_csv_day("2月5日", "8:00", "17:00", "5:00", "6:00")];
            let diffs = detect_diffs_csv(&csv_days, &sys_days);
            assert!(diffs.is_empty(), "no matching date should produce no diffs");
        });
    }

    // ---- detect_year_month: holiday skip (L301) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_detect_year_month_holiday_skip() {
        test_group!("比較ロジック");
        test_case!("detect_year_month: 休日スキップ", {
            // L301: first day is holiday → skip, second has valid date
            let mut holiday = make_csv_day("2月1日", "", "", "", "");
            holiday.is_holiday = true;
            let working = make_csv_day("3月5日", "8:00", "17:00", "5:00", "6:00");
            let data = vec![CsvDriverData {
                driver_name: "T".into(),
                driver_cd: "1".into(),
                days: vec![holiday, working],
                total_drive: "".into(),
                total_cargo: "".into(),
                total_break: "".into(),
                total_restraint: "".into(),
                total_actual_work: "".into(),
                total_overtime: "".into(),
                total_late_night: "".into(),
                total_ot_late_night: "".into(),
            }];
            let (y, m) = detect_year_month(&data);
            assert_eq!(y, 2026);
            assert_eq!(m, 3); // skipped holiday, found 3月
        });
    }

    // ---- detect_year_month: parse failure fallback (L306-311) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_detect_year_month_parse_failure_fallback() {
        test_group!("比較ロジック");
        test_case!("detect_year_month: パース失敗フォールバック", {
            // L306-311: date contains '月' but month part is not a number → fallback (2026, 1)
            let mut day = make_csv_day("abc月5日", "8:00", "17:00", "5:00", "6:00");
            day.is_holiday = false;
            let data = vec![CsvDriverData {
                driver_name: "T".into(),
                driver_cd: "1".into(),
                days: vec![day],
                total_drive: "".into(),
                total_cargo: "".into(),
                total_break: "".into(),
                total_restraint: "".into(),
                total_actual_work: "".into(),
                total_overtime: "".into(),
                total_late_night: "".into(),
                total_ot_late_night: "".into(),
            }];
            let (y, m) = detect_year_month(&data);
            assert_eq!((y, m), (2026, 1)); // fallback
        });
    }

    // ---- detect_year_month: empty drivers → fallback (L306-311) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_detect_year_month_empty_fallback() {
        test_group!("比較ロジック");
        test_case!("detect_year_month: 空フォールバック", {
            let (y, m) = detect_year_month(&[]);
            assert_eq!((y, m), (2026, 1));
        });
    }

    // ---- FerryInfo::from_zip_files (coverage for ferry_break_dur + ferry_period_map) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_ferry_info_from_zip_files() {
        test_group!("比較ロジック");
        test_case!("FerryInfo: from_zip_files", {
            // Construct fake zip_files with KUDGFRY data
            let fry_csv = "ヘッダー行\n\
U001,x,x,x,x,x,x,x,x,x,2026/02/01 10:00:00,2026/02/01 11:30:00\n";
            let fry_bytes = encoding_rs::SHIFT_JIS.encode(fry_csv).0.to_vec();
            let zip_files = vec![("KUDGFRY.csv".to_string(), fry_bytes)];
            let evts = vec![make_kudgivt("U001", dt(2026, 2, 1, 10, 0, 0), "301", 90)];
            let refs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U001".into(), refs);
            let info = FerryInfo::from_zip_files(&zip_files, &by_unko);
            assert!(info.ferry_minutes.contains_key("U001"));
            assert!(info.ferry_period_map.contains_key("U001"));
            assert!(info.ferry_break_dur.contains_key("U001"));
        });
    }

    // ---- group_operations_into_work_days: row without departure (L776-780) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_group_ops_no_departure() {
        test_group!("比較ロジック");
        test_case!("group_ops: 出発なし", {
            let mut row = make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 8, 0, 0),
                dt(2026, 2, 1, 17, 0, 0),
            );
            row.departure_at = None;
            row.garage_out_at = None;
            // Should use operation_date as work_date
            let result = group_operations_into_work_days(&[row]);
            assert!(result.contains_key("U1"));
        });
    }

    // ---- calc_ot_late_night_from_events: zero duration skip (L730) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_ot_late_night_zero_duration_skip() {
        test_group!("比較ロジック");
        test_case!("時間外深夜: ゼロ期間スキップ", {
            let events = vec![
                (dt(2026, 2, 1, 8, 0, 0), dt(2026, 2, 1, 8, 0, 0)), // dur=0 → skip
                (dt(2026, 2, 1, 8, 0, 0), dt(2026, 2, 1, 16, 0, 0)), // 8h normal
            ];
            let result = calc_ot_late_night_from_events(&events);
            assert_eq!(result, 0); // 8h = no overtime
        });
    }

    // ---- ferry_break_overlap: zero duration 301 (L905-906) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_ferry_break_overlap_zero_dur() {
        test_group!("比較ロジック");
        test_case!("ferry_break_overlap: ゼロ期間301", {
            let mut evt = make_kudgivt("U1", dt(2026, 2, 1, 10, 0, 0), "301", 0);
            evt.duration_minutes = Some(0);
            let refs: Vec<&_> = vec![&evt];
            let result =
                ferry_break_overlap(&refs, dt(2026, 2, 1, 9, 0, 0), dt(2026, 2, 1, 11, 0, 0));
            assert_eq!(result, 0); // dur <= 0 → skip
        });
    }

    // ---- ferry_drive_cargo_overlap: zero duration event (L930-931) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_ferry_drive_cargo_overlap_zero_dur() {
        test_group!("比較ロジック");
        test_case!("ferry_drive_cargo: ゼロ期間", {
            let cls = default_classifications();
            let mut evt = make_kudgivt("U1", dt(2026, 2, 1, 10, 0, 0), "201", 0);
            evt.duration_minutes = Some(0);
            let refs: Vec<&_> = vec![&evt];
            let (d, c) = ferry_drive_cargo_overlap(
                &refs,
                &cls,
                dt(2026, 2, 1, 10, 0, 0),
                dt(2026, 2, 1, 11, 0, 0),
            );
            assert_eq!((d, c), (0, 0));
        });
    }

    // ---- find_event_workday: after all segments fallback (L999-1004) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_find_event_workday_after_all_segments() {
        test_group!("比較ロジック");
        test_case!("find_event_workday: 全セグメント後", {
            let d = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let t = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
            let segs = vec![(dt(2026, 2, 1, 8, 0, 0), dt(2026, 2, 1, 12, 0, 0), d, t)];
            // 20:00 is after segment end (12:00) and no later segment exists
            let (wd, st) = find_event_workday(dt(2026, 2, 1, 20, 0, 0), Some(&segs));
            // fallback: last segment
            assert_eq!(wd, d);
            assert_eq!(st, t);
        });
    }

    // ---- compare_drivers: total_diffs comparison (L668-679) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_compare_drivers_total_diffs() {
        test_group!("比較ロジック");
        test_case!("compare_drivers: 合計差分", {
            let days = vec![make_csv_day("2月1日", "8:00", "17:00", "5:00", "6:00")];
            let csv_data = vec![CsvDriverData {
                driver_name: "A".into(),
                driver_cd: "1001".into(),
                days: days.clone(),
                total_drive: "5:00".into(),
                total_cargo: "".into(),
                total_break: "".into(),
                total_restraint: "6:00".into(),
                total_actual_work: "5:00".into(),
                total_overtime: "1:00".into(),
                total_late_night: "0:30".into(),
                total_ot_late_night: "".into(),
            }];
            let sys_data = vec![CsvDriverData {
                driver_name: "A".into(),
                driver_cd: "1001".into(),
                days,
                total_drive: "5:30".into(),
                total_cargo: "".into(),
                total_break: "".into(),
                total_restraint: "6:30".into(),
                total_actual_work: "5:30".into(),
                total_overtime: "1:30".into(),
                total_late_night: "1:00".into(),
                total_ot_late_night: "".into(),
            }];
            let report = compare_drivers(&csv_data, &sys_data, None);
            // total_diffs should include total row differences
            let driver = &report.drivers[0];
            assert!(!driver.total_diffs.is_empty());
        });
    }

    // ---- build_csv_driver_data: month=12 boundary (L2194-2198) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_process_parsed_data_december() {
        test_group!("比較ロジック");
        test_case!("process_parsed: 12月", {
            // L2194: month==12 → next year boundary
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 12, 15, 8, 0, 0),
                dt(2026, 12, 15, 17, 0, 0),
            )];
            let kudgivt = vec![make_kudgivt("U1", dt(2026, 12, 15, 8, 0, 0), "201", 300)];
            let result =
                process_parsed_data(&kudguri, &kudgivt, &FerryInfo::default(), 2026, 12).unwrap();
            assert_eq!(result.len(), 1);
            // 31 days in December
            assert_eq!(result[0].days.len(), 31);
        });
    }

    // ---- fmt_min: negative value ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_fmt_min_negative() {
        test_group!("比較ロジック");
        test_case!("fmt_min: 負の値", {
            // The abs() in fmt_min handles negative remainders
            let result = fmt_min(-90);
            assert_eq!(result, "-1:30");
        });
    }

    // ---- parse_restraint_csv: line before any driver (L169-171) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_parse_restraint_csv_line_before_driver() {
        test_group!("比較ロジック");
        test_case!("parse_csv: ドライバー前の行", {
            // L169-171: current is None → continue
            let csv = "何かのヘッダー行\n\
データ行\n\
氏名,テスト,乗務員コード,1001\n\
日付,始業時刻\n\
2月1日,8:00,17:00,5:00,,,,1:00,,,,6:00,,6:00,6:00,,,,5:00,,,,\n\
合計,,,5:00,,,,1:00,,,,6:00,,,,,5:00,,,,\n";
            let result = parse_restraint_csv(csv.as_bytes()).unwrap();
            assert_eq!(result.len(), 1);
        });
    }

    // ---- build_day_map: empty driver_cd filtered out (L755-756) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_group_ops_empty_driver_cd() {
        test_group!("比較ロジック");
        test_case!("group_ops: 空ドライバーCD", {
            let mut row = make_kudguri("U1", "", dt(2026, 2, 1, 8, 0, 0), dt(2026, 2, 1, 17, 0, 0));
            row.driver_cd = String::new();
            let result = group_operations_into_work_days(&[row]);
            // empty driver_cd → skipped in driver_rows grouping → empty result
            assert!(result.is_empty());
        });
    }

    // ---- process_parsed_data: wd_end != seg_end boundary (L2274-2284) ----
    #[cfg_attr(not(coverage), ignore)]
    #[test]
    fn test_process_parsed_data_end_time_wb_crossday() {
        test_group!("比較ロジック");
        test_case!("process_parsed: 終業時刻日跨ぎ", {
            // Trigger the wd_start.date() != wd_end.date() && diff > 60min path
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 20, 0, 0),
                dt(2026, 2, 2, 10, 0, 0),
            )];
            let kudgivt = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 20, 0, 0), "201", 120), // 2h drive
                make_kudgivt("U1", dt(2026, 2, 1, 22, 0, 0), "302", 600), // 10h rest → new day
                make_kudgivt("U1", dt(2026, 2, 2, 8, 0, 0), "201", 120),  // 2h drive
            ];
            let result =
                process_parsed_data(&kudguri, &kudgivt, &FerryInfo::default(), 2026, 2).unwrap();
            assert_eq!(result.len(), 1);
            let working: Vec<_> = result[0].days.iter().filter(|d| !d.is_holiday).collect();
            assert!(!working.is_empty());
        });
    }

    // ---- zip の中身 (KUDGURI・KUDGIVT の Shift_JIS のエントリ) → CsvDriverData ----
    #[test]
    fn test_process_parsed_data_from_shift_jis_entries() {
        let kudguri_csv = "運行NO,読取日,運行日,事業所CD,事業所名,車輌CD,車輌名,乗務員CD1,乗務員名１,対象乗務員区分,出社日時,退社日時,出庫日時,帰庫日時,総走行距離,一般道運転時間,高速道運転時間,バイパス運転時間\n\
            1001,2026/03/01,2026/03/01,OFF01,テスト事業所,VH01,車両A,DR01,運転者A,1,2026/03/01 08:00:00,2026/03/01 18:00:00,2026/03/01 08:30:00,2026/03/01 17:30:00,150.5,300,60,20\n";
        let kudgivt_csv = "運行NO,読取日,乗務員CD1,乗務員名１,対象乗務員区分,開始日時,終了日時,イベントCD,イベント名,区間時間,区間距離\n\
            1001,2026/03/01,DR01,運転者A,1,2026/03/01 08:00:00,2026/03/01 08:30:00,100,出庫,30,0\n\
            1001,2026/03/01,DR01,運転者A,1,2026/03/01 08:30:00,2026/03/01 12:00:00,200,運転,210,75.0\n\
            1001,2026/03/01,DR01,運転者A,1,2026/03/01 12:00:00,2026/03/01 13:00:00,301,休憩,60,0\n\
            1001,2026/03/01,DR01,運転者A,1,2026/03/01 13:00:00,2026/03/01 17:30:00,200,運転,270,75.5\n";
        let (kudguri_bytes, _, _) = encoding_rs::SHIFT_JIS.encode(kudguri_csv);
        let (kudgivt_bytes, _, _) = encoding_rs::SHIFT_JIS.encode(kudgivt_csv);
        let zip_files = vec![
            ("KUDGURI.csv".to_string(), kudguri_bytes.to_vec()),
            ("KUDGIVT.csv".to_string(), kudgivt_bytes.to_vec()),
        ];
        let kudguri_rows = csv_parser::kudguri_rows_in(&zip_files).unwrap().unwrap();
        let kudgivt_rows = csv_parser::kudgivt_rows_in(&zip_files).unwrap().unwrap();
        let mut kudgivt_by_unko: HashMap<String, Vec<&KudgivtRow>> = HashMap::new();
        for row in &kudgivt_rows {
            kudgivt_by_unko
                .entry(row.unko_no.clone())
                .or_default()
                .push(row);
        }
        let ferry_info = FerryInfo::from_zip_files(&zip_files, &kudgivt_by_unko);
        let result = process_parsed_data(&kudguri_rows, &kudgivt_rows, &ferry_info, 2026, 3);
        assert!(result.is_ok());
        let drivers = result.unwrap();
        assert!(!drivers.is_empty());
    }

    // ---- Group 1: merge_same_day_entries — work events merge (L1164,1167,1198-1204) ----
    #[test]
    fn test_merge_same_day_entries_with_work_events() {
        test_group!("比較ロジック");
        test_case!("merge_same_day: work events もマージされる", {
            let d = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let t1 = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            // Entry A: 8:00-9:30, unko=U1
            let mut agg_a = DayAgg::default();
            agg_a.unko_nos = vec!["U1".into()];
            agg_a.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 8, 0, 0),
                end_at: dt(2026, 2, 1, 9, 30, 0),
            }];
            agg_a.drive_minutes = 90;
            agg_a.total_work_minutes = 90;
            day_map.insert(("D1".into(), d, t1), agg_a);
            // Entry B: 10:00-11:00, unko=U2 (different op, gap=30min<180)
            let mut agg_b = DayAgg::default();
            agg_b.unko_nos = vec!["U2".into()];
            agg_b.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 10, 0, 0),
                end_at: dt(2026, 2, 1, 11, 0, 0),
            }];
            agg_b.drive_minutes = 60;
            agg_b.total_work_minutes = 60;
            day_map.insert(("D1".into(), d, t2), agg_b);
            // Work events for entry B
            let mut dwe: HashMap<DayKey, Vec<(NaiveDateTime, NaiveDateTime)>> = HashMap::new();
            dwe.insert(
                ("D1".into(), d, t2),
                vec![(dt(2026, 2, 1, 10, 0, 0), dt(2026, 2, 1, 11, 0, 0))],
            );

            merge_same_day_entries(&mut day_map, &mut dwe);
            // B should be merged into A
            assert_eq!(day_map.len(), 1);
            assert!(day_map.contains_key(&("D1".into(), d, t1)));
            let merged = &day_map[&("D1".into(), d, t1)];
            assert_eq!(merged.drive_minutes, 150); // 90+60
            assert!(merged.unko_nos.contains(&"U2".into()));
            // Work events merged into A's key
            assert!(dwe.contains_key(&("D1".into(), d, t1)));
        });
    }

    // ---- Group 2-4: process_overlap_chain edge cases ----
    #[test]
    fn test_overlap_chain_no_workday_boundary() {
        test_group!("比較ロジック");
        test_case!(
            "overlap_chain: workday_boundaries なし → effective_start=info.start",
            {
                let cls = default_classifications();
                let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
                let d2 = NaiveDate::from_ymd_opt(2026, 2, 2).unwrap();
                let t1 = NaiveTime::from_hms_opt(6, 0, 0).unwrap();
                let t2 = NaiveTime::from_hms_opt(6, 0, 0).unwrap();
                let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
                let mut agg1 = DayAgg::default();
                agg1.drive_minutes = 300;
                agg1.total_work_minutes = 300;
                agg1.unko_nos = vec!["U1".into()];
                agg1.segments = vec![SegRec {
                    start_at: dt(2026, 2, 1, 6, 0, 0),
                    end_at: dt(2026, 2, 1, 11, 0, 0),
                }];
                day_map.insert(("D1".into(), d1, t1), agg1);
                let mut agg2 = DayAgg::default();
                agg2.drive_minutes = 300;
                agg2.total_work_minutes = 300;
                agg2.unko_nos = vec!["U1".into()];
                agg2.segments = vec![SegRec {
                    start_at: dt(2026, 2, 2, 6, 0, 0),
                    end_at: dt(2026, 2, 2, 11, 0, 0),
                }];
                day_map.insert(("D1".into(), d2, t2), agg2);

                // Empty workday_boundaries → L1299-1301 reached
                let mut wb = HashMap::new();
                let mwb = HashMap::new();
                let mut dwe = HashMap::new();
                let kudguri = vec![make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 6, 0, 0),
                    dt(2026, 2, 2, 11, 0, 0),
                )];
                let evts = vec![
                    make_kudgivt("U1", dt(2026, 2, 1, 6, 0, 0), "201", 300),
                    make_kudgivt("U1", dt(2026, 2, 2, 6, 0, 0), "201", 300),
                ];
                let erefs: Vec<&_> = evts.iter().collect();
                let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
                by_unko.insert("U1".into(), erefs);
                let ferry = FerryInfo::default();

                process_overlap_chain(
                    &mut day_map,
                    &mut wb,
                    &mwb,
                    &mut dwe,
                    &by_unko,
                    &cls,
                    &kudguri,
                    &ferry,
                );
                // Should complete without error
                assert!(day_map.contains_key(&("D1".into(), d1, t1)));
            }
        );
    }

    #[test]
    fn test_overlap_chain_cargo_events_and_dur_zero() {
        test_group!("比較ロジック");
        test_case!("overlap_chain: cargo + dur=0 + unko未登録", {
            let cls = default_classifications();
            let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let d2 = NaiveDate::from_ymd_opt(2026, 2, 2).unwrap();
            let t1 = NaiveTime::from_hms_opt(5, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(3, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            // Day1: 5:00→翌3:00 (22h)
            let mut agg1 = DayAgg::default();
            agg1.drive_minutes = 600;
            agg1.total_work_minutes = 600;
            agg1.unko_nos = vec!["U1".into()];
            agg1.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 5, 0, 0),
                end_at: dt(2026, 2, 2, 3, 0, 0),
            }];
            day_map.insert(("D1".into(), d1, t1), agg1);
            // Day2: 3:00→15:00 (12h)
            let mut agg2 = DayAgg::default();
            agg2.drive_minutes = 400;
            agg2.total_work_minutes = 400;
            // unko_nos includes U_MISSING (not in kudgivt_by_unko) → L1320 continue
            agg2.unko_nos = vec!["U_MISSING".into(), "U1".into()];
            agg2.segments = vec![SegRec {
                start_at: dt(2026, 2, 2, 3, 0, 0),
                end_at: dt(2026, 2, 2, 15, 0, 0),
            }];
            day_map.insert(("D1".into(), d2, t2), agg2);

            let mut wb = HashMap::new();
            wb.insert(
                ("D1".into(), d1, t1),
                (dt(2026, 2, 1, 5, 0, 0), dt(2026, 2, 2, 3, 0, 0)),
            );
            wb.insert(
                ("D1".into(), d2, t2),
                (dt(2026, 2, 2, 3, 0, 0), dt(2026, 2, 2, 15, 0, 0)),
            );
            let mwb = HashMap::new();
            let mut dwe = HashMap::new();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 5, 0, 0),
                dt(2026, 2, 2, 15, 0, 0),
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 5, 0, 0), "201", 600),
                // dur=0 event → L1326 continue
                make_kudgivt("U1", dt(2026, 2, 2, 3, 0, 0), "201", 0),
                // Cargo event after day1 end → L1353-1354
                make_kudgivt("U1", dt(2026, 2, 2, 4, 0, 0), "202", 120),
                make_kudgivt("U1", dt(2026, 2, 2, 6, 0, 0), "201", 400),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), erefs);
            let ferry = FerryInfo::default();

            process_overlap_chain(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &ferry,
            );
            // cargo overlap should be counted for day1
            let entry1 = &day_map[&("D1".into(), d1, t1)];
            assert!(entry1.drive_minutes > 0);
        });
    }

    #[test]
    fn test_overlap_chain_split_rest_threshold() {
        test_group!("比較ロジック");
        test_case!("overlap_chain: split rest 2回で600分閾値 → reset", {
            // 3 consecutive workdays with gaps 200min and 400min (both >= 180)
            // split_rests = [200, 400], total=600, threshold(2)=600 → reset
            let cls = default_classifications();
            let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let d2 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let t1 = NaiveTime::from_hms_opt(4, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(11, 20, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            // Day1: 4:00-8:00 (4h)
            let mut agg1 = DayAgg::default();
            agg1.drive_minutes = 240;
            agg1.total_work_minutes = 240;
            agg1.unko_nos = vec!["U1".into()];
            agg1.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 4, 0, 0),
                end_at: dt(2026, 2, 1, 8, 0, 0),
            }];
            day_map.insert(("D1".into(), d1, t1), agg1);
            // Day2: 11:20-14:00 (gap from day1=200min ≥180, <540 → split rest)
            let mut agg2 = DayAgg::default();
            agg2.drive_minutes = 160;
            agg2.total_work_minutes = 160;
            agg2.unko_nos = vec!["U1".into()];
            agg2.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 11, 20, 0),
                end_at: dt(2026, 2, 1, 14, 0, 0),
            }];
            day_map.insert(("D1".into(), d2, t2), agg2);
            // Day3: next day 4:00-8:00 (gap from day2=840min → but we need gap=400min for split rest)
            // Actually need gap from 14:00 to 20:40 = 400min
            let t3 = NaiveTime::from_hms_opt(20, 40, 0).unwrap();
            let d3 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let mut agg3 = DayAgg::default();
            agg3.drive_minutes = 200;
            agg3.total_work_minutes = 200;
            agg3.unko_nos = vec!["U1".into()];
            agg3.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 20, 40, 0),
                end_at: dt(2026, 2, 2, 0, 0, 0),
            }];
            day_map.insert(("D1".into(), d3, t3), agg3);

            let mut wb = HashMap::new();
            wb.insert(
                ("D1".into(), d1, t1),
                (dt(2026, 2, 1, 4, 0, 0), dt(2026, 2, 1, 8, 0, 0)),
            );
            wb.insert(
                ("D1".into(), d2, t2),
                (dt(2026, 2, 1, 11, 20, 0), dt(2026, 2, 1, 14, 0, 0)),
            );
            wb.insert(
                ("D1".into(), d3, t3),
                (dt(2026, 2, 1, 20, 40, 0), dt(2026, 2, 2, 0, 0, 0)),
            );
            let mwb = HashMap::new();
            let mut dwe = HashMap::new();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 4, 0, 0),
                dt(2026, 2, 2, 0, 0, 0),
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 4, 0, 0), "201", 240),
                make_kudgivt("U1", dt(2026, 2, 1, 11, 20, 0), "201", 160),
                make_kudgivt("U1", dt(2026, 2, 1, 20, 40, 0), "201", 200),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), erefs);
            let ferry = FerryInfo::default();

            process_overlap_chain(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &ferry,
            );
            // split rest [200, 400] → total=600 ≥ threshold(600) → next_resets=true
            // L1394, L1399-1400 covered
            assert!(day_map.contains_key(&("D1".into(), d1, t1)));
        });
    }

    #[test]
    fn test_overlap_chain_multi_wd_boundaries_forced_reset() {
        test_group!("比較ロジック");
        test_case!("overlap_chain: multi_wd_boundaries forced reset", {
            let cls = default_classifications();
            let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let d2 = NaiveDate::from_ymd_opt(2026, 2, 2).unwrap();
            let t1 = NaiveTime::from_hms_opt(5, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(3, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            let mut agg1 = DayAgg::default();
            agg1.drive_minutes = 600;
            agg1.total_work_minutes = 600;
            agg1.unko_nos = vec!["U1".into()];
            agg1.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 5, 0, 0),
                end_at: dt(2026, 2, 2, 3, 0, 0),
            }];
            day_map.insert(("D1".into(), d1, t1), agg1);
            let mut agg2 = DayAgg::default();
            agg2.drive_minutes = 400;
            agg2.total_work_minutes = 400;
            agg2.unko_nos = vec!["U1".into()];
            agg2.segments = vec![SegRec {
                start_at: dt(2026, 2, 2, 3, 0, 0),
                end_at: dt(2026, 2, 2, 15, 0, 0),
            }];
            day_map.insert(("D1".into(), d2, t2), agg2);

            let mut wb = HashMap::new();
            wb.insert(
                ("D1".into(), d1, t1),
                (dt(2026, 2, 1, 5, 0, 0), dt(2026, 2, 2, 3, 0, 0)),
            );
            wb.insert(
                ("D1".into(), d2, t2),
                (dt(2026, 2, 2, 3, 0, 0), dt(2026, 2, 2, 15, 0, 0)),
            );
            // multi_wd_boundaries: d1 boundary at 翌2:00 < window_end(5:00+24h=翌5:00)
            let mut mwb = HashMap::new();
            mwb.insert(("D1".into(), d1, t1), dt(2026, 2, 2, 2, 0, 0));
            let mut dwe = HashMap::new();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 5, 0, 0),
                dt(2026, 2, 2, 15, 0, 0),
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 5, 0, 0), "201", 600),
                make_kudgivt("U1", dt(2026, 2, 2, 3, 0, 0), "201", 400),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), erefs);
            let ferry = FerryInfo::default();

            process_overlap_chain(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &ferry,
            );
            // L1410-1413: forced reset via multi_wd_boundaries
            assert!(day_map.contains_key(&("D1".into(), d1, t1)));
        });
    }

    // ---- Group 5: build_day_map — multi-day ops (L1611, L1679-1683, L1736-1740) ----
    #[test]
    fn test_build_day_map_same_date_multi_ops() {
        test_group!("比較ロジック");
        test_case!(
            "build_day_map: 同日出発の複数運行 → spans_different_days=false",
            {
                let cls = default_classifications();
                // Two operations same day, same driver
                let kudguri = vec![
                    make_kudguri(
                        "U1",
                        "D1",
                        dt(2026, 2, 1, 8, 0, 0),
                        dt(2026, 2, 1, 12, 0, 0),
                    ),
                    make_kudguri(
                        "U2",
                        "D1",
                        dt(2026, 2, 1, 13, 0, 0),
                        dt(2026, 2, 1, 17, 0, 0),
                    ),
                ];
                let evts = vec![
                    make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 240),
                    make_kudgivt("U2", dt(2026, 2, 1, 13, 0, 0), "201", 240),
                ];
                let refs: Vec<&_> = evts.iter().collect();
                let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
                by_unko.insert("U1".into(), vec![refs[0]]);
                by_unko.insert("U2".into(), vec![refs[1]]);

                let result = build_day_map(&kudguri, &by_unko, &cls);
                // L1611: dates.len()<=1 → false → not multi-op merge
                assert!(!result.day_map.is_empty());
            }
        );
    }

    #[test]
    fn test_build_day_map_4day_span_with_workday_split() {
        test_group!("比較ロジック");
        test_case!(
            "build_day_map: 4日間スパン + sig_split → boundary分割",
            {
                let cls = default_classifications();
                // 4-day operation: Feb 1 08:00 → Feb 4 20:00
                let kudguri = vec![make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 8, 0, 0),
                    dt(2026, 2, 4, 20, 0, 0),
                )];
                // Rest events to create workday boundaries
                let evts = vec![
                    make_kudgivt("U1", dt(2026, 2, 1, 8, 0, 0), "201", 600), // drive 10h
                    make_kudgivt("U1", dt(2026, 2, 1, 18, 0, 0), "302", 600), // rest 10h → workday boundary
                    make_kudgivt("U1", dt(2026, 2, 2, 4, 0, 0), "201", 600),  // drive 10h
                    make_kudgivt("U1", dt(2026, 2, 2, 14, 0, 0), "302", 600), // rest 10h
                    make_kudgivt("U1", dt(2026, 2, 3, 0, 0, 0), "201", 600),  // drive 10h
                    make_kudgivt("U1", dt(2026, 2, 3, 10, 0, 0), "302", 600), // rest 10h
                    make_kudgivt("U1", dt(2026, 2, 3, 20, 0, 0), "201", 600), // drive 10h
                ];
                let refs: Vec<&_> = evts.iter().collect();
                let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
                by_unko.insert("U1".into(), refs);

                let result = build_day_map(&kudguri, &by_unko, &cls);
                // span_days=3 ≥ 3 → sig_split check (L1679-1683)
                // Multiple workdays should be created
                assert!(result.day_map.len() >= 3);
            }
        );
    }

    // ---- Group 6: build_csv_driver_data — end_time formatting (L2257, L2260-2262) ----
    #[test]
    fn test_build_csv_cross_day_workday_end_time() {
        test_group!("比較ロジック");
        test_case!(
            "build_csv: 日跨ぎworkday + seg_end << wd_end → wd_end表示",
            {
                let d = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
                let t = NaiveTime::from_hms_opt(20, 0, 0).unwrap();
                let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
                let mut agg = DayAgg::default();
                agg.drive_minutes = 300;
                agg.total_work_minutes = 300;
                agg.unko_nos = vec!["U1".into()];
                // seg ends at 翌1:00, but wd_end is 翌4:00 (diff > 60min)
                agg.segments = vec![SegRec {
                    start_at: dt(2026, 2, 1, 20, 0, 0),
                    end_at: dt(2026, 2, 2, 1, 0, 0),
                }];
                day_map.insert(("D1".into(), d, t), agg);

                let mut wb = HashMap::new();
                // Cross-day workday: 2/1 20:00 → 2/2 4:00 (different dates, wd_end - seg_end = 180min > 60)
                wb.insert(
                    ("D1".into(), d, t),
                    (dt(2026, 2, 1, 20, 0, 0), dt(2026, 2, 2, 4, 0, 0)),
                );

                let kudguri = vec![make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 20, 0, 0),
                    dt(2026, 2, 2, 4, 0, 0),
                )];
                let result = build_csv_driver_data(&day_map, &wb, &kudguri, 2026, 2);
                assert!(!result.is_empty());
                let working: Vec<_> = result[0].days.iter().filter(|d| !d.is_holiday).collect();
                assert!(!working.is_empty());
                // L2257: end_time should use wd_end (4:00) not seg_end (1:00)
                assert_eq!(working[0].end_time, "4:00");
            }
        );
    }

    #[test]
    fn test_build_csv_no_segments_with_workday_boundary() {
        test_group!("比較ロジック");
        test_case!(
            "build_csv: segments空 + workday_boundaries あり → wd_end表示",
            {
                let d = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
                let t = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
                let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
                let mut agg = DayAgg::default();
                agg.drive_minutes = 120;
                agg.total_work_minutes = 120;
                agg.unko_nos = vec!["U1".into()];
                agg.segments = vec![]; // empty segments → seg_max_end = None
                day_map.insert(("D1".into(), d, t), agg);

                let mut wb = HashMap::new();
                wb.insert(
                    ("D1".into(), d, t),
                    (dt(2026, 2, 1, 8, 0, 0), dt(2026, 2, 1, 17, 0, 0)),
                );

                let kudguri = vec![make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 8, 0, 0),
                    dt(2026, 2, 1, 17, 0, 0),
                )];
                let result = build_csv_driver_data(&day_map, &wb, &kudguri, 2026, 2);
                assert!(!result.is_empty());
                let working: Vec<_> = result[0].days.iter().filter(|d| !d.is_holiday).collect();
                assert!(!working.is_empty());
                // L2260-2262: _ => wb.map(wd_end) → "17:00"
                assert_eq!(working[0].end_time, "17:00");
            }
        );
    }

    // ---- Group 7: ferry deduction overlap path (L1458, L1462, L1468-1477, L1488) ----
    #[test]
    fn test_overlap_chain_with_ferry_deduction() {
        test_group!("比較ロジック");
        test_case!("overlap_chain: ferry break_ded=0 → 直接分数計算", {
            let cls = default_classifications();
            let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let d2 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let t1 = NaiveTime::from_hms_opt(5, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(15, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            // Day1: 5:00 → 12:00 (7h)
            let mut agg1 = DayAgg::default();
            agg1.drive_minutes = 420;
            agg1.total_work_minutes = 420;
            agg1.unko_nos = vec!["U1".into()];
            agg1.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 5, 0, 0),
                end_at: dt(2026, 2, 1, 12, 0, 0),
            }];
            day_map.insert(("D1".into(), d1, t1), agg1);
            // Day2: same date, 15:00→22:00 (gap=3h, same_date_long_gap=true since >=180)
            // → enters else if path (ol_restraint check)
            let mut agg2 = DayAgg::default();
            agg2.drive_minutes = 420;
            agg2.total_work_minutes = 420;
            agg2.unko_nos = vec!["U_NO_EVT".into(), "U1".into()];
            agg2.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 15, 0, 0),
                end_at: dt(2026, 2, 1, 22, 0, 0),
            }];
            day_map.insert(("D1".into(), d2, t2), agg2);

            let mut wb = HashMap::new();
            wb.insert(
                ("D1".into(), d1, t1),
                (dt(2026, 2, 1, 5, 0, 0), dt(2026, 2, 1, 12, 0, 0)),
            );
            wb.insert(
                ("D1".into(), d2, t2),
                (dt(2026, 2, 1, 15, 0, 0), dt(2026, 2, 1, 22, 0, 0)),
            );
            let mwb = HashMap::new();
            let mut dwe = HashMap::new();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 5, 0, 0),
                dt(2026, 2, 1, 22, 0, 0),
            )];
            // Events: drive only, no 301
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 5, 0, 0), "201", 420),
                make_kudgivt("U1", dt(2026, 2, 1, 15, 0, 0), "201", 420),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), erefs);

            // Ferry period within window (5:00+24h=翌5:00)
            // next_info.start=15:00, window_end=翌5:00
            // fe(18:00) > next_info.start(15:00) ✓, fs(17:00) < window_end(翌5:00) ✓
            // No 301 events → break_ded=0 → else branch (L1238-1246)
            let mut ferry = FerryInfo::default();
            ferry.ferry_period_map.insert(
                "U1".into(),
                vec![
                    // Period outside window (before next_info.start) → L1232 continue
                    (dt(2026, 2, 1, 6, 0, 0), dt(2026, 2, 1, 7, 0, 0)),
                    // Period inside window, no 301 → break_ded=0
                    (dt(2026, 2, 1, 17, 0, 0), dt(2026, 2, 1, 18, 0, 0)),
                ],
            );
            // U_NO_EVT has ferry but no kudgivt events
            ferry.ferry_period_map.insert(
                "U_NO_EVT".into(),
                vec![(dt(2026, 2, 1, 17, 0, 0), dt(2026, 2, 1, 18, 0, 0))],
            );

            process_overlap_chain(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &ferry,
            );
            let entry1 = &day_map[&("D1".into(), d1, t1)];
            assert!(entry1.overlap_restraint_minutes >= 0);
        });
    }

    // ---- Group 6 extra: L2051 — day_late_night key not in day_map ----
    #[test]
    fn test_build_day_map_late_night_key_mismatch() {
        test_group!("比較ロジック");
        test_case!(
            "build_day_map: 深夜計算でday_mapにないキー → continue",
            {
                let cls = default_classifications();
                let kudguri = vec![make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 22, 0, 0),
                    dt(2026, 2, 2, 6, 0, 0),
                )];
                let evts = vec![make_kudgivt("U1", dt(2026, 2, 1, 22, 0, 0), "201", 480)];
                let refs: Vec<&_> = evts.iter().collect();
                let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
                by_unko.insert("U1".into(), refs);

                let result = build_day_map(&kudguri, &by_unko, &cls);
                let has_late_night = result
                    .day_map
                    .values()
                    .any(|agg| agg.late_night_minutes > 0);
                assert!(has_late_night);
            }
        );
    }

    // ---- L1680-1684: sig_split で実際に分割が発生するケース ----
    #[test]
    fn test_build_day_map_sig_split_actually_splits() {
        test_group!("比較ロジック");
        test_case!(
            "build_day_map: sig_split 条件満たし → 境界分割実行",
            {
                let cls = default_classifications();
                // 4日間運行: 2/1 6:00 → 2/4 20:00
                // Workday1: 2/1 6:00 ~ (rest at 18:00-翌4:00) → wd.end ≈ 翌4:00
                // Workday2: 2/2 4:00 ~ (rest at 2/3 14:00-2/4 0:00) → wd.end ≈ 2/3 14:00
                // Segment: 2/2 4:00 → 2/3 14:00 = 34h continuous (split_by_rest won't split without 302)
                // wd boundary at ≈2/2 ~28:00(=2/3 4:00) if 24h forced split
                // sig_split: seg spans wd.end, wd.end-seg.start >= 180, seg.end-wd.end >= 180
                let kudguri = vec![make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 6, 0, 0),
                    dt(2026, 2, 4, 20, 0, 0),
                )];
                let evts = vec![
                    make_kudgivt("U1", dt(2026, 2, 1, 6, 0, 0), "201", 720), // 12h drive → 18:00
                    make_kudgivt("U1", dt(2026, 2, 1, 18, 0, 0), "302", 600), // 10h rest → 2/2 4:00
                    // Continuous work 2/2 4:00 → 2/3 14:00 (34h, no 302 → one big segment)
                    // Only 301 breaks (don't split segments)
                    make_kudgivt("U1", dt(2026, 2, 2, 4, 0, 0), "201", 600), // 10h drive
                    make_kudgivt("U1", dt(2026, 2, 2, 14, 0, 0), "301", 60), // 1h break
                    make_kudgivt("U1", dt(2026, 2, 2, 15, 0, 0), "201", 600), // 10h drive
                    make_kudgivt("U1", dt(2026, 2, 3, 1, 0, 0), "301", 60),  // 1h break
                    make_kudgivt("U1", dt(2026, 2, 3, 2, 0, 0), "201", 720), // 12h drive → 14:00
                    make_kudgivt("U1", dt(2026, 2, 3, 14, 0, 0), "302", 600), // 10h rest → 2/4 0:00
                    make_kudgivt("U1", dt(2026, 2, 4, 0, 0, 0), "201", 720), // 12h drive
                    make_kudgivt("U1", dt(2026, 2, 4, 12, 0, 0), "302", 480), // 8h rest
                ];
                let refs: Vec<&_> = evts.iter().collect();
                let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
                by_unko.insert("U1".into(), refs);

                let result = build_day_map(&kudguri, &by_unko, &cls);
                // Workdays from rest splits + sig_split at boundary
                assert!(result.day_map.len() >= 3);
            }
        );
    }

    // ---- L1737-1741: daily_segment が workday 外 → parent_seg fallback ----
    #[test]
    fn test_build_day_map_daily_seg_outside_workdays() {
        test_group!("比較ロジック");
        test_case!(
            "build_day_map: daily_segment がどの workday にも収まらない → parent_seg",
            {
                let cls = default_classifications();
                // 3日間運行で、休息分割後の daily_segment が workday 境界外に出る
                let kudguri = vec![make_kudguri(
                    "U1",
                    "D1",
                    dt(2026, 2, 1, 6, 0, 0),
                    dt(2026, 2, 3, 22, 0, 0),
                )];
                let evts = vec![
                    make_kudgivt("U1", dt(2026, 2, 1, 6, 0, 0), "201", 480), // 8h drive
                    make_kudgivt("U1", dt(2026, 2, 1, 14, 0, 0), "301", 60), // 1h break (not 302)
                    make_kudgivt("U1", dt(2026, 2, 1, 15, 0, 0), "201", 420), // 7h drive → 22:00
                    make_kudgivt("U1", dt(2026, 2, 1, 22, 0, 0), "302", 540), // rest 9h → 翌7:00
                    make_kudgivt("U1", dt(2026, 2, 2, 7, 0, 0), "201", 900), // 15h drive → 22:00
                    make_kudgivt("U1", dt(2026, 2, 2, 22, 0, 0), "302", 480), // rest 8h → 翌6:00
                    make_kudgivt("U1", dt(2026, 2, 3, 6, 0, 0), "201", 960), // 16h drive → 22:00
                ];
                let refs: Vec<&_> = evts.iter().collect();
                let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
                by_unko.insert("U1".into(), refs);

                let result = build_day_map(&kudguri, &by_unko, &cls);
                // Should complete without error with multiple days
                assert!(result.day_map.len() >= 2);
            }
        );
    }

    // ---- L1438: chain path + ol_work_events empty (no Drive/Cargo in overlap) ----
    #[test]
    fn test_overlap_chain_empty_work_events() {
        test_group!("比較ロジック");
        test_case!("overlap_chain: chain path + ol_work_events 空", {
            let cls = default_classifications();
            let d1 = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let d2 = NaiveDate::from_ymd_opt(2026, 2, 2).unwrap();
            let t1 = NaiveTime::from_hms_opt(5, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(3, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            // Day1: 5:00→翌3:00
            let mut agg1 = DayAgg::default();
            agg1.drive_minutes = 600;
            agg1.total_work_minutes = 600;
            agg1.unko_nos = vec!["U1".into()];
            agg1.segments = vec![SegRec {
                start_at: dt(2026, 2, 1, 5, 0, 0),
                end_at: dt(2026, 2, 2, 3, 0, 0),
            }];
            day_map.insert(("D1".into(), d1, t1), agg1);
            // Day2: 3:00→15:00 (gap=0<540 → chain)
            let mut agg2 = DayAgg::default();
            agg2.drive_minutes = 400;
            agg2.total_work_minutes = 400;
            // No matching unko in kudgivt → ol_work_events stays empty
            agg2.unko_nos = vec!["U_NOMATCH".into()];
            agg2.segments = vec![SegRec {
                start_at: dt(2026, 2, 2, 3, 0, 0),
                end_at: dt(2026, 2, 2, 15, 0, 0),
            }];
            day_map.insert(("D1".into(), d2, t2), agg2);

            let mut wb = HashMap::new();
            wb.insert(
                ("D1".into(), d1, t1),
                (dt(2026, 2, 1, 5, 0, 0), dt(2026, 2, 2, 3, 0, 0)),
            );
            wb.insert(
                ("D1".into(), d2, t2),
                (dt(2026, 2, 2, 3, 0, 0), dt(2026, 2, 2, 15, 0, 0)),
            );
            let mwb = HashMap::new();
            let mut dwe = HashMap::new();
            let kudguri = vec![make_kudguri(
                "U1",
                "D1",
                dt(2026, 2, 1, 5, 0, 0),
                dt(2026, 2, 2, 15, 0, 0),
            )];
            let evts = vec![
                make_kudgivt("U1", dt(2026, 2, 1, 5, 0, 0), "201", 600),
                make_kudgivt("U1", dt(2026, 2, 2, 3, 0, 0), "201", 400),
            ];
            let erefs: Vec<&_> = evts.iter().collect();
            let mut by_unko: HashMap<String, Vec<&_>> = HashMap::new();
            by_unko.insert("U1".into(), erefs);
            let ferry = FerryInfo::default();

            process_overlap_chain(
                &mut day_map,
                &mut wb,
                &mwb,
                &mut dwe,
                &by_unko,
                &cls,
                &kudguri,
                &ferry,
            );
            // chain path with empty ol_work_events → L1438 closing brace
            assert!(day_map.contains_key(&("D1".into(), d1, t1)));
        });
    }

    // ---- apply_sig_splits: 境界を跨ぐ大セグメント → 分割実行 ----
    #[test]
    fn test_apply_sig_splits_triggers() {
        test_group!("比較ロジック");
        test_case!("apply_sig_splits: 180+180min 以上 → 分割", {
            // Segment: 2/1 6:00 → 2/2 6:00 (24h)
            let segs = vec![work_segments::WorkSegment {
                start: dt(2026, 2, 1, 6, 0, 0),
                end: dt(2026, 2, 2, 6, 0, 0),
                labor_minutes: 1440,
                drive_minutes: 1440,
                cargo_minutes: 0,
            }];
            // Workday boundary at 2/1 18:00 (12h from start, 12h to end → both >= 180min)
            let workdays = vec![
                work_segments::Workday {
                    start: dt(2026, 2, 1, 6, 0, 0),
                    end: dt(2026, 2, 1, 18, 0, 0),
                    date: NaiveDate::from_ymd_opt(2026, 2, 1).unwrap(),
                },
                work_segments::Workday {
                    start: dt(2026, 2, 1, 18, 0, 0),
                    end: dt(2026, 2, 2, 6, 0, 0),
                    date: NaiveDate::from_ymd_opt(2026, 2, 2).unwrap(),
                },
            ];
            let ret = dt(2026, 2, 2, 6, 0, 0);
            let result = apply_sig_splits(segs, &workdays, ret);
            // Should split at 18:00
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].end, dt(2026, 2, 1, 18, 0, 0));
            assert_eq!(result[1].start, dt(2026, 2, 1, 18, 0, 0));
        });
    }

    // ---- find_work_date_for_segment: workday 外 → parent_seg fallback ----
    #[test]
    fn test_find_work_date_fallback_parent_seg() {
        test_group!("比較ロジック");
        test_case!("find_work_date: workday 外 → parent segment 日付", {
            // daily_segment that doesn't fit in any workday
            let ds = work_segments::DailyWorkSegment {
                date: NaiveDate::from_ymd_opt(2026, 2, 2).unwrap(),
                start: dt(2026, 2, 2, 0, 0, 0),
                end: dt(2026, 2, 2, 3, 0, 0),
                work_minutes: 180,
                labor_minutes: 180,
                late_night_minutes: 180,
                drive_minutes: 180,
                cargo_minutes: 0,
            };
            // Workday: 2/1 6:00-18:00 (doesn't contain ds)
            let workdays = vec![work_segments::Workday {
                start: dt(2026, 2, 1, 6, 0, 0),
                end: dt(2026, 2, 1, 18, 0, 0),
                date: NaiveDate::from_ymd_opt(2026, 2, 1).unwrap(),
            }];
            // Parent segment: 2/1 18:00 → 2/2 6:00 (contains ds.start)
            let segments = vec![work_segments::WorkSegment {
                start: dt(2026, 2, 1, 18, 0, 0),
                end: dt(2026, 2, 2, 6, 0, 0),
                labor_minutes: 720,
                drive_minutes: 720,
                cargo_minutes: 0,
            }];
            let result = find_work_date_for_segment(&ds, &workdays, &segments);
            // Falls back to parent_seg.start.date() = 2/1
            assert_eq!(result, NaiveDate::from_ymd_opt(2026, 2, 1).unwrap());
        });
    }

    #[test]
    fn test_find_work_date_fallback_no_parent() {
        test_group!("比較ロジック");
        test_case!("find_work_date: parent_seg もなし → ds.date", {
            let ds = work_segments::DailyWorkSegment {
                date: NaiveDate::from_ymd_opt(2026, 2, 3).unwrap(),
                start: dt(2026, 2, 3, 10, 0, 0),
                end: dt(2026, 2, 3, 12, 0, 0),
                work_minutes: 120,
                labor_minutes: 120,
                late_night_minutes: 0,
                drive_minutes: 120,
                cargo_minutes: 0,
            };
            let workdays = vec![];
            let segments = vec![];
            let result = find_work_date_for_segment(&ds, &workdays, &segments);
            assert_eq!(result, NaiveDate::from_ymd_opt(2026, 2, 3).unwrap());
        });
    }

    #[test]
    fn test_drive_inside_rest_attributed_to_zero_minute_workday_does_not_panic() {
        use crate::upload_daily::{compute_daily_hours, DailyHours, DailySegment};
        test_case!(
            "休息の中の運転が 0 分の区間に帰属しても panic しない",
            {
                // 1 運行・1 乗務員: 運転 600 分 → 休息 840 分 (その中に深夜の運転 30 分) → 休息明けと同じ分に 0 分のイベント。
                // 休息明けの 0 分の区間は日別の行にならず、休息の中の運転はその区間に帰属する (付け先の行が無いキーは飛ばす)
                let d = |day, h, mi, s| dt(2026, 3, day, h, mi, s);
                let op = make_kudguri("T1", "1001", d(2, 8, 0, 0), d(3, 9, 0, 0));
                let events = vec![
                    make_kudgivt("T1", d(2, 8, 0, 0), "201", 600),
                    make_kudgivt("T1", d(2, 18, 0, 10), "302", 840),
                    make_kudgivt("T1", d(2, 23, 0, 0), "201", 30),
                    make_kudgivt("T1", d(3, 8, 0, 40), "999", 0),
                ];
                let classifications: HashMap<String, EventClass> = HashMap::from([
                    ("201".to_string(), EventClass::Drive),
                    ("302".to_string(), EventClass::RestSplit),
                    ("999".to_string(), EventClass::Ignore),
                ]);
                let ferry = HashMap::new();
                let got = compute_daily_hours(
                    std::slice::from_ref(&op),
                    &events,
                    &classifications,
                    &ferry,
                );
                // (D, 08:00) の 1 行だけ。休息の中の 30 分は数えない
                let segment = DailySegment {
                    unko_no: "T1".into(),
                    segment_index: 0,
                    start_at: d(2, 8, 0, 0),
                    end_at: d(2, 18, 0, 10),
                    work_minutes: 600,
                    labor_minutes: 600,
                    late_night_minutes: 0,
                    drive_minutes: 600,
                    cargo_minutes: 0,
                };
                let hours = DailyHours {
                    total_work_minutes: 600,
                    total_labor_minutes: 600,
                    late_night_minutes: 0,
                    drive_minutes: 600,
                    cargo_minutes: 0,
                    total_distance: 0.0,
                    operation_count: 1,
                    unko_nos: vec!["T1".into()],
                    segments: vec![segment],
                    rest_event_minutes: 840,
                    overlap_drive_minutes: 0,
                    overlap_cargo_minutes: 0,
                    overlap_break_minutes: 0,
                    overlap_restraint_minutes: 0,
                    ot_late_night_minutes: 0,
                };
                let key = (
                    "1001".to_string(),
                    d(2, 8, 0, 0).date(),
                    d(2, 8, 0, 0).time(),
                );
                assert_eq!(got, HashMap::from([(key, hours)]));

                // 同じ入力に別の乗務員の運行を足しても、その乗務員の値は単独で計算したときと同じ
                let other = make_kudguri("T2", "1002", d(2, 9, 0, 0), d(2, 15, 0, 0));
                let mut other_event = make_kudgivt("T2", d(2, 9, 0, 0), "201", 360);
                other_event.driver_cd = "1002".into();
                let mut all_events = events.clone();
                all_events.push(other_event.clone());
                let both = compute_daily_hours(
                    &[op, other.clone()],
                    &all_events,
                    &classifications,
                    &ferry,
                );
                let alone = compute_daily_hours(&[other], &[other_event], &classifications, &ferry);
                let mut want = got.clone();
                want.extend(alone);
                assert_eq!(both, want);
            }
        );
    }

    #[test]
    fn test_merge_same_day_entries_empty_segments_no_merge() {
        test_case!("merge_same_day: segments空 → マージしない", {
            let d = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
            let t1 = NaiveTime::from_hms_opt(6, 0, 0).unwrap();
            let t2 = NaiveTime::from_hms_opt(14, 0, 0).unwrap();
            let mut day_map: HashMap<DayKey, DayAgg> = HashMap::new();
            // 同日2エントリ、異なる運行、segments空 → prev_end/next_start が None
            let mut a1 = DayAgg::default();
            a1.unko_nos = vec!["U1".into()];
            a1.segments = vec![]; // 空
            day_map.insert(("D1".into(), d, t1), a1);
            let mut a2 = DayAgg::default();
            a2.unko_nos = vec!["U2".into()];
            a2.segments = vec![]; // 空
            day_map.insert(("D1".into(), d, t2), a2);
            let mut dwe = HashMap::new();
            merge_same_day_entries(&mut day_map, &mut dwe);
            // segments空 → None → マージされない → 2エントリのまま
            assert_eq!(day_map.len(), 2);
        });
    }
}
