use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

/// KUDGURI.csv の1行をパースした結果
#[derive(Debug, Clone)]
pub struct KudguriRow {
    pub unko_no: String,
    pub reading_date: NaiveDate,
    pub operation_date: Option<NaiveDate>,
    pub office_cd: String,
    pub office_name: String,
    pub vehicle_cd: String,
    pub vehicle_name: String,
    pub driver_cd: String,
    pub driver_name: String,
    pub crew_role: i32,
    pub departure_at: Option<NaiveDateTime>,
    pub return_at: Option<NaiveDateTime>,
    pub garage_out_at: Option<NaiveDateTime>,
    pub garage_in_at: Option<NaiveDateTime>,
    pub meter_start: Option<f64>,
    pub meter_end: Option<f64>,
    pub total_distance: Option<f64>,
    pub drive_time_general: Option<i32>,
    pub drive_time_highway: Option<i32>,
    pub drive_time_bypass: Option<i32>,
    pub safety_score: Option<f64>,
    pub economy_score: Option<f64>,
    pub total_score: Option<f64>,
    pub raw_data: serde_json::Value,
}

/// KUDGURI.csv ヘッダー名とカラムインデックスのマッピング
struct ColumnIndex {
    unko_no: usize,
    reading_date: usize,
    operation_date: Option<usize>,
    office_cd: usize,
    office_name: usize,
    vehicle_cd: usize,
    vehicle_name: usize,
    driver_cd: usize,
    driver_name: usize,
    crew_role: usize,
    departure_at: Option<usize>,
    return_at: Option<usize>,
    garage_out_at: Option<usize>,
    garage_in_at: Option<usize>,
    meter_start: Option<usize>,
    meter_end: Option<usize>,
    total_distance: Option<usize>,
    drive_time_general: Option<usize>,
    drive_time_highway: Option<usize>,
    drive_time_bypass: Option<usize>,
    safety_score: Option<usize>,
    economy_score: Option<usize>,
    total_score: Option<usize>,
}

fn find_col(headers: &[&str], name: &str) -> Option<usize> {
    headers.iter().position(|h| h.trim() == name)
}

fn require_col<'a>(headers: &[&str], name: &'a str, missing: &mut Vec<&'a str>) -> Option<usize> {
    let idx = find_col(headers, name);
    if idx.is_none() {
        missing.push(name);
    }
    idx
}

fn build_column_index(headers: &[&str]) -> Result<ColumnIndex, String> {
    let mut missing = Vec::new();

    let unko_no = require_col(headers, "運行NO", &mut missing);
    let reading_date = require_col(headers, "読取日", &mut missing);
    let office_cd = require_col(headers, "事業所CD", &mut missing);
    let office_name = require_col(headers, "事業所名", &mut missing);
    let vehicle_cd = require_col(headers, "車輌CD", &mut missing);
    let vehicle_name = require_col(headers, "車輌名", &mut missing);
    // 「乗務員CD1」は運行 primary driver で、KUDGURI の全行で同じ値になる。
    // 行ごとの「対象」は「対象乗務員CD」 (subject driver of this row) で、これを
    // 採用しないと crew_role=2 副運転手の行も primary driver として登録されてしまう。
    // 対象乗務員CD/名 が無い古い CSV (1人乗務) は乗務員CD1 にフォールバック。
    let driver_cd = find_col(headers, "対象乗務員CD").or_else(|| find_col(headers, "乗務員CD1"));
    if driver_cd.is_none() {
        missing.push("対象乗務員CD or 乗務員CD1");
    }
    let driver_name = find_col(headers, "対象乗務員名").or_else(|| find_col(headers, "乗務員名１"));
    if driver_name.is_none() {
        missing.push("対象乗務員名 or 乗務員名１");
    }
    let crew_role = require_col(headers, "対象乗務員区分", &mut missing);

    if !missing.is_empty() {
        return Err(format!("missing required columns: {}", missing.join(", ")));
    }

    Ok(ColumnIndex {
        unko_no: unko_no.unwrap(),
        reading_date: reading_date.unwrap(),
        operation_date: find_col(headers, "運行日"),
        office_cd: office_cd.unwrap(),
        office_name: office_name.unwrap(),
        vehicle_cd: vehicle_cd.unwrap(),
        vehicle_name: vehicle_name.unwrap(),
        driver_cd: driver_cd.unwrap(),
        driver_name: driver_name.unwrap(),
        crew_role: crew_role.unwrap(),
        departure_at: find_col(headers, "出社日時"),
        return_at: find_col(headers, "退社日時"),
        garage_out_at: find_col(headers, "出庫日時"),
        garage_in_at: find_col(headers, "帰庫日時"),
        meter_start: find_col(headers, "出庫メーター"),
        meter_end: find_col(headers, "帰庫メーター"),
        total_distance: find_col(headers, "総走行距離"),
        drive_time_general: find_col(headers, "一般道運転時間"),
        drive_time_highway: find_col(headers, "高速道運転時間"),
        drive_time_bypass: find_col(headers, "バイパス運転時間"),
        safety_score: find_col(headers, "安全評価点"),
        economy_score: find_col(headers, "経済評価点"),
        total_score: find_col(headers, "総合評価点"),
    })
}

fn get_field<'a>(fields: &'a [&str], idx: usize) -> &'a str {
    fields.get(idx).map(|s| s.trim()).unwrap_or("")
}

fn get_opt_field<'a>(fields: &'a [&str], idx: Option<usize>) -> Option<&'a str> {
    idx.and_then(|i| fields.get(i).map(|s| s.trim()))
        .filter(|s| !s.is_empty())
}

fn parse_date(s: &str) -> Option<NaiveDate> {
    // "2026/02/27 00:00:00" or "2026/02/27 0:00:00" or "2026/02/27"
    let date_part = s.split_whitespace().next().unwrap_or(s);
    NaiveDate::parse_from_str(date_part, "%Y/%m/%d").ok()
}

fn parse_datetime(s: &str) -> Option<NaiveDateTime> {
    // "2026/02/24 10:13:11"
    NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y/%m/%d %k:%M:%S"))
        .ok()
}

fn parse_f64(s: &str) -> Option<f64> {
    s.parse::<f64>().ok()
}

fn parse_i32(s: &str) -> Option<i32> {
    s.parse::<i32>().ok()
}

/// KUDGURI.csv テキスト全体をパースして KudguriRow のリストを返す
pub fn parse_kudguri(csv_text: &str) -> Result<Vec<KudguriRow>, anyhow::Error> {
    let mut lines = csv_text.lines();
    let header_line = lines.next().ok_or_else(|| anyhow::anyhow!("empty CSV"))?;
    let headers: Vec<&str> = header_line.split(',').collect();
    let col_idx = build_column_index(&headers).map_err(|e| anyhow::anyhow!(e))?;

    let mut rows = Vec::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').collect();

        // Build raw_data as JSON object with all columns
        let mut raw_map = serde_json::Map::new();
        for (i, header) in headers.iter().enumerate() {
            let val = fields.get(i).map(|s| s.trim()).unwrap_or("");
            raw_map.insert(
                header.trim().to_string(),
                serde_json::Value::String(val.to_string()),
            );
        }

        let unko_no = get_field(&fields, col_idx.unko_no).to_string();
        let reading_date_str = get_field(&fields, col_idx.reading_date);
        let reading_date = parse_date(reading_date_str)
            .ok_or_else(|| anyhow::anyhow!("invalid reading_date: {}", reading_date_str))?;

        let crew_role_str = get_field(&fields, col_idx.crew_role);
        let crew_role = crew_role_str.parse::<i32>().unwrap_or(1);

        rows.push(KudguriRow {
            unko_no,
            reading_date,
            operation_date: get_opt_field(&fields, col_idx.operation_date).and_then(parse_date),
            office_cd: get_field(&fields, col_idx.office_cd).to_string(),
            office_name: get_field(&fields, col_idx.office_name).to_string(),
            vehicle_cd: get_field(&fields, col_idx.vehicle_cd).to_string(),
            vehicle_name: get_field(&fields, col_idx.vehicle_name).to_string(),
            driver_cd: get_field(&fields, col_idx.driver_cd).to_string(),
            driver_name: get_field(&fields, col_idx.driver_name).to_string(),
            crew_role,
            departure_at: get_opt_field(&fields, col_idx.departure_at).and_then(parse_datetime),
            return_at: get_opt_field(&fields, col_idx.return_at).and_then(parse_datetime),
            garage_out_at: get_opt_field(&fields, col_idx.garage_out_at).and_then(parse_datetime),
            garage_in_at: get_opt_field(&fields, col_idx.garage_in_at).and_then(parse_datetime),
            meter_start: get_opt_field(&fields, col_idx.meter_start).and_then(parse_f64),
            meter_end: get_opt_field(&fields, col_idx.meter_end).and_then(parse_f64),
            total_distance: get_opt_field(&fields, col_idx.total_distance).and_then(parse_f64),
            drive_time_general: get_opt_field(&fields, col_idx.drive_time_general)
                .and_then(parse_i32),
            drive_time_highway: get_opt_field(&fields, col_idx.drive_time_highway)
                .and_then(parse_i32),
            drive_time_bypass: get_opt_field(&fields, col_idx.drive_time_bypass)
                .and_then(parse_i32),
            safety_score: get_opt_field(&fields, col_idx.safety_score).and_then(parse_f64),
            economy_score: get_opt_field(&fields, col_idx.economy_score).and_then(parse_f64),
            total_score: get_opt_field(&fields, col_idx.total_score).and_then(parse_f64),
            raw_data: serde_json::Value::Object(raw_map),
        });
    }

    Ok(rows)
}

/// 再計算で、DB に保存された運行 1 行から [`KudguriRow`] を作るための値 (DB の型に依らない素の値)。
///
/// backend の再計算 (`alc-dtako`) と分割 worker (ippoan/alc-dtako-worker) が、同じ [`RecalcOperation::into_kudguri_row`] を呼ぶ。
#[derive(Debug, Clone, PartialEq)]
pub struct RecalcOperation {
    pub unko_no: String,
    pub reading_date: NaiveDate,
    pub operation_date: Option<NaiveDate>,
    pub driver_cd: String,
    pub departure_at: Option<DateTime<Utc>>,
    pub return_at: Option<DateTime<Utc>>,
    pub total_distance: Option<f64>,
    pub drive_time_general: Option<i32>,
    pub drive_time_highway: Option<i32>,
    pub drive_time_bypass: Option<i32>,
}

impl RecalcOperation {
    /// [`KudguriRow`] にする。DB に無い欄は空 (文字列は空・数値は `None`・`crew_role` は 0・`raw_data` は `Null`)。
    /// 日時は UTC の値の壁時計をそのまま使う (`naive_utc`)。
    pub fn into_kudguri_row(self) -> KudguriRow {
        KudguriRow {
            unko_no: self.unko_no,
            reading_date: self.reading_date,
            operation_date: self.operation_date,
            office_cd: String::new(),
            office_name: String::new(),
            vehicle_cd: String::new(),
            vehicle_name: String::new(),
            driver_cd: self.driver_cd,
            driver_name: String::new(),
            crew_role: 0,
            departure_at: self.departure_at.map(|dt| dt.naive_utc()),
            return_at: self.return_at.map(|dt| dt.naive_utc()),
            garage_out_at: None,
            garage_in_at: None,
            meter_start: None,
            meter_end: None,
            total_distance: self.total_distance,
            drive_time_general: self.drive_time_general,
            drive_time_highway: self.drive_time_highway,
            drive_time_bypass: self.drive_time_bypass,
            safety_score: None,
            economy_score: None,
            total_score: None,
            raw_data: serde_json::Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kudguri_sample() {
        test_group!("CSVパーサー");
        test_case!("KUDGURIサンプルパース", {
            let csv = "運行NO,読取日,運行日,事業所CD,事業所名,車輌CD,車輌名,乗務員CD1,乗務員名１,対象乗務員区分,出社日時,退社日時,出庫日時,帰庫日時,出庫メーター,帰庫メーター,総走行距離,一般道運転時間,高速道運転時間,バイパス運転時間,安全評価点,経済評価点,総合評価点\n\
2602241025060000000272,2026/02/27 00:00:00,2026/02/24 0:00:00,1,本社,272,帯広100け272,2,梅津　政弘,1,2026/02/24 10:13:11,2026/02/27 16:00:54,2026/02/24 10:25:06,2026/02/27 15:48:59,449854.1,451990.6,2136.5,108,1687,0,98,99,98";

            let rows = parse_kudguri(csv).unwrap();
            assert_eq!(rows.len(), 1);
            let row = &rows[0];
            assert_eq!(row.unko_no, "2602241025060000000272");
            assert_eq!(
                row.reading_date,
                NaiveDate::from_ymd_opt(2026, 2, 27).unwrap()
            );
            assert_eq!(row.office_cd, "1");
            assert_eq!(row.office_name, "本社");
            assert_eq!(row.vehicle_cd, "272");
            assert_eq!(row.driver_cd, "2");
            assert_eq!(row.driver_name, "梅津　政弘");
            assert_eq!(row.crew_role, 1);
            assert!((row.total_distance.unwrap() - 2136.5).abs() < 0.01);
            assert_eq!(row.drive_time_general, Some(108));
            assert_eq!(row.drive_time_highway, Some(1687));
            assert!((row.total_score.unwrap() - 98.0).abs() < 0.01);
        });
    }

    #[test]
    fn test_parse_kudguri_empty_lines() {
        test_group!("CSVパーサー");
        test_case!("空行を含むKUDGURIパース", {
            let csv = "運行NO,読取日,事業所CD,事業所名,車輌CD,車輌名,乗務員CD1,乗務員名１,対象乗務員区分\n\
                       1001,2026/03/01,OFF01,テスト事業所,VH01,テスト車両,DR01,テスト運転者,1\n\
                       \n\
                       1002,2026/03/02,OFF01,テスト事業所,VH02,テスト車両2,DR02,テスト運転者2,1\n";
            let rows = parse_kudguri(csv).unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].unko_no, "1001");
            assert_eq!(rows[1].unko_no, "1002");
        });
    }

    #[test]
    fn test_missing_columns_error_message() {
        test_group!("CSVパーサー");
        test_case!("必須カラム不足のエラーメッセージ", {
            let csv = "運行NO,読取日\ndata1,data2";
            let err = parse_kudguri(csv).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("missing required columns"), "got: {msg}");
            assert!(msg.contains("事業所CD"), "got: {msg}");
            assert!(msg.contains("乗務員CD1"), "got: {msg}");
            assert!(msg.contains("対象乗務員区分"), "got: {msg}");
            // 存在するカラムは含まれない
            assert!(!msg.contains("運行NO"), "got: {msg}");
            assert!(!msg.contains("読取日"), "got: {msg}");
        });
    }

    #[test]
    fn test_recalc_operation_into_kudguri_row() {
        test_group!("CSVパーサー");
        test_case!(
            "再計算: DB の運行の行から KudguriRow を作る (DB に無い欄は空・日時は UTC の壁時計)",
            {
                let at = |h, m| {
                    NaiveDate::from_ymd_opt(2026, 3, 2)
                        .unwrap()
                        .and_hms_opt(h, m, 0)
                        .unwrap()
                };
                let op = RecalcOperation {
                    unko_no: "T-1".into(),
                    reading_date: NaiveDate::from_ymd_opt(2026, 3, 3).unwrap(),
                    operation_date: NaiveDate::from_ymd_opt(2026, 3, 2),
                    driver_cd: "D-ONE".into(),
                    departure_at: Some(at(8, 15).and_utc()),
                    return_at: Some(at(17, 45).and_utc()),
                    total_distance: Some(123.5),
                    drive_time_general: Some(100),
                    drive_time_highway: Some(200),
                    drive_time_bypass: Some(30),
                };
                let row = op.into_kudguri_row();
                assert_eq!(row.unko_no, "T-1");
                assert_eq!(
                    row.reading_date,
                    NaiveDate::from_ymd_opt(2026, 3, 3).unwrap()
                );
                assert_eq!(row.operation_date, NaiveDate::from_ymd_opt(2026, 3, 2));
                assert_eq!(row.driver_cd, "D-ONE");
                assert_eq!(row.departure_at, Some(at(8, 15)));
                assert_eq!(row.return_at, Some(at(17, 45)));
                assert_eq!(row.total_distance, Some(123.5));
                assert_eq!(
                    (
                        row.drive_time_general,
                        row.drive_time_highway,
                        row.drive_time_bypass
                    ),
                    (Some(100), Some(200), Some(30))
                );
                let names = [
                    &row.office_cd,
                    &row.office_name,
                    &row.vehicle_cd,
                    &row.vehicle_name,
                    &row.driver_name,
                ];
                assert!(names.iter().all(|s| s.is_empty()));
                assert_eq!(
                    (row.crew_role, row.garage_out_at, row.garage_in_at),
                    (0, None, None)
                );
                assert_eq!((row.meter_start, row.meter_end), (None, None));
                assert_eq!(
                    (row.safety_score, row.economy_score, row.total_score),
                    (None, None, None)
                );
                assert_eq!(row.raw_data, serde_json::Value::Null);
            }
        );
        test_case!(
            "再計算: 乗務員CD が空・日時と数値が無い行",
            {
                let op = RecalcOperation {
                    unko_no: "T-2".into(),
                    reading_date: NaiveDate::from_ymd_opt(2026, 3, 3).unwrap(),
                    operation_date: None,
                    driver_cd: String::new(),
                    departure_at: None,
                    return_at: None,
                    total_distance: None,
                    drive_time_general: None,
                    drive_time_highway: None,
                    drive_time_bypass: None,
                };
                let row = op.clone().into_kudguri_row();
                assert_eq!((row.driver_cd.as_str(), row.operation_date), ("", None));
                assert_eq!(
                    (row.departure_at, row.return_at, row.total_distance),
                    (None, None, None)
                );
                assert_eq!(
                    (
                        row.drive_time_general,
                        row.drive_time_highway,
                        row.drive_time_bypass
                    ),
                    (None, None, None)
                );
                assert_eq!(format!("{op:?}").len(), format!("{:?}", op.clone()).len());
                assert_eq!(op.clone(), op);
            }
        );
    }
}
