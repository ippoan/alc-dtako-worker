use chrono::{NaiveDate, NaiveDateTime};

/// KUDGIVT.csv の1行をパースした結果
#[derive(Debug, Clone)]
pub struct KudgivtRow {
    pub unko_no: String,
    pub reading_date: NaiveDate,
    pub driver_cd: String,
    pub driver_name: String,
    pub crew_role: i32,
    pub start_at: NaiveDateTime,
    pub end_at: Option<NaiveDateTime>,
    pub event_cd: String,
    pub event_name: String,
    pub duration_minutes: Option<i32>,
    pub section_distance: Option<f64>,
}

struct ColumnIndex {
    unko_no: usize,
    reading_date: usize,
    driver_cd: usize,
    driver_name: usize,
    crew_role: usize,
    start_at: usize,
    end_at: Option<usize>,
    event_cd: usize,
    event_name: usize,
    duration_minutes: Option<usize>,
    section_distance: Option<usize>,
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
    // 「乗務員CD1」は運行 primary driver で、KUDGIVT の全行で同じ値になる。
    // 行ごとの「対象」は「対象乗務員CD」 (subject driver of this row) で、これを
    // 採用しないと crew_role=2 副運転手の行も primary driver のイベントとして
    // 集計されてしまう (KUDGURI 側は既に対象乗務員CD を採用しているため、
    // dtako_daily_work_hours の休息集計でキーが食い違う)。
    // 対象乗務員CD が無い古い CSV (1人乗務) は乗務員CD1 にフォールバック。
    let driver_cd = find_col(headers, "対象乗務員CD").or_else(|| find_col(headers, "乗務員CD1"));
    if driver_cd.is_none() {
        missing.push("対象乗務員CD or 乗務員CD1");
    }
    let driver_name = require_col(headers, "乗務員名１", &mut missing);
    let crew_role = require_col(headers, "対象乗務員区分", &mut missing);
    let start_at = require_col(headers, "開始日時", &mut missing);
    let event_cd = require_col(headers, "イベントCD", &mut missing);
    let event_name = require_col(headers, "イベント名", &mut missing);

    if !missing.is_empty() {
        return Err(format!("missing required columns: {}", missing.join(", ")));
    }

    Ok(ColumnIndex {
        unko_no: unko_no.unwrap(),
        reading_date: reading_date.unwrap(),
        driver_cd: driver_cd.unwrap(),
        driver_name: driver_name.unwrap(),
        crew_role: crew_role.unwrap(),
        start_at: start_at.unwrap(),
        end_at: find_col(headers, "終了日時"),
        event_cd: event_cd.unwrap(),
        event_name: event_name.unwrap(),
        duration_minutes: find_col(headers, "区間時間"),
        section_distance: find_col(headers, "区間距離"),
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
    let date_part = s.split_whitespace().next().unwrap_or(s);
    NaiveDate::parse_from_str(date_part, "%Y/%m/%d").ok()
}

fn parse_datetime(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y/%m/%d %k:%M:%S"))
        .ok()
}

fn parse_i32(s: &str) -> Option<i32> {
    s.parse::<i32>().ok()
}

fn parse_f64(s: &str) -> Option<f64> {
    s.parse::<f64>().ok()
}

/// KUDGIVT.csv テキスト全体をパースして KudgivtRow のリストを返す
pub fn parse_kudgivt(csv_text: &str) -> Result<Vec<KudgivtRow>, anyhow::Error> {
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

        let unko_no = get_field(&fields, col_idx.unko_no).to_string();
        let reading_date_str = get_field(&fields, col_idx.reading_date);
        let reading_date = parse_date(reading_date_str)
            .ok_or_else(|| anyhow::anyhow!("invalid reading_date: {}", reading_date_str))?;

        let start_at_str = get_field(&fields, col_idx.start_at);
        let start_at = match parse_datetime(start_at_str) {
            Some(dt) => dt,
            None => continue, // skip rows with invalid datetime
        };

        let crew_role_str = get_field(&fields, col_idx.crew_role);
        let crew_role = crew_role_str.parse::<i32>().unwrap_or(1);

        rows.push(KudgivtRow {
            unko_no,
            reading_date,
            driver_cd: get_field(&fields, col_idx.driver_cd).to_string(),
            driver_name: get_field(&fields, col_idx.driver_name).to_string(),
            crew_role,
            start_at,
            end_at: get_opt_field(&fields, col_idx.end_at).and_then(parse_datetime),
            event_cd: get_field(&fields, col_idx.event_cd).to_string(),
            event_name: get_field(&fields, col_idx.event_name).to_string(),
            duration_minutes: get_opt_field(&fields, col_idx.duration_minutes).and_then(parse_i32),
            section_distance: get_opt_field(&fields, col_idx.section_distance).and_then(parse_f64),
        });
    }

    Ok(rows)
}

/// 保存先に置かれた運行 1 件ぶんの KUDGIVT.csv のバイト列を読み、`crew_role` の行だけを返す。
///
/// 分割で置かれる CSV は UTF-8。古い Shift_JIS のままのデータも読めるよう
/// [`crate::decode_utf8_or_shift_jis`] を通す。エラーは [`parse_kudgivt`] のもの。
pub fn parse_kudgivt_for_crew(
    bytes: &[u8],
    crew_role: i32,
) -> Result<Vec<KudgivtRow>, anyhow::Error> {
    let text = crate::decode_utf8_or_shift_jis(bytes);
    let rows = parse_kudgivt(&text)?;
    Ok(rows
        .into_iter()
        .filter(|r| r.crew_role == crew_role)
        .collect())
}

/// 同じ (運行NO, イベントCD, 開始日時) の行を 1 つにする (最初に出た行を残す。残る行の順はそのまま)。
/// 再計算で、同じ KUDGIVT が複数の zip に入っているときに使う。
pub fn dedup_kudgivt_rows(mut rows: Vec<KudgivtRow>) -> Vec<KudgivtRow> {
    let mut seen = std::collections::HashSet::new();
    rows.retain(|row| seen.insert((row.unko_no.clone(), row.event_cd.clone(), row.start_at)));
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kudgivt_sample() {
        test_group!("CSVパーサー");
        test_case!("KUDGIVTサンプルパース", {
            let csv = "運行NO,読取日,事業所CD,事業所名,車輌CD,車輌名,乗務員CD1,乗務員名１,対象乗務員区分,開始日時,イベントCD,イベント名,開始走行距離,終了走行距離,区間時間,区間距離,開始市町村CD,開始市町村名,終了市町村CD,終了市町村名,開始場所CD,開始場所名,終了場所CD,終了場所名\n\
2602241025060000000272,2026/02/27 00:00:00,1,本社,272,帯広100け272,2,梅津　政弘,1,2026/02/24 14:40:56,302,休息,248.9,250.1,1123,1.2,1203,小樽市築港,1203,小樽市築港,,,,";

            let rows = parse_kudgivt(csv).unwrap();
            assert_eq!(rows.len(), 1);
            let row = &rows[0];
            assert_eq!(row.unko_no, "2602241025060000000272");
            assert_eq!(row.event_cd, "302");
            assert_eq!(row.event_name, "休息");
            assert_eq!(row.duration_minutes, Some(1123));
            assert!((row.section_distance.unwrap() - 1.2).abs() < 0.01);
            assert_eq!(row.driver_cd, "2");
        });
    }

    #[test]
    fn test_parse_kudgivt_prefers_taisho_driver_cd() {
        test_group!("CSVパーサー");
        test_case!("対象乗務員CDを乗務員CD1より優先", {
            // 乗務員CD1 は運行の primary driver (全行同じ)、対象乗務員CD が行ごとの対象。
            // crew_role=2 の副運転手の行は対象乗務員CD 側を採らないと primary に寄ってしまう。
            let csv = "運行NO,読取日,乗務員CD1,乗務員名１,対象乗務員CD,対象乗務員区分,開始日時,イベントCD,イベント名\n\
                       1001,2026/03/01,2,梅津　政弘,2,1,2026/03/01 08:00:00,100,出庫\n\
                       1001,2026/03/01,2,梅津　政弘,77,2,2026/03/01 09:00:00,302,休息\n";
            let rows = parse_kudgivt(csv).unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].driver_cd, "2");
            assert_eq!(rows[0].crew_role, 1);
            // 副運転手の行は対象乗務員CD が採用される (乗務員CD1 の "2" ではない)
            assert_eq!(rows[1].driver_cd, "77");
            assert_eq!(rows[1].crew_role, 2);
        });
    }

    #[test]
    fn test_parse_kudgivt_falls_back_to_driver_cd1() {
        test_group!("CSVパーサー");
        test_case!(
            "対象乗務員CD が無ければ乗務員CD1にフォールバック",
            {
                // 対象乗務員CD を持たない古い CSV (1人乗務)
                let csv = "運行NO,読取日,乗務員CD1,乗務員名１,対象乗務員区分,開始日時,イベントCD,イベント名\n\
                       1001,2026/03/01,DR01,テスト運転者,1,2026/03/01 08:00:00,100,出庫\n";
                let rows = parse_kudgivt(csv).unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].driver_cd, "DR01");
            }
        );
    }

    #[test]
    fn test_parse_kudgivt_empty_lines() {
        test_group!("CSVパーサー");
        test_case!("空行を含むKUDGIVTパース", {
            let csv = "運行NO,読取日,乗務員CD1,乗務員名１,対象乗務員区分,開始日時,イベントCD,イベント名\n\
                       1001,2026/03/01,DR01,テスト運転者,1,2026/03/01 08:00:00,100,出庫\n\
                       \n\
                       1002,2026/03/01,DR02,テスト運転者2,1,2026/03/01 09:00:00,200,運転\n";
            let rows = parse_kudgivt(csv).unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].unko_no, "1001");
            assert_eq!(rows[1].unko_no, "1002");
        });
    }

    #[test]
    fn test_parse_kudgivt_invalid_datetime() {
        test_group!("CSVパーサー");
        test_case!("不正な日時の行をスキップ", {
            let csv = "運行NO,読取日,乗務員CD1,乗務員名１,対象乗務員区分,開始日時,イベントCD,イベント名\n\
                       1001,2026/03/01,DR01,テスト運転者,1,INVALID_DATE,100,出庫\n\
                       1002,2026/03/01,DR02,テスト運転者2,1,2026/03/01 09:00:00,200,運転\n";
            let rows = parse_kudgivt(csv).unwrap();
            assert_eq!(rows.len(), 1, "invalid datetime row should be skipped");
            assert_eq!(rows[0].unko_no, "1002");
        });
    }

    #[test]
    fn test_missing_columns_error_message() {
        test_group!("CSVパーサー");
        test_case!("必須カラム不足のエラーメッセージ", {
            let csv = "運行NO,読取日\ndata1,data2";
            let err = parse_kudgivt(csv).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("missing required columns"), "got: {msg}");
            assert!(msg.contains("乗務員CD1"), "got: {msg}");
            assert!(msg.contains("イベントCD"), "got: {msg}");
            assert!(!msg.contains("運行NO"), "got: {msg}");
        });
    }

    /// 乗務員 2 人 (対象乗務員区分 1 と 2) の行を持つ CSV
    const TWO_CREW_CSV: &str = "運行NO,読取日,乗務員CD1,乗務員名１,対象乗務員CD,対象乗務員区分,開始日時,イベントCD,イベント名\n\
        1001,2026/03/01,2,テスト運転者,2,1,2026/03/01 08:00:00,201,運転\n\
        1001,2026/03/01,2,テスト運転者,77,2,2026/03/01 09:00:00,302,休息\n\
        1001,2026/03/01,2,テスト運転者,2,1,2026/03/01 10:00:00,301,休憩\n";

    #[test]
    fn test_parse_kudgivt_for_crew_utf8() {
        test_group!("CSVパーサー");
        test_case!(
            "UTF-8 のバイト列を読み、crew_role の行だけを返す",
            {
                let rows = parse_kudgivt_for_crew(TWO_CREW_CSV.as_bytes(), 1).unwrap();
                let cds: Vec<&str> = rows.iter().map(|r| r.event_cd.as_str()).collect();
                assert_eq!(cds, ["201", "301"]);
                assert!(rows.iter().all(|r| r.crew_role == 1 && r.driver_cd == "2"));

                let rows = parse_kudgivt_for_crew(TWO_CREW_CSV.as_bytes(), 2).unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    (rows[0].event_cd.as_str(), rows[0].driver_cd.as_str()),
                    ("302", "77")
                );

                // 該当の crew_role が居なければ空
                assert!(parse_kudgivt_for_crew(TWO_CREW_CSV.as_bytes(), 3)
                    .unwrap()
                    .is_empty());
            }
        );
    }

    #[test]
    fn test_parse_kudgivt_for_crew_shift_jis() {
        test_group!("CSVパーサー");
        test_case!(
            "Shift_JIS のバイト列 (古いデータ) も同じ結果になる",
            {
                let sjis = encoding_rs::SHIFT_JIS.encode(TWO_CREW_CSV).0.to_vec();
                assert!(std::str::from_utf8(&sjis).is_err());
                let rows = parse_kudgivt_for_crew(&sjis, 1).unwrap();
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].driver_name, "テスト運転者");
                assert_eq!(rows[1].event_name, "休憩");
            }
        );
    }

    #[test]
    fn test_parse_kudgivt_for_crew_parse_error() {
        test_group!("CSVパーサー");
        test_case!(
            "parse の失敗は parse_kudgivt のエラーのまま返る",
            {
                let err =
                    parse_kudgivt_for_crew("運行NO,読取日\ndata1,data2".as_bytes(), 1).unwrap_err();
                assert!(err.to_string().contains("missing required columns"));
                let err = parse_kudgivt_for_crew(b"", 1).unwrap_err();
                assert_eq!(err.to_string(), "empty CSV");
            }
        );
    }

    #[test]
    fn test_dedup_kudgivt_rows() {
        test_group!("CSVパーサー");
        let date = NaiveDate::from_ymd_opt(2026, 3, 2).unwrap();
        let row = |unko: &str, cd: &str, hour: u32, name: &str| KudgivtRow {
            unko_no: unko.into(),
            reading_date: date,
            driver_cd: "D-ONE".into(),
            driver_name: name.into(),
            crew_role: 1,
            start_at: date.and_hms_opt(hour, 0, 0).unwrap(),
            end_at: None,
            event_cd: cd.into(),
            event_name: String::new(),
            duration_minutes: None,
            section_distance: None,
        };
        let names = |rows: &[KudgivtRow]| {
            rows.iter()
                .map(|r| r.driver_name.clone())
                .collect::<Vec<_>>()
        };
        test_case!("KUDGIVT の重複: (運行NO, イベントCD, 開始日時) が同じ行は最初の 1 つだけ残り、順はそのまま", {
            let rows = vec![
                row("T-1", "201", 8, "a"),
                row("T-1", "202", 8, "b"),
                row("T-1", "201", 8, "c"),
                row("T-2", "201", 8, "d"),
                row("T-1", "201", 9, "e"),
                row("T-2", "201", 8, "f"),
            ];
            assert_eq!(names(&dedup_kudgivt_rows(rows)), ["a", "b", "d", "e"]);
        });
        test_case!(
            "KUDGIVT の重複: 重複が無ければそのまま・空は空",
            {
                let rows = vec![row("T-1", "201", 8, "a"), row("T-1", "201", 9, "b")];
                assert_eq!(names(&dedup_kudgivt_rows(rows)), ["a", "b"]);
                assert!(dedup_kudgivt_rows(Vec::new()).is_empty());
            }
        );
    }
}
