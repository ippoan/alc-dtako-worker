#[cfg(test)]
#[macro_use]
mod test_macros;

pub mod kudgivt;
pub mod kudguri;
pub mod operation_changes;
pub mod work_segments;

#[cfg(feature = "zip-extract")]
use std::io::Read;

/// ZIP バイト列を展開し、(ファイル名, バイト列) のリストを返す
#[cfg(feature = "zip-extract")]
pub fn extract_zip(bytes: &[u8]) -> Result<Vec<(String, Vec<u8>)>, anyhow::Error> {
    let cursor = std::io::Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(cursor)?;
    let mut files = Vec::new();
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let name = file.name().to_string();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)?;
        files.push((name, contents));
    }
    Ok(files)
}

/// Shift-JIS バイト列を UTF-8 文字列に変換
pub fn decode_shift_jis(bytes: &[u8]) -> String {
    let (decoded, _, _) = encoding_rs::SHIFT_JIS.decode(bytes);
    decoded.into_owned()
}

/// UTF-8 として読めればそのまま、読めなければ Shift_JIS として decode する。
///
/// 分割で保存先に置く運行ごとの CSV は UTF-8 (分割のときに [`decode_shift_jis`] を通す)。
/// 古い Shift_JIS のままのデータも読めるようにするための fallback。
pub fn decode_utf8_or_shift_jis(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_owned(),
        Err(_) => decode_shift_jis(bytes),
    }
}

/// 名前 (大文字にして比べる) に `marker` を含む最初のエントリの中身を、Shift_JIS として decode する。
fn entry_text(files: &[(String, Vec<u8>)], marker: &str) -> Option<String> {
    let (_, bytes) = files
        .iter()
        .find(|(name, _)| name.to_uppercase().contains(marker))?;
    Some(decode_shift_jis(bytes))
}

/// zip の中身 ((名前, バイト列) の列) から KUDGURI の CSV を選んで parse する。
/// 名前に `KUDGURI` を含むエントリが無ければ `None` (無いときの扱いは呼び手が決める)。
pub fn kudguri_rows_in(
    files: &[(String, Vec<u8>)],
) -> Option<Result<Vec<kudguri::KudguriRow>, anyhow::Error>> {
    entry_text(files, "KUDGURI").map(|text| kudguri::parse_kudguri(&text))
}

/// zip の中身 ((名前, バイト列) の列) から KUDGIVT の CSV を選んで parse する。
/// 名前に `KUDGIVT` を含むエントリが無ければ `None` (無いときの扱いは呼び手が決める)。
pub fn kudgivt_rows_in(
    files: &[(String, Vec<u8>)],
) -> Option<Result<Vec<kudgivt::KudgivtRow>, anyhow::Error>> {
    entry_text(files, "KUDGIVT").map(|text| kudgivt::parse_kudgivt(&text))
}

/// 運行NOでCSVデータをグループ化
/// 各CSVファイルから運行NOを抽出し、運行NO→行データのマップを返す
pub fn group_csv_by_unko_no(csv_text: &str) -> std::collections::HashMap<String, Vec<String>> {
    let mut map: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    let mut lines = csv_text.lines();
    let _header = lines.next(); // skip header
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        // 運行NO is always the first column
        if let Some(unko_no) = line.split(',').next() {
            map.entry(unko_no.to_string())
                .or_default()
                .push(line.to_string());
        }
    }
    map
}

/// CSVテキストのヘッダー行を返す
pub fn csv_header(csv_text: &str) -> Option<&str> {
    csv_text.lines().next()
}

/// 分割した 1 ファイル (保存先に置く 1 object)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitFile {
    /// `{key_tenant}/unko/{unko_no}/{CSV名}`
    pub key: String,
    /// UTF-8。ヘッダ + '\n' + 各行 + '\n'
    pub content: Vec<u8>,
    pub is_kudgivt: bool,
    pub unko_no: String,
}

/// zip の 1 エントリ (名前と Shift_JIS のバイト列) を運行NO ごとに分ける。
/// 名前が `.csv` で終わらない (小文字化して判定) エントリは空を返す。
///
/// backend と分割 worker (ippoan/alc-dtako-worker) が同じこの関数を呼ぶ (同じ key・同じバイト列を出すため)。
/// 戻りの並びは不定 (運行NO の HashMap の順)。
pub fn split_csv_entry(key_tenant: &str, name: &str, bytes: &[u8]) -> Vec<SplitFile> {
    if !name.to_lowercase().ends_with(".csv") {
        return Vec::new();
    }
    let utf8_text = decode_shift_jis(bytes);
    let header = csv_header(&utf8_text);
    let grouped = group_csv_by_unko_no(&utf8_text);
    let is_kudgivt = name.to_uppercase().contains("KUDGIVT");

    let mut files = Vec::new();
    for (unko_no, lines) in &grouped {
        let csv_name = name
            .rsplit('/')
            .next()
            .unwrap_or(name)
            .to_uppercase()
            .replace(".CSV", ".csv");
        let key = format!("{}/unko/{}/{}", key_tenant, unko_no, csv_name);
        let mut content = String::new();
        if let Some(h) = header {
            content.push_str(h);
            content.push('\n');
        }
        for line in lines {
            content.push_str(line);
            content.push('\n');
        }
        files.push(SplitFile {
            key,
            content: content.into_bytes(),
            is_kudgivt,
            unko_no: unko_no.clone(),
        });
    }
    files
}

/// `requested` のうち `matched` に無いもの (印を付けようとして、当たる行が無かった運行NO)。
///
/// 運行NO は乗務員ごとに複数行あることが在るので、行数ではなく集合で比べる
/// (`requested` に重複が在れば、重複ぶんもそのまま返す)。
pub fn find_unmatched_kudgivt_unko_nos<'a>(
    requested: &'a [String],
    matched: &std::collections::HashSet<String>,
) -> Vec<&'a str> {
    requested
        .iter()
        .filter(|u| !matched.contains(u.as_str()))
        .map(|u| u.as_str())
        .collect()
}

/// 一覧をソートして `limit` 件に切り、(切った一覧, 切る前の総数) を返す (重複は除かない)。
pub fn cap_sorted(mut list: Vec<String>, limit: usize) -> (Vec<String>, usize) {
    list.sort();
    let total = list.len();
    list.truncate(limit);
    (list, total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "zip-extract")]
    #[test]
    fn test_extract_zip() {
        test_group!("CSVパーサー");
        test_case!("ZIP展開", {
            use std::io::Write;
            let mut buf = std::io::Cursor::new(Vec::new());
            {
                let mut zip = zip::ZipWriter::new(&mut buf);
                let opts = zip::write::SimpleFileOptions::default();
                zip.start_file("test.txt", opts).unwrap();
                zip.write_all(b"hello world").unwrap();
                zip.start_file("sub/data.csv", opts).unwrap();
                zip.write_all(b"col1,col2\na,b").unwrap();
                zip.finish().unwrap();
            }
            let files = extract_zip(&buf.into_inner()).unwrap();
            assert_eq!(files.len(), 2);
            assert_eq!(files[0].0, "test.txt");
            assert_eq!(files[0].1, b"hello world");
            assert_eq!(files[1].0, "sub/data.csv");
        });
    }

    #[cfg(feature = "zip-extract")]
    #[test]
    fn test_extract_zip_invalid() {
        test_group!("CSVパーサー");
        test_case!("不正なZIPでエラー", {
            assert!(extract_zip(b"not a zip").is_err());
        });
    }

    #[test]
    fn test_decode_shift_jis() {
        test_group!("CSVパーサー");
        test_case!("Shift-JISデコード", {
            let sjis_bytes = encoding_rs::SHIFT_JIS.encode("テスト").0.to_vec();
            assert_eq!(decode_shift_jis(&sjis_bytes), "テスト");
        });
    }

    #[test]
    fn test_decode_shift_jis_ascii() {
        test_group!("CSVパーサー");
        test_case!("ASCII文字のShift-JISデコード", {
            assert_eq!(decode_shift_jis(b"hello"), "hello");
        });
    }

    #[test]
    fn test_group_csv_by_unko_no() {
        test_group!("CSVパーサー");
        test_case!("運行NOでグループ化", {
            let csv = "運行NO,名前\n1001,田中\n1002,佐藤\n1001,鈴木\n";
            let map = group_csv_by_unko_no(csv);
            assert_eq!(map.len(), 2);
            assert_eq!(map["1001"].len(), 2);
            assert_eq!(map["1002"].len(), 1);
        });
    }

    #[test]
    fn test_group_csv_by_unko_no_empty_lines() {
        test_group!("CSVパーサー");
        test_case!("空行を含むCSVのグループ化", {
            let csv = "header\ndata1\n\n\ndata2\n";
            let map = group_csv_by_unko_no(csv);
            assert_eq!(map.len(), 2);
        });
    }

    #[test]
    fn test_csv_header() {
        test_group!("CSVパーサー");
        test_case!("CSVヘッダー取得", {
            assert_eq!(csv_header("col1,col2\nrow1"), Some("col1,col2"));
            assert_eq!(csv_header("single line"), Some("single line"));
        });
    }

    #[test]
    fn test_csv_header_none() {
        test_group!("CSVパーサー");
        test_case!("空CSVのヘッダーはNone", {
            assert_eq!(csv_header(""), None);
        });
    }

    #[test]
    fn decode_utf8_or_shift_jis_reads_utf8_as_is() {
        assert_eq!(
            decode_utf8_or_shift_jis("運行NO,読取日".as_bytes()),
            "運行NO,読取日"
        );
    }

    #[test]
    fn decode_utf8_or_shift_jis_falls_back_to_shift_jis() {
        // "運行NO,読取日" の Shift-JIS bytes。UTF-8 として不正なので、一致するのは
        // フォールバックが走った場合だけ。
        let sjis: &[u8] = &[
            0x89, 0x5e, 0x8d, 0x73, 0x4e, 0x4f, 0x2c, 0x93, 0xc7, 0x8e, 0xe6, 0x93, 0xfa,
        ];
        assert_eq!(decode_utf8_or_shift_jis(sjis), "運行NO,読取日");
    }

    #[test]
    fn decode_utf8_or_shift_jis_replaces_bytes_valid_in_neither() {
        // UTF-8 としても Shift_JIS としても読めないバイトは、Shift_JIS の decode が置換文字にする (落ちない)
        let decoded = decode_utf8_or_shift_jis(&[b'a', 0xff, b'b']);
        assert!(decoded.starts_with('a') && decoded.ends_with('b'));
        assert!(decoded.contains('\u{fffd}'), "{decoded:?}");
    }

    /// crew_role 1 (運転手) / 2 (副運転手) で同じ unko_no が 2 行 UPDATE されても
    /// (RETURNING が 2 行返る想定)、集合比較なら誤検知しない。これが今回の本題:
    /// 件数比較 (rows_affected) に戻すとこのケースで再び誤検知する。
    #[test]
    fn duplicate_rows_for_same_unko_no_do_not_produce_false_positive() {
        use std::collections::HashSet;
        let requested = vec!["1001".to_string(), "1002".to_string(), "1003".to_string()];
        let matched: HashSet<String> = ["1001", "1001", "1002", "1003"]
            .into_iter()
            .map(String::from)
            .collect();
        assert!(find_unmatched_kudgivt_unko_nos(&requested, &matched).is_empty());
    }

    #[test]
    fn missing_unko_no_is_reported() {
        use std::collections::HashSet;
        let requested = vec!["1001".to_string(), "1002".to_string(), "1003".to_string()];
        let matched: HashSet<String> = ["1001", "1003"].into_iter().map(String::from).collect();
        assert_eq!(
            find_unmatched_kudgivt_unko_nos(&requested, &matched),
            vec!["1002"]
        );
    }

    const KUDGURI_CSV: &str = "運行NO,読取日,運行日,事業所CD,事業所名,車輌CD,車輌名,乗務員CD1,乗務員名１,対象乗務員区分,出社日時,退社日時,出庫日時,帰庫日時,総走行距離,一般道運転時間,高速道運転時間,バイパス運転時間\n\
        1001,2026/03/01,2026/03/01,OFF01,テスト事業所,VH01,車両A,DR01,運転者A,1,2026/03/01 08:00:00,2026/03/01 18:00:00,2026/03/01 08:30:00,2026/03/01 17:30:00,150.5,300,60,20\n";
    const KUDGIVT_CSV: &str = "運行NO,読取日,乗務員CD1,乗務員名１,対象乗務員区分,開始日時,終了日時,イベントCD,イベント名,区間時間,区間距離\n\
        1001,2026/03/01,DR01,運転者A,1,2026/03/01 08:00:00,2026/03/01 08:30:00,100,出庫,30,0\n\
        1001,2026/03/01,DR01,運転者A,1,2026/03/01 12:00:00,2026/03/01 13:00:00,301,休憩,60,0\n";

    #[test]
    fn test_kudguri_and_kudgivt_rows_in() {
        test_group!("zip の中身から選ぶ");
        // 名前は大文字にして比べる (小文字・ディレクトリ付きでも当たる)。中身は Shift_JIS
        let files = vec![
            ("readme.txt".to_string(), b"x".to_vec()),
            ("data/kudguri.csv".to_string(), sjis(KUDGURI_CSV)),
            ("KUDGIVT.csv".to_string(), sjis(KUDGIVT_CSV)),
        ];
        test_case!("両方在る", {
            let kudguri = kudguri_rows_in(&files).unwrap().unwrap();
            assert_eq!(kudguri.len(), 1);
            assert_eq!(kudguri[0].unko_no, "1001");
            assert_eq!(kudguri[0].office_name, "テスト事業所");
            let kudgivt = kudgivt_rows_in(&files).unwrap().unwrap();
            let cds: Vec<&str> = kudgivt.iter().map(|r| r.event_cd.as_str()).collect();
            assert_eq!(cds, ["100", "301"]);
            assert_eq!(kudgivt[1].event_name, "休憩");
        });
        test_case!("KUDGURI が無い・KUDGIVT が無い → None", {
            assert!(kudguri_rows_in(&files[..1]).is_none());
            assert!(kudguri_rows_in(&files[2..]).is_none());
            assert!(kudgivt_rows_in(&files[..2]).is_none());
            assert!(kudgivt_rows_in(&[]).is_none());
        });
        test_case!(
            "同じ名前を含むエントリが複数在れば、最初のものを使う",
            {
                let two = vec![
                    ("KUDGIVT.csv".to_string(), sjis(KUDGIVT_CSV)),
                    ("old/KUDGIVT.csv".to_string(), b"broken".to_vec()),
                ];
                assert_eq!(kudgivt_rows_in(&two).unwrap().unwrap().len(), 2);
            }
        );
        test_case!("在るが parse できない → Some(Err)", {
            let bad = vec![
                ("KUDGURI.csv".to_string(), b"a,b\n1,2\n".to_vec()),
                ("KUDGIVT.csv".to_string(), Vec::new()),
            ];
            let err = kudguri_rows_in(&bad).unwrap().unwrap_err();
            assert!(err.to_string().contains("missing required columns"));
            let err = kudgivt_rows_in(&bad).unwrap().unwrap_err();
            assert_eq!(err.to_string(), "empty CSV");
        });
    }

    /// Shift_JIS のバイト列にする (テストの入力用)
    fn sjis(text: &str) -> Vec<u8> {
        encoding_rs::SHIFT_JIS.encode(text).0.to_vec()
    }

    /// key → (content, is_kudgivt, unko_no)。並びに依らずに比べる
    type SplitMap = std::collections::BTreeMap<String, (Vec<u8>, bool, String)>;

    fn split_map(key_tenant: &str, name: &str, bytes: &[u8]) -> SplitMap {
        let files = split_csv_entry(key_tenant, name, bytes);
        let n = files.len();
        let map: SplitMap = files
            .into_iter()
            .map(|f| (f.key, (f.content, f.is_kudgivt, f.unko_no)))
            .collect();
        assert_eq!(map.len(), n);
        map
    }

    fn expect(items: &[(&str, &[u8], bool, &str)]) -> SplitMap {
        items
            .iter()
            .map(|(k, c, kud, u)| (k.to_string(), (c.to_vec(), *kud, u.to_string())))
            .collect()
    }

    #[test]
    fn test_split_csv_entry_not_csv() {
        test_group!("CSV分割");
        test_case!(".csv 以外は空", {
            assert_eq!(split_csv_entry("t", "readme.txt", b"h\n1,a\n"), vec![]);
            assert_eq!(split_csv_entry("t", "KUDGIVT.csv.bak", b"h\n1,a\n"), vec![]);
            assert_eq!(split_csv_entry("t", "csv", b"h\n1,a\n"), vec![]);
        });
    }

    #[test]
    fn test_split_csv_entry_multiple_unko_nos() {
        test_group!("CSV分割");
        test_case!("複数の運行NO・空行を飛ばす", {
            let csv = b"unko,name\n1001,a\n1002,b\n\n   \n1001,c\n";
            assert_eq!(
                split_map("tenant-a", "KUDGURI.csv", csv),
                expect(&[
                    (
                        "tenant-a/unko/1001/KUDGURI.csv",
                        b"unko,name\n1001,a\n1001,c\n",
                        false,
                        "1001"
                    ),
                    (
                        "tenant-a/unko/1002/KUDGURI.csv",
                        b"unko,name\n1002,b\n",
                        false,
                        "1002"
                    ),
                ])
            );
        });
    }

    #[test]
    fn test_split_csv_entry_uppercase_extension() {
        test_group!("CSV分割");
        test_case!(
            "大文字の .CSV・小文字の名前は大文字化して拡張子だけ小文字",
            {
                assert_eq!(
                    split_map("t", "kudgivt.CSV", b"h\n7,x\n"),
                    expect(&[("t/unko/7/KUDGIVT.csv", b"h\n7,x\n", true, "7")])
                );
                assert_eq!(
                    split_map("t", "Sokudo.Csv", b"h\n7,x\n"),
                    expect(&[("t/unko/7/SOKUDO.csv", b"h\n7,x\n", false, "7")])
                );
            }
        );
    }

    #[test]
    fn test_split_csv_entry_directory_in_name() {
        test_group!("CSV分割");
        test_case!(
            "ディレクトリ付きの名前は basename を key に使う",
            {
                assert_eq!(
                    split_map("t", "a/b/KUDGIVT.csv", b"h\n7,x\n"),
                    expect(&[("t/unko/7/KUDGIVT.csv", b"h\n7,x\n", true, "7")])
                );
            }
        );
        test_case!("KUDGIVT の判定はフルパスの名前で効く", {
            assert_eq!(
                split_map("t", "KUDGIVT/x.csv", b"h\n7,x\n"),
                expect(&[("t/unko/7/X.csv", b"h\n7,x\n", true, "7")])
            );
            assert_eq!(
                split_map("t", "other/x.csv", b"h\n7,x\n"),
                expect(&[("t/unko/7/X.csv", b"h\n7,x\n", false, "7")])
            );
        });
    }

    #[test]
    fn test_split_csv_entry_header_only_or_empty() {
        test_group!("CSV分割");
        test_case!("ヘッダだけ・空の入力は空", {
            assert_eq!(split_csv_entry("t", "KUDGIVT.csv", b"unko,name\n"), vec![]);
            assert_eq!(split_csv_entry("t", "KUDGIVT.csv", b"unko,name"), vec![]);
            assert_eq!(split_csv_entry("t", "KUDGIVT.csv", b""), vec![]);
        });
    }

    #[test]
    fn test_split_csv_entry_crlf() {
        test_group!("CSV分割");
        test_case!(
            "CRLF の入力は LF で出る・末尾に改行が無くても付く",
            {
                assert_eq!(
                    split_map("t", "KUDGIVT.csv", b"h1,h2\r\n7,x\r\n8,y"),
                    expect(&[
                        ("t/unko/7/KUDGIVT.csv", b"h1,h2\n7,x\n", true, "7"),
                        ("t/unko/8/KUDGIVT.csv", b"h1,h2\n8,y\n", true, "8"),
                    ])
                );
            }
        );
    }

    #[test]
    fn test_split_csv_entry_first_column_is_the_key_as_is() {
        test_group!("CSV分割");
        test_case!(
            "先頭の列をそのまま運行NO に使う (空白・引用符を整えない)",
            {
                assert_eq!(
                    split_map("t", "KUDGIVT.csv", b"h\n 7,x\n\"7\",y\n,z\n"),
                    expect(&[
                        ("t/unko/ 7/KUDGIVT.csv", b"h\n 7,x\n", true, " 7"),
                        ("t/unko/\"7\"/KUDGIVT.csv", b"h\n\"7\",y\n", true, "\"7\""),
                        ("t/unko//KUDGIVT.csv", b"h\n,z\n", true, ""),
                    ])
                );
            }
        );
    }

    #[test]
    fn test_split_csv_entry_shift_jis() {
        test_group!("CSV分割");
        test_case!("Shift_JIS の日本語は UTF-8 で出る", {
            let input = sjis("運行NO,乗務員名\r\n2601,山田 太郎\r\n2602,鈴木\r\n2601,田中\r\n");
            assert_eq!(
                split_map("0a0a", "data/kudguri.csv", &input),
                expect(&[
                    (
                        "0a0a/unko/2601/KUDGURI.csv",
                        "運行NO,乗務員名\n2601,山田 太郎\n2601,田中\n".as_bytes(),
                        false,
                        "2601"
                    ),
                    (
                        "0a0a/unko/2602/KUDGURI.csv",
                        "運行NO,乗務員名\n2602,鈴木\n".as_bytes(),
                        false,
                        "2602"
                    ),
                ])
            );
        });
    }

    #[test]
    fn test_cap_sorted() {
        test_group!("CSV分割");
        let list = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<String>>();
        test_case!("limit 未満はソートだけ", {
            assert_eq!(cap_sorted(list(&["b", "a"]), 3), (list(&["a", "b"]), 2));
            assert_eq!(cap_sorted(list(&[]), 3), (list(&[]), 0));
        });
        test_case!("ちょうど limit", {
            assert_eq!(
                cap_sorted(list(&["c", "a", "b"]), 3),
                (list(&["a", "b", "c"]), 3)
            );
        });
        test_case!(
            "limit を超えるぶんは切り、切る前の総数を返す",
            {
                assert_eq!(
                    cap_sorted(list(&["d", "c", "a", "b"]), 3),
                    (list(&["a", "b", "c"]), 4)
                );
                assert_eq!(cap_sorted(list(&["b", "a"]), 0), (list(&[]), 2));
            }
        );
        test_case!("重複は残す", {
            assert_eq!(
                cap_sorted(list(&["b", "a", "b", "a"]), 3),
                (list(&["a", "a", "b"]), 4)
            );
        });
    }
}
