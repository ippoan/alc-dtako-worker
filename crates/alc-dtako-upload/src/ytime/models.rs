use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};

/// Y時間 シート 1 行分の出力データ。
///
/// `*_minutes_*` は **整数の分数** で返す。Worker 側で `/1440` して
/// fractional-day numeric として cell に書き込むことで、テンプレ既存の
/// `[h]:mm` 形式 (`25:30` のような 24h+ 表示も含む) を維持する。
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct YTimeRow {
    /// A 列とマッチングする bucket date (yyyy-mm-dd)
    pub date: NaiveDate,
    /// F 列: true → `1`、false → 空。深夜跨ぎの入力を終業日側の行に置いたとき true
    /// (勤務 ≥ 7h で始業が前日 5:00 以降、または 勤務 < 7h で終業が翌日 22:00 より後)。
    pub previous_day_start: bool,
    /// G 列の元値: 始業時刻の 0:00 からの分 (0..=1439)。
    /// F=1 のとき: 前日始業時刻 (前日 0:00 起点)。
    pub start_minutes_of_day: i32,
    /// H 列の元値: 終業時刻の bucket_date 0:00 からの分。
    /// 深夜跨ぎの入力を始業日側の行に置いたときは 1440 以上 (例: 翌日 09:30 → 33h30m → 2010)。
    /// 24 時間以内の入力と `y-time-rows` の口では、翌日 22:00 (2760) が上限。
    pub end_minutes_from_bucket_date: i32,
    /// I 列 (前日 5-22時): 休憩時間 (分)
    pub rest_prev_5_22: i32,
    /// J 列 (前日 22-0時): 休憩時間 (分)
    pub rest_prev_22_0: i32,
    /// K 列 (当日 0-5時): 休憩時間 (分)
    pub rest_today_0_5: i32,
    /// L 列 (当日 5-22時): 休憩時間 (分)
    pub rest_today_5_22: i32,
    /// M 列 (当日 22-0時): 休憩時間 (分)
    pub rest_today_22_0: i32,
    /// N 列 (翌日 0-5時): 休憩時間 (分)
    pub rest_next_0_5: i32,
    /// O 列 (翌日 5-22時): 休憩時間 (分)
    pub rest_next_5_22: i32,
    /// C 列: 自由文 (オプション)。同 bucket 結合や 24h cut 時の備考も入れる
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct YTimeDriver {
    pub cd: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct YTimePeriod {
    pub from: NaiveDate,
    pub to: NaiveDate,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct YTimeExportResponse {
    pub driver: YTimeDriver,
    pub period: YTimePeriod,
    pub rows: Vec<YTimeRow>,
    /// 例: 同 bucket_date に複数 segment が出現した場合の警告
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct YTimeExportQuery {
    pub driver_cd: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
}

/// `YYYY-MM-DD HH:MM:SS` (JST の壁時計) の文字列と `NaiveDateTime` の相互変換。
/// 勤怠の計算の応答 (`shift-days`) の時刻の形に合わせてある。
mod wall_clock {
    use chrono::NaiveDateTime;
    use serde::{Deserialize, Deserializer, Serializer};

    const FORMAT: &str = "%Y-%m-%d %H:%M:%S";

    pub fn serialize<S: Serializer>(dt: &NaiveDateTime, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&dt.format(FORMAT))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<NaiveDateTime, D::Error> {
        let raw = String::deserialize(d)?;
        NaiveDateTime::parse_from_str(&raw, FORMAT).map_err(serde::de::Error::custom)
    }
}

/// `POST /api/dtako/y-time-rows` の body。勤怠の勤務の列から Y時間 の行を作る。
#[derive(Debug, Clone, Deserialize)]
pub struct YTimeRowsRequest {
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub shifts: Vec<YTimeShiftInput>,
}

/// 勤務 1 本。`start` / `end` は JST の壁時計で、秒は 0 だけ (秒つきは 400。分に切り捨てて受けない)。
#[derive(Debug, Clone, Deserialize)]
pub struct YTimeShiftInput {
    #[serde(with = "wall_clock")]
    pub start: NaiveDateTime,
    #[serde(with = "wall_clock")]
    pub end: NaiveDateTime,
    /// 実働でない区間。`null` (欄なしも同じ) = まだ畳み直していない勤務、`[]` = 区間なし。
    #[serde(default)]
    pub non_working: Option<Vec<YTimeNonWorking>>,
    #[serde(default)]
    pub note: Option<String>,
}

/// 実働でない区間 `[start, end)`。秒は 0 だけ。種別 (`kind`) は在っても読まない (どの種別も実働でない)。
#[derive(Debug, Clone, Deserialize)]
pub struct YTimeNonWorking {
    #[serde(with = "wall_clock")]
    pub start: NaiveDateTime,
    #[serde(with = "wall_clock")]
    pub end: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct YTimeRowsResponse {
    pub rows: Vec<YTimeRow>,
    pub warnings: Vec<String>,
    /// 行を作らなかった勤務 (入力の順)
    pub excluded: Vec<YTimeExcludedShift>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct YTimeExcludedShift {
    #[serde(with = "wall_clock")]
    pub start: NaiveDateTime,
    #[serde(with = "wall_clock")]
    pub end: NaiveDateTime,
    pub reason: YTimeExcludedReason,
}

/// 勤務から行を作らなかった理由。
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum YTimeExcludedReason {
    /// `non_working` が null (まだ畳み直していない)
    NoNonWorking,
    /// 始業日と終業日が 2 日以上離れている
    ThreeDays,
    /// 同じ入力の中の別の勤務と時間帯が重なる
    Overlap,
    /// 始業が 5:00 より前 かつ 終業が翌日 22:00 より後 (どちらの形の行でも深夜の時間帯に載らない)
    NightBands,
}
