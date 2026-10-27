//! 1 ドライバー分の segment 列 (start, end, rest_minutes) を Y時間 シート行に
//! bucketing する pure logic。DB / R2 アクセスはここに含まない。

use chrono::{NaiveDate, NaiveDateTime, Timelike};

use super::models::YTimeRow;

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentInput {
    pub start: NaiveDateTime,
    pub end: NaiveDateTime,
    /// 後方互換用 (sum)。実態は rest_intervals の duration 合計と一致。
    pub rest_minutes: i32,
    /// 実働でない区間 (運行の経路では休憩イベント、勤怠の経路では休息と休憩) の (start, end)。
    /// bucket 確定後に時間帯別に振り分けるために必要。
    pub rest_intervals: Vec<(NaiveDateTime, NaiveDateTime)>,
    pub note: Option<String>,
}

/// (A) cutoff: 所定労働時間 (h)。深夜跨ぎの入力は、勤務時間 (= end - start - rest) がこれ以上なら
/// 終業日側 row (F=1)、未満なら始業日 row (24h+ 表記) をまず選ぶ。選んだ形が深夜の時間帯に
/// 載らないときは、もう一方の形に替える ([`RowShape::fits`])。
const WORK_CUTOFF_HOURS: f64 = 7.0;

/// 深夜跨ぎの入力を置く行の形。形ごとに、夜として数えられる時間帯が違う。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowShape {
    /// 終業日の行 (F=1)。夜 = 前日 22-24 時 / 当日 0-5 時 / 当日 22-24 時。
    /// 前日の始業〜22:00 は 1 つの昼の帯なので、前日 5:00 より前から始まる入力は載らない。
    EndDate,
    /// 始業日の行 (F=0、終業は 24h+ 表記)。夜 = 当日 0-5 時 / 当日 22-24 時 / 翌日 0-5 時。
    /// 翌日 5:00 以降は 1 つの昼の帯なので、翌日 22:00 より後に終わる入力は載らない。
    StartDate,
}

impl RowShape {
    fn other(self) -> Self {
        match self {
            RowShape::EndDate => RowShape::StartDate,
            RowShape::StartDate => RowShape::EndDate,
        }
    }

    /// この形の行の深夜の時間帯に `start`〜`end` が載るか。時刻だけで決める
    /// (その時間帯が全部休憩かどうかは見ない)。
    fn fits(self, start: NaiveDateTime, end: NaiveDateTime) -> bool {
        match self {
            RowShape::EndDate => {
                let prev = end.date().pred_opt().expect("valid prev date");
                start >= prev.and_hms_opt(5, 0, 0).expect("valid time")
            }
            RowShape::StartDate => {
                let next = start.date().succ_opt().expect("valid next date");
                end <= next.and_hms_opt(22, 0, 0).expect("valid time")
            }
        }
    }
}

/// 深夜跨ぎの入力が、どちらの形の行でも深夜の時間帯に載らないか
/// (始業が 5:00 より前 かつ 終業が翌日 22:00 より後)。同日の入力は常に false。
///
/// 24 時間以内の入力では起きない (始業が 5:00 より前なら終業は翌日 5:00 より前)。
pub(crate) fn fits_no_row_shape(start: NaiveDateTime, end: NaiveDateTime) -> bool {
    start.date() != end.date()
        && !RowShape::EndDate.fits(start, end)
        && !RowShape::StartDate.fits(start, end)
}

/// 1 つの入力を置く行: (行の日付, F, 終業の行の日付 0:00 からの分)。
type Placement = (NaiveDate, bool, i32);

/// 行の日付・F・終業の表記を決める。戻り値の 2 つ目は「どちらの形でも深夜の時間帯に載らない」
/// (そのときは勤務時間で選んだ形のまま)。
fn place(seg: &SegmentInput) -> (Placement, bool) {
    if seg.start.date() == seg.end.date() {
        return ((seg.start.date(), false, minutes_of_day(seg.end)), false);
    }

    let work_minutes = (seg.end - seg.start).num_minutes() as i32 - seg.rest_minutes;
    let work_hours = work_minutes as f64 / 60.0;
    let by_cutoff = if work_hours >= WORK_CUTOFF_HOURS {
        RowShape::EndDate
    } else {
        RowShape::StartDate
    };
    let no_shape_fits = fits_no_row_shape(seg.start, seg.end);
    let shape = if no_shape_fits || by_cutoff.fits(seg.start, seg.end) {
        by_cutoff
    } else {
        by_cutoff.other()
    };

    let placement = match shape {
        // 終業日 row F=1
        RowShape::EndDate => (seg.end.date(), true, minutes_of_day(seg.end)),
        // 始業日 row 24h+ 表記
        RowShape::StartDate => {
            let bucket = seg.start.date();
            let bucket_midnight = bucket.and_hms_opt(0, 0, 0).expect("valid midnight");
            let end_min = (seg.end - bucket_midnight).num_minutes() as i32;
            (bucket, false, end_min)
        }
    };
    (placement, no_shape_fits)
}

/// `segments` を bucketing して `YTimeRow` の列を返す。
///
/// 戻り値: `(rows, warnings)`
///
/// ルール:
/// - 同日 (start.date() == end.date()) → bucket = start.date()、F=0
/// - 深夜跨ぎ + 勤務時間 < 7h → bucket = start.date()、F=0、H 列 24h+ 表記
/// - 深夜跨ぎ + 勤務時間 ≥ 7h → bucket = end.date()、F=1、H 列 = end の minutes_of_day
///   (テンプレ数式が「F=1 のとき G を 0 として扱う」ので、当日 0:00 から H までを当日労働として計算)
/// - 深夜跨ぎで、上で選んだ形が深夜の時間帯に載らないとき (終業日 row で始業が 5:00 より前 /
///   始業日 row で終業が翌日 22:00 より後) は、もう一方の形に替える。どちらの形でも載らない入力は
///   勤務時間で選んだ形のまま行にして warning を 1 行足す (24 時間以内の入力では起きない)
/// - 期間外 (`bucket_date < from || > to`) は drop
/// - 同 bucket_date に複数 segment: 結合 (G=最早, H=最遅, rest=各 segment 内の休憩合計 +
///   segment 間の時間, F=any)
pub fn build_y_time_rows(
    mut segments: Vec<SegmentInput>,
    from: NaiveDate,
    to: NaiveDate,
) -> (Vec<YTimeRow>, Vec<String>) {
    segments.sort_by_key(|s| s.start);

    let mut warnings: Vec<String> = Vec::new();
    // bucket_date → 集約中の row データ
    let mut buckets: std::collections::HashMap<NaiveDate, BucketAccum> =
        std::collections::HashMap::new();
    let mut bucket_order: Vec<NaiveDate> = Vec::new();

    for seg in segments {
        let ((bucket, previous_day_start, end_min_from_bucket), no_shape_fits) = place(&seg);

        // 期間外
        if bucket < from || bucket > to {
            continue;
        }

        if no_shape_fits {
            warnings.push(format!(
                "{bucket}: 始業が 5:00 より前で終業が翌日 22:00 より後のため、深夜の時間帯に載らない (行の形は替えていない)"
            ));
        }

        // 休憩 7 セル split を計算 (bucket date に対する 前日/当日/翌日 で時間帯振り分け)
        let rest_split = split_rest_intervals(&seg.rest_intervals, bucket);

        let entry = buckets.entry(bucket).or_insert_with(|| {
            bucket_order.push(bucket);
            BucketAccum::new(bucket)
        });

        // 同じ行の直前のかたまりとの間 (最後に入ったかたまりの終わり 〜 今の始まり) は
        // 休憩の欄に算入する。重なり (間が 0 分以下) は何も足さない。
        let gap_split = gap_rest(entry.last_end, seg.start, bucket);
        entry.last_end = Some(entry.last_end.map_or(seg.end, |e| e.max(seg.end)));
        entry.rest.add(&gap_split);

        let already_had_seg = entry.has_segment;
        entry.merge(SegInBucket {
            start_min: minutes_of_day(seg.start),
            end_min: end_min_from_bucket,
            previous_day_start,
            rest: rest_split,
            note: seg.note,
        });

        if already_had_seg {
            warnings.push(format!(
                "{bucket}: 複数 segment 結合 (1 行に集約: 最早始業 / 最遅終業 / 間の {} 分を休憩に算入)",
                gap_split.total()
            ));
        }
    }

    let mut rows: Vec<YTimeRow> = bucket_order
        .into_iter()
        .map(|d| buckets.remove(&d).expect("bucket exists").into_row())
        .collect();
    rows.sort_by_key(|r| r.date);
    (rows, warnings)
}

/// 同じ行の直前のかたまりの終わり `prev_end` 〜 次の始まり `start` の間を 7 セルに振る。
/// `prev_end` が無い / 間が 0 分以下なら空。
///
/// 同じ行 (bucket) に入るかたまりの終わりは必ず bucket の 0:00 以降、次の始まりは bucket の
/// 24:00 より前なので、間は当日の 3 セル (0-5 / 5-22 / 22-24) に全部収まる (欄の外には出ない)。
fn gap_rest(prev_end: Option<NaiveDateTime>, start: NaiveDateTime, bucket: NaiveDate) -> RestSplit {
    match prev_end.filter(|e| start > *e) {
        Some(e) => split_rest_intervals(&[(e, start)], bucket),
        None => RestSplit::default(),
    }
}

/// 実働でない区間 (運行の経路では休憩イベント、勤怠の経路では休息と休憩) を、bucket date を基準にして
/// 7 セル (前日/当日/翌日 × 時間帯) に振り分け。
fn split_rest_intervals(
    intervals: &[(NaiveDateTime, NaiveDateTime)],
    bucket: NaiveDate,
) -> RestSplit {
    let mut s = RestSplit::default();
    let prev = bucket.pred_opt().expect("valid prev date");
    let next = bucket.succ_opt().expect("valid next date");
    for (start, end) in intervals {
        // 前日
        s.prev_5_22 += overlap_minutes(*start, *end, prev, 5 * 60, 22 * 60);
        s.prev_22_0 += overlap_minutes(*start, *end, prev, 22 * 60, 24 * 60);
        // 当日
        s.today_0_5 += overlap_minutes(*start, *end, bucket, 0, 5 * 60);
        s.today_5_22 += overlap_minutes(*start, *end, bucket, 5 * 60, 22 * 60);
        s.today_22_0 += overlap_minutes(*start, *end, bucket, 22 * 60, 24 * 60);
        // 翌日
        s.next_0_5 += overlap_minutes(*start, *end, next, 0, 5 * 60);
        s.next_5_22 += overlap_minutes(*start, *end, next, 5 * 60, 22 * 60);
    }
    s
}

/// interval (start, end) と (date 0:00 + start_min, date 0:00 + end_min) の重複分数。
fn overlap_minutes(
    a_start: NaiveDateTime,
    a_end: NaiveDateTime,
    date: NaiveDate,
    start_min: i32,
    end_min: i32,
) -> i32 {
    let midnight = date.and_hms_opt(0, 0, 0).expect("valid midnight");
    let b_start = midnight + chrono::Duration::minutes(start_min as i64);
    let b_end = midnight + chrono::Duration::minutes(end_min as i64);
    let lo = a_start.max(b_start);
    let hi = a_end.min(b_end);
    if hi > lo {
        (hi - lo).num_minutes() as i32
    } else {
        0
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
struct RestSplit {
    prev_5_22: i32,
    prev_22_0: i32,
    today_0_5: i32,
    today_5_22: i32,
    today_22_0: i32,
    next_0_5: i32,
    next_5_22: i32,
}

impl RestSplit {
    fn add(&mut self, other: &RestSplit) {
        self.prev_5_22 += other.prev_5_22;
        self.prev_22_0 += other.prev_22_0;
        self.today_0_5 += other.today_0_5;
        self.today_5_22 += other.today_5_22;
        self.today_22_0 += other.today_22_0;
        self.next_0_5 += other.next_0_5;
        self.next_5_22 += other.next_5_22;
    }

    fn total(&self) -> i32 {
        self.prev_5_22
            + self.prev_22_0
            + self.today_0_5
            + self.today_5_22
            + self.today_22_0
            + self.next_0_5
            + self.next_5_22
    }
}

struct SegInBucket {
    start_min: i32,
    end_min: i32,
    previous_day_start: bool,
    rest: RestSplit,
    note: Option<String>,
}

struct BucketAccum {
    date: NaiveDate,
    has_segment: bool,
    /// 最早始業 (= 一番小さい start_min)
    earliest_start: i32,
    /// 最遅終業 (= 一番大きい end_min。bucket midnight 起点 24h+ 含む)
    latest_end: i32,
    rest: RestSplit,
    previous_day_start: bool,
    notes: Vec<String>,
    /// この行に入ったかたまりの終わり (絶対時刻) の最大。次のかたまりとの間を出すのに使う
    last_end: Option<NaiveDateTime>,
}

impl BucketAccum {
    fn new(date: NaiveDate) -> Self {
        Self {
            date,
            has_segment: false,
            earliest_start: 0,
            latest_end: 0,
            rest: RestSplit::default(),
            previous_day_start: false,
            notes: Vec::new(),
            last_end: None,
        }
    }

    fn merge(&mut self, seg: SegInBucket) {
        if !self.has_segment {
            self.earliest_start = seg.start_min;
            self.latest_end = seg.end_min;
            self.has_segment = true;
        } else {
            // F=1 (前日始業) は値が大きいことが多い (前日 22:00 = 1320) ので min/max を素朴に取ると
            // バグる。F が混在するなら「F=1 を優先」する: F=1 の start_min はそのまま記録、
            // F=0 の start_min は無視 (テンプレ数式上 F=1 の G は 0 として扱うため、表示用)
            if seg.previous_day_start && !self.previous_day_start {
                // F=1 の segment を採用
                self.earliest_start = seg.start_min;
            } else if seg.previous_day_start == self.previous_day_start {
                // 同じ F なら最早を取る
                self.earliest_start = self.earliest_start.min(seg.start_min);
            }
            // 終業は常に最遅
            self.latest_end = self.latest_end.max(seg.end_min);
        }
        self.rest.add(&seg.rest);
        self.previous_day_start |= seg.previous_day_start;
        if let Some(n) = seg.note {
            self.notes.push(n);
        }
    }

    fn into_row(self) -> YTimeRow {
        YTimeRow {
            date: self.date,
            previous_day_start: self.previous_day_start,
            start_minutes_of_day: self.earliest_start,
            end_minutes_from_bucket_date: self.latest_end,
            rest_prev_5_22: self.rest.prev_5_22,
            rest_prev_22_0: self.rest.prev_22_0,
            rest_today_0_5: self.rest.today_0_5,
            rest_today_5_22: self.rest.today_5_22,
            rest_today_22_0: self.rest.today_22_0,
            rest_next_0_5: self.rest.next_0_5,
            rest_next_5_22: self.rest.next_5_22,
            note: if self.notes.is_empty() {
                None
            } else {
                Some(self.notes.join(" / "))
            },
        }
    }
}

fn minutes_of_day(dt: NaiveDateTime) -> i32 {
    dt.time().hour() as i32 * 60 + dt.time().minute() as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(date: (i32, u32, u32), time: (u32, u32)) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(date.0, date.1, date.2)
            .unwrap()
            .and_hms_opt(time.0, time.1, 0)
            .unwrap()
    }

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    fn seg_simple(
        start: NaiveDateTime,
        end: NaiveDateTime,
        rest: i32,
        note: Option<&str>,
    ) -> SegmentInput {
        SegmentInput {
            start,
            end,
            rest_minutes: rest,
            rest_intervals: Vec::new(),
            note: note.map(String::from),
        }
    }

    #[test]
    fn single_same_day_segment() {
        let s = seg_simple(
            dt((2024, 4, 15), (8, 30)),
            dt((2024, 4, 15), (17, 0)),
            60,
            None,
        );
        let (rows, warns) = build_y_time_rows(vec![s], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].date, d(2024, 4, 15));
        assert!(!rows[0].previous_day_start);
        assert_eq!(rows[0].start_minutes_of_day, 8 * 60 + 30);
        assert_eq!(rows[0].end_minutes_from_bucket_date, 17 * 60);
        assert!(warns.is_empty());
    }

    #[test]
    fn cross_midnight_short_work_uses_start_date_24h_plus() {
        // 4/15 22:30 → 4/16 04:30 (work 6h, rest 30 → work 5.5h < 7h cutoff)
        let s = SegmentInput {
            start: dt((2024, 4, 15), (22, 30)),
            end: dt((2024, 4, 16), (4, 30)),
            rest_minutes: 30,
            rest_intervals: vec![(dt((2024, 4, 16), (1, 0)), dt((2024, 4, 16), (1, 30)))],
            note: None,
        };
        let (rows, warns) = build_y_time_rows(vec![s], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows.len(), 1);
        // start_date 側 (4/15) に 24h+ で記録
        assert_eq!(rows[0].date, d(2024, 4, 15));
        assert!(!rows[0].previous_day_start);
        assert_eq!(rows[0].start_minutes_of_day, 22 * 60 + 30);
        // 28:30 = 4:30 翌日 = 28*60+30 = 1710
        assert_eq!(rows[0].end_minutes_from_bucket_date, 28 * 60 + 30);
        // 休憩 4/16 1:00-1:30 → 4/15 bucket では「翌日 0-5時」
        assert_eq!(rows[0].rest_next_0_5, 30);
        assert!(warns.is_empty());
    }

    #[test]
    fn cross_midnight_long_work_uses_end_date_with_f1() {
        // 4/15 22:00 → 4/16 06:00 (work 8h ≥ 7h cutoff)
        let s = seg_simple(
            dt((2024, 4, 15), (22, 0)),
            dt((2024, 4, 16), (6, 0)),
            0,
            None,
        );
        let (rows, _warns) = build_y_time_rows(vec![s], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].date, d(2024, 4, 16));
        assert!(rows[0].previous_day_start);
        assert_eq!(rows[0].start_minutes_of_day, 22 * 60);
        assert_eq!(rows[0].end_minutes_from_bucket_date, 6 * 60);
    }

    #[test]
    fn template_example_two_starts_on_same_calendar_day() {
        // テンプレ作者の例: 5/14 1:00-12:00 + 5/14 22:30 → 5/15 9:30
        let s1 = seg_simple(
            dt((2024, 5, 14), (1, 0)),
            dt((2024, 5, 14), (12, 0)),
            0,
            None,
        );
        let s2 = seg_simple(
            dt((2024, 5, 14), (22, 30)),
            dt((2024, 5, 15), (9, 30)),
            0,
            None,
        );
        let (rows, warns) = build_y_time_rows(vec![s1, s2], d(2024, 5, 1), d(2024, 5, 31));
        assert_eq!(rows.len(), 2);
        // 5/14 row: same-day
        assert_eq!(rows[0].date, d(2024, 5, 14));
        assert!(!rows[0].previous_day_start);
        assert_eq!(rows[0].start_minutes_of_day, 60);
        assert_eq!(rows[0].end_minutes_from_bucket_date, 12 * 60);
        // 5/15 row: cross-midnight, 11h ≥ 7h → end_date with F=1
        assert_eq!(rows[1].date, d(2024, 5, 15));
        assert!(rows[1].previous_day_start);
        assert_eq!(rows[1].start_minutes_of_day, 22 * 60 + 30);
        assert_eq!(rows[1].end_minutes_from_bucket_date, 9 * 60 + 30);
        assert!(warns.is_empty());
    }

    #[test]
    fn same_bucket_combine_two_segments() {
        // 4/2 22:00 → 4/3 09:00 (11h cross-midnight, ≥ 7h → 4/3 F=1)
        // 4/3 11:00 → 4/3 17:00 (6h same-day → 4/3)
        // → 4/3 row に結合: F=1, 始業 22:00 (前日), 終業 17:00
        let s1 = seg_simple(dt((2024, 4, 2), (22, 0)), dt((2024, 4, 3), (9, 0)), 0, None);
        let s2 = seg_simple(
            dt((2024, 4, 3), (11, 0)),
            dt((2024, 4, 3), (17, 0)),
            0,
            None,
        );
        let (rows, warns) = build_y_time_rows(vec![s1, s2], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].date, d(2024, 4, 3));
        assert!(rows[0].previous_day_start);
        // F=1 を優先で start = 22:00 (前日)
        assert_eq!(rows[0].start_minutes_of_day, 22 * 60);
        // 終業最遅 17:00
        assert_eq!(rows[0].end_minutes_from_bucket_date, 17 * 60);
        assert_eq!(warns.len(), 1);
        assert!(warns[0].contains("複数 segment 結合"));
        // 間の 09:00-11:00 は当日 5-22 の休憩に入る
        assert_eq!(rows[0].rest_today_5_22, 120);
        assert_eq!(rows[0].rest_prev_5_22, 0);
        assert_eq!(rows[0].rest_prev_22_0, 0);
        assert_eq!(rows[0].rest_today_0_5, 0);
        assert_eq!(rows[0].rest_today_22_0, 0);
        assert_eq!(rows[0].rest_next_0_5, 0);
        assert_eq!(rows[0].rest_next_5_22, 0);
        assert_eq!(
            warns[0],
            "2024-04-03: 複数 segment 結合 (1 行に集約: 最早始業 / 最遅終業 / 間の 120 分を休憩に算入)"
        );
    }

    #[test]
    fn same_bucket_three_segments_add_both_gaps() {
        let s1 = seg_simple(dt((2024, 4, 3), (6, 0)), dt((2024, 4, 3), (8, 0)), 0, None);
        let s2 = seg_simple(dt((2024, 4, 3), (9, 0)), dt((2024, 4, 3), (12, 0)), 0, None);
        let s3 = seg_simple(
            dt((2024, 4, 3), (14, 0)),
            dt((2024, 4, 3), (18, 0)),
            0,
            None,
        );
        let (rows, warns) = build_y_time_rows(vec![s3, s1, s2], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows.len(), 1);
        // 08:00-09:00 (60) + 12:00-14:00 (120)
        assert_eq!(rows[0].rest_today_5_22, 180);
        assert_eq!(warns.len(), 2);
        assert!(warns[0].contains("間の 60 分を休憩に算入"));
        assert!(warns[1].contains("間の 120 分を休憩に算入"));
    }

    #[test]
    fn gap_across_midnight_is_split_by_time_band_in_f1_row() {
        // 4/2 21:00 → 4/3 04:00 (7h、F=1 で 4/3 row) の後 4/3 08:00-12:00。
        // 間 04:00-08:00 は 当日 0-5 が 60、当日 5-22 が 180
        let s1 = seg_simple(dt((2024, 4, 2), (21, 0)), dt((2024, 4, 3), (4, 0)), 0, None);
        let s2 = seg_simple(dt((2024, 4, 3), (8, 0)), dt((2024, 4, 3), (12, 0)), 0, None);
        let (rows, warns) = build_y_time_rows(vec![s1, s2], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows.len(), 1);
        assert!(rows[0].previous_day_start);
        assert_eq!(rows[0].rest_today_0_5, 60);
        assert_eq!(rows[0].rest_today_5_22, 180);
        assert_eq!(rows[0].rest_prev_22_0, 0);
        assert_eq!(warns.len(), 1);
        assert!(warns[0].contains("間の 240 分を休憩に算入"));
    }

    #[test]
    fn gap_rest_none_or_non_positive_is_empty() {
        let b = d(2024, 4, 3);
        let t = dt((2024, 4, 3), (10, 0));
        assert_eq!(gap_rest(None, t, b), RestSplit::default());
        assert_eq!(gap_rest(Some(t), t, b), RestSplit::default());
        let later = dt((2024, 4, 3), (11, 0));
        assert_eq!(gap_rest(Some(later), t, b), RestSplit::default());
    }

    /// かたまりの種類の組み合わせごとに「間の長さ = 行の休憩に足された分」を固定する。
    /// (i) 始業も終業も当日 / (ii) 深夜またぎ 7h 以上 (終業日の行、F=1) /
    /// (iii) 深夜またぎ 7h 未満 (始業日の行)
    fn assert_gap_fully_counted(segs: Vec<SegmentInput>, expected_gap: i32) {
        let (rows, _) = build_y_time_rows(segs, d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.rest_prev_5_22, 0);
        assert_eq!(r.rest_prev_22_0, 0);
        assert_eq!(r.rest_next_0_5, 0);
        assert_eq!(r.rest_next_5_22, 0);
        assert_eq!(
            r.rest_today_0_5 + r.rest_today_5_22 + r.rest_today_22_0,
            expected_gap
        );
    }

    #[test]
    fn gap_equals_added_rest_for_ii_then_i() {
        assert_gap_fully_counted(
            vec![
                seg_simple(dt((2024, 4, 2), (21, 0)), dt((2024, 4, 3), (4, 0)), 0, None),
                seg_simple(dt((2024, 4, 3), (8, 0)), dt((2024, 4, 3), (12, 0)), 0, None),
            ],
            240,
        );
    }

    #[test]
    fn gap_equals_added_rest_for_i_then_i() {
        assert_gap_fully_counted(
            vec![
                seg_simple(dt((2024, 4, 3), (8, 0)), dt((2024, 4, 3), (12, 0)), 0, None),
                seg_simple(
                    dt((2024, 4, 3), (14, 0)),
                    dt((2024, 4, 3), (18, 0)),
                    0,
                    None,
                ),
            ],
            120,
        );
    }

    #[test]
    fn gap_equals_added_rest_for_i_then_iii() {
        // (iii) 4/3 22:00 → 4/4 02:00 (4h < 7h) は始業日 4/3 の行。間 12:00-22:00 = 600
        assert_gap_fully_counted(
            vec![
                seg_simple(dt((2024, 4, 3), (8, 0)), dt((2024, 4, 3), (12, 0)), 0, None),
                seg_simple(dt((2024, 4, 3), (22, 0)), dt((2024, 4, 4), (2, 0)), 0, None),
            ],
            600,
        );
    }

    #[test]
    fn gap_equals_added_rest_for_ii_then_iii() {
        // 間 04:00-22:00 = 1080 (当日 0-5 が 60、5-22 が 1020)
        assert_gap_fully_counted(
            vec![
                seg_simple(dt((2024, 4, 2), (21, 0)), dt((2024, 4, 3), (4, 0)), 0, None),
                seg_simple(dt((2024, 4, 3), (22, 0)), dt((2024, 4, 4), (3, 0)), 0, None),
            ],
            1080,
        );
    }

    #[test]
    fn overlapping_segments_add_no_rest() {
        let s1 = seg_simple(dt((2024, 4, 3), (8, 0)), dt((2024, 4, 3), (15, 0)), 0, None);
        let s2 = seg_simple(
            dt((2024, 4, 3), (10, 0)),
            dt((2024, 4, 3), (12, 0)),
            0,
            None,
        );
        let (rows, warns) = build_y_time_rows(vec![s1, s2], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].rest_today_5_22, 0);
        assert_eq!(warns.len(), 1);
        assert!(warns[0].contains("間の 0 分を休憩に算入"));
    }

    #[test]
    fn overlap_does_not_double_count_gap_after_longer_segment() {
        // s1 8-15、s2 10-12 (s1 に内包)、s3 16-18。間は 15-16 の 60 分だけ
        let s1 = seg_simple(dt((2024, 4, 3), (8, 0)), dt((2024, 4, 3), (15, 0)), 0, None);
        let s2 = seg_simple(
            dt((2024, 4, 3), (10, 0)),
            dt((2024, 4, 3), (12, 0)),
            0,
            None,
        );
        let s3 = seg_simple(
            dt((2024, 4, 3), (16, 0)),
            dt((2024, 4, 3), (18, 0)),
            0,
            None,
        );
        let (rows, _) = build_y_time_rows(vec![s1, s2, s3], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows[0].rest_today_5_22, 60);
    }

    #[test]
    fn contiguous_split_pieces_add_no_rest() {
        // 24 時間で強制的に切った片どうしは連続 (間 0 分)
        let s1 = seg_simple(dt((2024, 4, 3), (0, 0)), dt((2024, 4, 3), (12, 0)), 0, None);
        let s2 = seg_simple(
            dt((2024, 4, 3), (12, 0)),
            dt((2024, 4, 3), (20, 0)),
            0,
            None,
        );
        let (rows, warns) = build_y_time_rows(vec![s1, s2], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows[0].rest_today_5_22, 0);
        assert_eq!(rows[0].rest_today_0_5, 0);
        assert!(warns[0].contains("間の 0 分を休憩に算入"));
    }

    #[test]
    fn rest_inside_segments_and_gap_are_summed() {
        let s1 = SegmentInput {
            start: dt((2024, 4, 3), (8, 0)),
            end: dt((2024, 4, 3), (12, 0)),
            rest_minutes: 30,
            rest_intervals: vec![(dt((2024, 4, 3), (10, 0)), dt((2024, 4, 3), (10, 30)))],
            note: None,
        };
        let s2 = SegmentInput {
            start: dt((2024, 4, 3), (13, 0)),
            end: dt((2024, 4, 3), (17, 0)),
            rest_minutes: 15,
            rest_intervals: vec![(dt((2024, 4, 3), (15, 0)), dt((2024, 4, 3), (15, 15)))],
            note: None,
        };
        let (rows, _) = build_y_time_rows(vec![s1, s2], d(2024, 4, 1), d(2024, 4, 30));
        // 30 + 15 + 間 60
        assert_eq!(rows[0].rest_today_5_22, 105);
    }

    #[test]
    fn out_of_range_filtered() {
        let s = seg_simple(
            dt((2024, 4, 15), (8, 0)),
            dt((2024, 4, 15), (17, 0)),
            0,
            None,
        );
        let (rows, _) = build_y_time_rows(vec![s], d(2024, 4, 16), d(2024, 4, 30));
        assert!(rows.is_empty());
    }

    #[test]
    fn multiple_distinct_days_sorted_ascending() {
        let segments = vec![
            seg_simple(
                dt((2024, 4, 18), (8, 0)),
                dt((2024, 4, 18), (17, 0)),
                0,
                None,
            ),
            seg_simple(
                dt((2024, 4, 15), (8, 0)),
                dt((2024, 4, 15), (17, 0)),
                0,
                None,
            ),
            seg_simple(
                dt((2024, 4, 22), (8, 0)),
                dt((2024, 4, 22), (17, 0)),
                0,
                None,
            ),
        ];
        let (rows, warns) = build_y_time_rows(segments, d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].date, d(2024, 4, 15));
        assert_eq!(rows[1].date, d(2024, 4, 18));
        assert_eq!(rows[2].date, d(2024, 4, 22));
        assert!(warns.is_empty());
    }

    #[test]
    fn rest_split_categorizes_by_time_of_day() {
        // 4/15 8:00 - 4/15 22:00 (same-day) with rests:
        //   4/15 4:00-5:00 (前日?? いや、同日の 0-5時) ← 当日 0-5
        //   4/15 12:00-13:00 (5-22 帯) ← 当日 5-22
        //   4/15 23:00-23:30 (22-0 帯) ← 当日 22-0
        let s = SegmentInput {
            start: dt((2024, 4, 15), (8, 0)),
            end: dt((2024, 4, 15), (22, 0)),
            rest_minutes: 150,
            rest_intervals: vec![
                (dt((2024, 4, 15), (4, 0)), dt((2024, 4, 15), (5, 0))),
                (dt((2024, 4, 15), (12, 0)), dt((2024, 4, 15), (13, 0))),
                (dt((2024, 4, 15), (23, 0)), dt((2024, 4, 15), (23, 30))),
            ],
            note: None,
        };
        let (rows, _) = build_y_time_rows(vec![s], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].rest_today_0_5, 60);
        assert_eq!(rows[0].rest_today_5_22, 60);
        assert_eq!(rows[0].rest_today_22_0, 30);
        assert_eq!(rows[0].rest_prev_5_22, 0);
        assert_eq!(rows[0].rest_next_0_5, 0);
    }

    #[test]
    fn note_passes_through() {
        let s = seg_simple(
            dt((2024, 4, 15), (8, 0)),
            dt((2024, 4, 15), (17, 0)),
            0,
            Some("テスト備考"),
        );
        let (rows, _) = build_y_time_rows(vec![s], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows[0].note.as_deref(), Some("テスト備考"));
    }

    #[test]
    fn empty_input_yields_empty_output() {
        let (rows, warns) = build_y_time_rows(vec![], d(2024, 4, 1), d(2024, 4, 30));
        assert!(rows.is_empty());
        assert!(warns.is_empty());
    }

    // ---- 行の置き方: 選んだ形が深夜の時間帯に載らないとき、もう一方の形に替える ----

    /// `[start, end]` のうち休憩を除いた分を、`date` の `[from_min, to_min)` の帯について数える。
    fn work_in_band(seg: &SegmentInput, date: NaiveDate, from_min: i32, to_min: i32) -> i32 {
        let rest: i32 = seg
            .rest_intervals
            .iter()
            .map(|(s, e)| overlap_minutes(*s, *e, date, from_min, to_min))
            .sum();
        overlap_minutes(seg.start, seg.end, date, from_min, to_min) - rest
    }

    /// 勤務の中の 22-5 時の実労働 (暦日ごとの 0-5 時と 22-24 時の和)。
    fn night_work_of_segment(seg: &SegmentInput) -> i32 {
        let mut total = 0;
        let mut day = seg.start.date();
        while day <= seg.end.date() {
            total += work_in_band(seg, day, 0, 5 * 60) + work_in_band(seg, day, 22 * 60, 24 * 60);
            day = day.succ_opt().unwrap();
        }
        total
    }

    /// その形の行が夜として数える時間帯の中の実労働。
    /// 終業日の行 (F=1): 前日 22-24 / 当日 0-5 / 当日 22-24。
    /// 始業日の行 (F=0): 当日 0-5 / 当日 22-24 / 翌日 0-5。
    fn night_work_counted_by_row(seg: &SegmentInput, row: &YTimeRow) -> i32 {
        let today = row.date;
        let same_day =
            work_in_band(seg, today, 0, 5 * 60) + work_in_band(seg, today, 22 * 60, 24 * 60);
        if row.previous_day_start {
            same_day + work_in_band(seg, today.pred_opt().unwrap(), 22 * 60, 24 * 60)
        } else {
            same_day + work_in_band(seg, today.succ_opt().unwrap(), 0, 5 * 60)
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
    fn long_work_starting_before_5_moves_to_start_date_row() {
        // (a) 4/15 03:00 → 4/16 01:00、休憩 03:30-04:00。実働 21.5h (7h 以上) だが始業が 5:00 より前
        let s = SegmentInput {
            start: dt((2024, 4, 15), (3, 0)),
            end: dt((2024, 4, 16), (1, 0)),
            rest_minutes: 30,
            rest_intervals: vec![(dt((2024, 4, 15), (3, 30)), dt((2024, 4, 15), (4, 0)))],
            note: None,
        };
        let (rows, warns) = build_y_time_rows(vec![s.clone()], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].date, d(2024, 4, 15));
        assert!(!rows[0].previous_day_start);
        assert_eq!(rows[0].start_minutes_of_day, 3 * 60);
        // 25:00
        assert_eq!(rows[0].end_minutes_from_bucket_date, 1500);
        // 03:30-04:00 の休憩は「当日 0-5 時」に入る (欄の外へ消えない)
        assert_eq!(rows[0].rest_today_0_5, 30);
        assert_eq!(rest_cells_total(&rows[0]), 30);
        assert_eq!(night_work_of_segment(&s), 270);
        assert_eq!(night_work_counted_by_row(&s, &rows[0]), 270);
        assert!(warns.is_empty());
    }

    #[test]
    fn short_work_ending_after_next_22_moves_to_end_date_row() {
        // (b) 4/15 20:00 → 4/16 23:00、休憩 4/15 21:00-4/16 17:30 と 4/16 22:10-22:40。
        // 実働 6h (7h 未満) だが終業が翌日 22:00 より後
        let s = SegmentInput {
            start: dt((2024, 4, 15), (20, 0)),
            end: dt((2024, 4, 16), (23, 0)),
            rest_minutes: 1260,
            rest_intervals: vec![
                (dt((2024, 4, 15), (21, 0)), dt((2024, 4, 16), (17, 30))),
                (dt((2024, 4, 16), (22, 10)), dt((2024, 4, 16), (22, 40))),
            ],
            note: None,
        };
        let (rows, warns) = build_y_time_rows(vec![s.clone()], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].date, d(2024, 4, 16));
        assert!(rows[0].previous_day_start);
        assert_eq!(rows[0].start_minutes_of_day, 20 * 60);
        assert_eq!(rows[0].end_minutes_from_bucket_date, 23 * 60);
        // 22:10-22:40 の休憩は「当日 22-24 時」に入る (欄の外へ消えない)
        assert_eq!(rows[0].rest_today_22_0, 30);
        assert_eq!(rows[0].rest_prev_5_22, 60);
        assert_eq!(rows[0].rest_prev_22_0, 120);
        assert_eq!(rows[0].rest_today_0_5, 300);
        assert_eq!(rows[0].rest_today_5_22, 750);
        assert_eq!(rest_cells_total(&rows[0]), 1260);
        assert_eq!(night_work_of_segment(&s), 30);
        assert_eq!(night_work_counted_by_row(&s, &rows[0]), 30);
        assert!(warns.is_empty());
    }

    #[test]
    fn long_work_starting_at_or_after_5_stays_on_end_date_row() {
        // (c) 22:00 → 翌 09:00 は今までどおり。境界: 始業ちょうど 5:00 は終業日の行のまま
        for start_hm in [(22, 0), (5, 0)] {
            let s = seg_simple(
                dt((2024, 4, 15), start_hm),
                dt((2024, 4, 16), (9, 0)),
                0,
                None,
            );
            let (rows, warns) = build_y_time_rows(vec![s], d(2024, 4, 1), d(2024, 4, 30));
            assert_eq!(rows[0].date, d(2024, 4, 16));
            assert!(rows[0].previous_day_start);
            assert_eq!(rows[0].end_minutes_from_bucket_date, 9 * 60);
            assert!(warns.is_empty());
        }
    }

    #[test]
    fn short_work_ending_by_next_22_stays_on_start_date_row() {
        // (d) 23:00 → 翌 02:00 は今までどおり
        let s = seg_simple(
            dt((2024, 4, 15), (23, 0)),
            dt((2024, 4, 16), (2, 0)),
            0,
            None,
        );
        let (rows, warns) = build_y_time_rows(vec![s], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows[0].date, d(2024, 4, 15));
        assert!(!rows[0].previous_day_start);
        assert_eq!(rows[0].end_minutes_from_bucket_date, 26 * 60);
        assert!(warns.is_empty());

        // 境界: 終業ちょうど翌日 22:00 は始業日の行のまま (20:00 → 翌 22:00、休憩 20h で実働 6h)
        let s = SegmentInput {
            start: dt((2024, 4, 15), (20, 0)),
            end: dt((2024, 4, 16), (22, 0)),
            rest_minutes: 1200,
            rest_intervals: vec![(dt((2024, 4, 15), (21, 0)), dt((2024, 4, 16), (17, 0)))],
            note: None,
        };
        let (rows, warns) = build_y_time_rows(vec![s], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows[0].date, d(2024, 4, 15));
        assert!(!rows[0].previous_day_start);
        assert_eq!(rows[0].end_minutes_from_bucket_date, 46 * 60);
        assert!(warns.is_empty());
    }

    #[test]
    fn input_fitting_no_shape_keeps_cutoff_shape_and_warns() {
        // (e) 03:00 → 翌 23:00。どちらの形でも載らない → 勤務時間で選んだ形のまま + 警告 1 行
        let long = seg_simple(
            dt((2024, 4, 15), (3, 0)),
            dt((2024, 4, 16), (23, 0)),
            0,
            None,
        );
        let (rows, warns) = build_y_time_rows(vec![long], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows[0].date, d(2024, 4, 16));
        assert!(rows[0].previous_day_start);
        assert_eq!(
            warns,
            vec!["2024-04-16: 始業が 5:00 より前で終業が翌日 22:00 より後のため、深夜の時間帯に載らない (行の形は替えていない)".to_string()]
        );

        // 実働 7h 未満なら始業日の行のまま
        let short = seg_simple(
            dt((2024, 4, 15), (3, 0)),
            dt((2024, 4, 16), (23, 0)),
            2400,
            None,
        );
        let (rows, warns) = build_y_time_rows(vec![short], d(2024, 4, 1), d(2024, 4, 30));
        assert_eq!(rows[0].date, d(2024, 4, 15));
        assert!(!rows[0].previous_day_start);
        assert_eq!(rows[0].end_minutes_from_bucket_date, 47 * 60);
        assert_eq!(warns.len(), 1);

        // 行の日付が期間の外なら、行も警告も出ない
        let out = seg_simple(
            dt((2024, 4, 15), (3, 0)),
            dt((2024, 4, 16), (23, 0)),
            0,
            None,
        );
        let (rows, warns) = build_y_time_rows(vec![out], d(2024, 5, 1), d(2024, 5, 31));
        assert!(rows.is_empty());
        assert!(warns.is_empty());
    }

    #[test]
    fn pieces_within_24h_always_fit_one_shape() {
        // (f) 24 時間以内の片 (運行から作る経路) では「どちらの形でも載らない」が起きない
        let day0 = dt((2024, 4, 15), (0, 0));
        for start_min in 0..1440 {
            let start = day0 + chrono::Duration::minutes(start_min);
            for len_min in [1, 300, 419, 420, 1020, 1021, 1439, 1440] {
                let end = start + chrono::Duration::minutes(len_min);
                assert!(!fits_no_row_shape(start, end), "{start} → {end}");
            }
        }
        // 24 時間を 1 分でも超えれば起きうる (04:59 → 翌 22:01 は 41h02m)
        assert!(fits_no_row_shape(
            dt((2024, 4, 15), (4, 59)),
            dt((2024, 4, 16), (22, 1))
        ));
        // 同日は対象外
        assert!(!fits_no_row_shape(
            dt((2024, 4, 15), (3, 0)),
            dt((2024, 4, 15), (23, 30))
        ));
    }

    /// 同じ日に、前日始業でない区間の後から前日始業の区間が入ると、始業は前日始業の区間のものにする
    /// (alc-dtako-worker で足したテスト。rust では mock の結合テストが通していた分岐)。
    #[test]
    fn merge_prefers_previous_day_start_arriving_later() {
        let seg = |start_min, end_min, previous_day_start| SegInBucket {
            start_min,
            end_min,
            previous_day_start,
            rest: RestSplit::default(),
            note: None,
        };
        let mut acc = BucketAccum::new(NaiveDate::from_ymd_opt(2024, 4, 15).unwrap());
        acc.merge(seg(8 * 60, 12 * 60, false));
        acc.merge(seg(22 * 60, 9 * 60, true));
        assert_eq!(acc.earliest_start, 22 * 60);
        assert_eq!(acc.latest_end, 12 * 60);
        assert!(acc.previous_day_start);
        // 前日始業の後から来た前日始業でない区間は始業を変えない
        acc.merge(seg(6 * 60, 7 * 60, false));
        assert_eq!(acc.earliest_start, 22 * 60);
    }
}
