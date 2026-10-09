//! 取り込みの後の日別の計算し直し ([`crate::ingest`] の段 9) で、乗務員の月の運行のうち計算に渡す範囲を束ねで縮める
//! (Refs ippoan/alc-dtako-worker#23)。計算と保存は呼び手 (ここは行と日エントリを選ぶだけ。DB と保存先に触れない)。
//!
//! 束ね (鎖) = 運行を出発の順に並べ、[`alc_compare::starts_new_chain`] (= `group_operations_into_work_days` の判定) で切ったもの。
//! 今回の運行を含む鎖 (対象) とその前の鎖・次の鎖を計算に渡し、前の鎖と対象の鎖の日エントリだけを保存する
//! (前の鎖の最後の日の overlap の列は対象の最初の日で決まり、対象の最後の日の overlap の列は次の鎖の最初の日で決まるため)。
//!
//! 縮めた計算が月全体の計算と同じになると言える形だけ縮め、ほかは `None` (呼び手は月全体で計算する):
//! - 月の行の全てが出発・帰着を持ち、帰着 > 出発 (束ねの出発・帰着と、日の組み立てが見る出発・帰着が同じ運行になる)
//! - 計算に入れない鎖の日付 (出発日〜帰着日) が計算に入れる鎖の日付と重ならず、保存する鎖と次の鎖の日付も重ならない
//!   (同じ日の束ね・休息の集計・日エントリの key が鎖の外に漏れない)
//! - 前の鎖に入る境目 (前の鎖より前に鎖が在るとき) と次の鎖に入る境目の空きが [`BOUNDARY_REST_MINUTES`] 以上
//!   (overlap の連鎖が空きだけで必ず切れ、分割休息の持ち越しも翌日の控除も鎖をまたがない)
//! - 段 7 が書いた日エントリ (月の行の運行NO を持つもの) が、この乗務員CD で、保存する鎖の日付に入る
//!   (段 7 は日でセグメントを消すので、その日がほかの鎖のものだと、ほかの鎖のセグメントが消えたまま残る)
//!
//! 計算の後にも [`Window::saved_days`] が確かめる (外れたら `None`)。

use std::collections::{BTreeSet, HashMap};

use alc_compare::upload_daily::DailyHours;
use alc_compare::{starts_new_chain, DayKey};
use alc_csv_parser::kudguri::KudguriRow;
use chrono::{NaiveDate, NaiveDateTime};

/// 境目の空きの下限 (分)。overlap の連鎖が休息とみなす空きの大きい方 (日を跨がない運行の 540 分)。
pub const BOUNDARY_REST_MINUTES: i64 = 540;

/// 段 7 が書いた日エントリ 1 つ (乗務員CD・勤務日・運行NO)。
pub type Written = (String, NaiveDate, Vec<String>);

/// 縮めた範囲。
#[derive(Debug, Clone)]
pub struct Window {
    /// 計算に渡す行 (前の鎖・対象の鎖・次の鎖。月の行の順)
    pub rows: Vec<KudguriRow>,
    /// 保存する鎖 (前の鎖と対象の鎖) の運行NO (月の行の順。保存で消す対象)
    pub saved_unko_nos: Vec<String>,
    next_unko_nos: BTreeSet<String>,
    saved_dates: BTreeSet<NaiveDate>,
    next_dates: BTreeSet<NaiveDate>,
}

/// 鎖 1 つ (月の行の index と、それまでの帰着の最大からの空き (分)。最初の鎖は `None`)。
struct Chain {
    rows: Vec<usize>,
    gap: Option<i64>,
}

/// 運行の出発日〜帰着日。
fn dates_of(dep: NaiveDateTime, ret: NaiveDateTime) -> impl Iterator<Item = NaiveDate> {
    dep.date().iter_days().take_while(move |d| *d <= ret.date())
}

/// 乗務員 1 人の月の行 `rows` (乗務員CD `driver_cd`) から、今回の運行 `new_unko_nos` を含む範囲を選ぶ (形は module の doc)。
/// `written` = 段 7 が書いた日エントリ (全乗務員)。縮められなければ `None`。
pub fn narrow(
    rows: &[KudguriRow],
    new_unko_nos: &BTreeSet<String>,
    driver_cd: &str,
    written: &[Written],
) -> Option<Window> {
    let mut spans: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::with_capacity(rows.len());
    for row in rows {
        let (dep, ret) = (row.departure_at?, row.return_at?);
        if ret <= dep {
            return None;
        }
        spans.push((dep, ret));
    }

    // 出発の順 (同じなら月の行の順) に並べて鎖に分ける (`group_operations_into_work_days` と同じ並べ方・同じ判定)
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by_key(|&i| spans[i].0);
    let mut chains: Vec<Chain> = Vec::new();
    let mut last_end: Option<NaiveDateTime> = None;
    for i in order {
        let (dep, ret) = spans[i];
        match last_end {
            Some(end) if !starts_new_chain(dep, ret, end) => {
                chains.last_mut().expect("a chain").rows.push(i)
            }
            _ => chains.push(Chain {
                rows: vec![i],
                gap: last_end.map(|end| (dep - end).num_minutes()),
            }),
        }
        last_end = Some(last_end.map_or(ret, |end| end.max(ret)));
    }

    let is_target = |c: &Chain| {
        c.rows
            .iter()
            .any(|&i| new_unko_nos.contains(&rows[i].unko_no))
    };
    let lo = chains.iter().position(is_target)?;
    let hi = chains.iter().rposition(is_target).expect("a target chain");
    let first = lo.saturating_sub(1);
    let next = (hi + 1 < chains.len()).then_some(hi + 1);

    // 境目の空き
    let rested = |c: &Chain| c.gap.is_some_and(|gap| gap >= BOUNDARY_REST_MINUTES);
    if first > 0 && !rested(&chains[first]) {
        return None;
    }
    if next.is_some_and(|n| !rested(&chains[n])) {
        return None;
    }

    // 日付
    let chain_dates = |c: &Chain| -> BTreeSet<NaiveDate> {
        let days = c
            .rows
            .iter()
            .flat_map(|&i| dates_of(spans[i].0, spans[i].1));
        days.collect()
    };
    let mut saved_dates = BTreeSet::new();
    let mut next_dates = BTreeSet::new();
    let mut other_dates = BTreeSet::new();
    for (index, chain) in chains.iter().enumerate() {
        let side = if (first..=hi).contains(&index) {
            &mut saved_dates
        } else if Some(index) == next {
            &mut next_dates
        } else {
            &mut other_dates
        };
        side.extend(chain_dates(chain));
    }
    let computed_dates: BTreeSet<NaiveDate> = saved_dates.union(&next_dates).copied().collect();
    if !saved_dates.is_disjoint(&next_dates) || !computed_dates.is_disjoint(&other_dates) {
        return None;
    }

    // 段 7 が書いた日エントリ
    let month_unko_nos: BTreeSet<&str> = rows.iter().map(|r| r.unko_no.as_str()).collect();
    for (cd, date, unko_nos) in written {
        let in_month = unko_nos.iter().any(|u| month_unko_nos.contains(u.as_str()));
        if in_month && (cd != driver_cd || !saved_dates.contains(date)) {
            return None;
        }
    }

    let side_of = |index: usize| -> Option<bool> {
        let at = chains.iter().position(|c| c.rows.contains(&index))?;
        if (first..=hi).contains(&at) {
            Some(true)
        } else {
            (Some(at) == next).then_some(false)
        }
    };
    let mut window_rows = Vec::new();
    let mut saved_unko_nos: Vec<String> = Vec::new();
    let mut next_unko_nos = BTreeSet::new();
    for (index, row) in rows.iter().enumerate() {
        let Some(saved) = side_of(index) else {
            continue;
        };
        window_rows.push(row.clone());
        if !saved {
            next_unko_nos.insert(row.unko_no.clone());
        } else if !saved_unko_nos.contains(&row.unko_no) {
            saved_unko_nos.push(row.unko_no.clone());
        }
    }
    Some(Window {
        rows: window_rows,
        saved_unko_nos,
        next_unko_nos,
        saved_dates,
        next_dates,
    })
}

impl Window {
    /// [`Window::rows`] で計算した日エントリから、保存するもの (前の鎖と対象の鎖のもの) を選ぶ。
    /// 計算の結果が縮めた前提から外れたら `None` (呼び手は月全体で計算し直す):
    /// エントリの運行NO が保存する側と次の鎖の両方に掛かる / エントリの日・セグメントの日がその側の鎖の日付の外 /
    /// 計算に渡した運行が、どのエントリにも出ない。
    pub fn saved_days(
        &self,
        daily: HashMap<DayKey, DailyHours>,
    ) -> Option<HashMap<DayKey, DailyHours>> {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut kept = HashMap::new();
        for (key, hours) in daily {
            let in_next = |u: &String| self.next_unko_nos.contains(u);
            let saved = !hours.unko_nos.iter().any(in_next);
            if !saved && !hours.unko_nos.iter().all(in_next) {
                return None;
            }
            let dates = if saved {
                &self.saved_dates
            } else {
                &self.next_dates
            };
            let segment_days = hours
                .segments
                .iter()
                .flat_map(|s| [s.start_at.date(), s.end_at.date()]);
            if !std::iter::once(key.1)
                .chain(segment_days)
                .all(|d| dates.contains(&d))
            {
                return None;
            }
            seen.extend(hours.unko_nos.iter().cloned());
            if saved {
                kept.insert(key, hours);
            }
        }
        let all_seen = self.rows.iter().all(|r| seen.contains(&r.unko_no));
        all_seen.then_some(kept)
    }
}
