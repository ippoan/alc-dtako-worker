//! 勤怠向けの dtako の生イベントの読み取り (W5b、Refs ippoan/rust-alc-api#680。元は ohishi-exp/rust-ichibanboshi#205)。
//!
//! - `GET /dtako/events?driver_cd=X&date_from=&date_to=` (乗務員 1 人) /
//!   `GET /dtako/events?date_from=&date_to=[&page_size=&after_driver_cd=]` (全乗務員、乗務員CD の keyset のページ):
//!   期間内の分割済みの運行の KUDGIVT.csv を保存先から読み、**畳まずに** `{headers, rows}` のまま返す
//! - `GET /dtako/events/etags?date_from=&date_to=`: CSV を読まずに LIST だけで「月の指紋」(運行NO → ETag) を作る
//!
//! 呼び手は rust-ichibanboshi の勤怠 Worker (auth-worker の `KintaiAlcEntrypoint` の RPC 越し)。**応答の形・status・
//! 本文・期間の上限・ページの大きさ・運行の並び・ETag の形は rust-alc-api の `alc_dtako::dtako_events` と同じ**
//! (`etags` の `items` の件数・順・`unko_no`・`etag` は呼び手の月の指紋の入力そのもの。変えると全部の月が計算し直しになる)。
//!
//! rust との違いは 2 つ: 保存先の同時の読みが Workers の同時接続の上限 ([`GET_CONCURRENCY`] = 6) に合わせて少ない
//! (rust は 16)、取得に失敗した運行の warning の理由が固定の語 (`not found`・保存先のエラーの段の名前)。

use std::collections::HashMap;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::pg::{self, EventsOperation};
use crate::routes::DtakoState;
use crate::split::LogLevel;
use crate::store::{ListedObject, GET_CONCURRENCY};

/// `driver_cd` 指定時の期間の上限 (日)。
pub const MAX_RANGE_DAYS_SINGLE: i64 = 366;
/// `driver_cd` 省略時 (全乗務員) の期間の上限 (日)。
pub const MAX_RANGE_DAYS_ALL: i64 = 31;
/// `etags` の期間の上限 (日。LIST だけなので CSV の枚数の根拠は当てはまらない。呼び手の月 + 猶予に余裕を持たせた値)。
pub const MAX_RANGE_DAYS_ETAGS: i64 = 40;
/// 全乗務員版の 1 ページの乗務員数の既定値と上限。
pub const DEFAULT_PAGE_SIZE: i64 = 25;
pub const MAX_PAGE_SIZE: i64 = 50;
/// `etags` が全乗務員を 1 回で引くための件数 (ページングの概念は無い)。
const ALL_DRIVERS_UNLIMITED: i64 = i64::MAX;
/// 応答に載せる未分割の運行の上限 (総数は `unsplit_total`)。
pub const UNSPLIT_DISPLAY_LIMIT: usize = 500;

#[derive(Debug, Clone, Deserialize)]
pub struct EventsQuery {
    pub driver_cd: Option<String>,
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
    pub page_size: Option<i64>,
    pub after_driver_cd: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EtagsQuery {
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EventsDriver {
    pub cd: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EventsPeriod {
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
}

/// 1 運行分の生の CSV。
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EventsOperationJson {
    pub unko_no: String,
    pub crew_role: i32,
    pub departure_at: Option<DateTime<Utc>>,
    pub return_at: Option<DateTime<Utc>>,
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EventsDriverGroup {
    pub driver: EventsDriver,
    pub operations: Vec<EventsOperationJson>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EventsSingleResponse {
    pub driver: EventsDriver,
    pub period: EventsPeriod,
    pub operations: Vec<EventsOperationJson>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EventsAllResponse {
    pub period: EventsPeriod,
    pub drivers: Vec<EventsDriverGroup>,
    pub next_after_driver_cd: Option<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(untagged)]
pub enum EventsResponse {
    Single(Box<EventsSingleResponse>),
    All(Box<EventsAllResponse>),
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EtagItem {
    pub unko_no: String,
    pub etag: Option<String>,
    pub driver_cds: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct UnsplitOperation {
    pub unko_no: String,
    pub driver_cd: String,
    pub reading_date: NaiveDate,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EtagsResponse {
    pub period: EventsPeriod,
    pub items: Vec<EtagItem>,
    pub unsplit: Vec<UnsplitOperation>,
    pub unsplit_total: usize,
    pub warnings: Vec<String>,
}

/// 失敗 (rust の `ComputeError` と同じ 3 種。応答は status と本文の文字列)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventsError {
    BadRequest(String),
    NotFound(String),
    /// 本文は `internal error` だけ (原因はログ)
    Internal,
}

fn db_failure(state: &DtakoState, what: &str, e: &tokio_postgres::Error) -> EventsError {
    let kind = alc_worker_db::kind(e);
    (state.log)(LogLevel::Error, &format!("{what} failed: db ({kind})"));
    EventsError::Internal
}

/// 期間の検査。逆転と上限超えは 400。
pub fn validate_range(from: NaiveDate, to: NaiveDate, max_days: i64) -> Result<(), EventsError> {
    if from > to {
        return Err(EventsError::BadRequest("date_from > date_to".to_owned()));
    }
    let span_days = (to - from).num_days() + 1;
    if span_days > max_days {
        return Err(EventsError::BadRequest(format!(
            "range too wide: {span_days} days (max {max_days})"
        )));
    }
    Ok(())
}

/// 運行の列挙は暦日を跨ぐ運行を落とさないよう前後 1 日ずつ広げる (rust の repo と同じ)。
fn widened(from: NaiveDate, to: NaiveDate) -> (NaiveDate, NaiveDate) {
    (from - Duration::days(1), to + Duration::days(1))
}

/// KUDGIVT.csv の key (rust の `build_kudgivt_key` と同じ)。
pub fn kudgivt_key(tenant_id: Uuid, unko_no: &str, r2_key_prefix: Option<&str>) -> String {
    match r2_key_prefix {
        Some(p) => format!("{}/KUDGIVT.csv", p.trim_end_matches('/')),
        None => format!("{tenant_id}/unko/{unko_no}/KUDGIVT.csv"),
    }
}

/// CSV → `(headers, rows)`。素朴な `,` の split (KUDGIVT は引用符の無い固定の形)。空行は落とす。
pub fn parse_csv(text: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let mut lines = text.lines();
    let first = lines.next().unwrap_or("");
    let headers = first.split(',').map(|h| h.trim().to_owned()).collect();
    let rows = lines
        .filter(|l| !l.trim().is_empty())
        .map(|line| line.split(',').map(|f| f.trim().to_owned()).collect())
        .collect();
    (headers, rows)
}

/// 読む key の重複を除いた採番 (同じ運行に相乗りした 2 人分でも 1 回だけ読む)。
#[derive(Default)]
struct FetchPlan {
    keys: Vec<String>,
    index: HashMap<String, usize>,
}

impl FetchPlan {
    fn intern(&mut self, key: String) -> usize {
        if let Some(i) = self.index.get(&key) {
            return *i;
        }
        let i = self.keys.len();
        self.index.insert(key.clone(), i);
        self.keys.push(key);
        i
    }

    fn planned(&mut self, tenant_id: Uuid, op: EventsOperation) -> PlannedOp {
        let key = kudgivt_key(tenant_id, &op.unko_no, op.r2_key_prefix.as_deref());
        PlannedOp {
            key_idx: self.intern(key),
            op,
        }
    }
}

struct PlannedOp {
    op: EventsOperation,
    key_idx: usize,
}

/// key を同時 [`GET_CONCURRENCY`] 本で読む。戻りは入力と同じ順で、読めない理由は固定の語。
async fn fetch_all(state: &DtakoState, keys: Vec<String>) -> Vec<Result<Vec<u8>, String>> {
    let store = &state.store;
    let mut fetched: Vec<(usize, Result<Vec<u8>, String>)> =
        stream::iter(keys.into_iter().enumerate())
            .map(|(i, key)| async move {
                let got = store.get(&key).await;
                let res = match got {
                    Ok(Some(bytes)) => Ok(bytes),
                    Ok(None) => Err("not found".to_owned()),
                    Err(e) => Err(e.to_string()),
                };
                (i, res)
            })
            .buffer_unordered(GET_CONCURRENCY)
            .collect()
            .await;
    fetched.sort_by_key(|(i, _)| *i);
    fetched.into_iter().map(|(_, r)| r).collect()
}

/// 読んだ結果を運行ごとに組み立てる。読めなかった運行は warning に落とす。出発の早い順 (同じなら運行NO の順)。
fn assemble(
    planned: Vec<PlannedOp>,
    fetched: &[Result<Vec<u8>, String>],
) -> (Vec<EventsOperationJson>, Vec<String>) {
    let mut operations = Vec::with_capacity(planned.len());
    let mut warnings = Vec::new();
    for p in planned {
        match &fetched[p.key_idx] {
            Ok(bytes) => {
                // 分割の出力は UTF-8。古い Shift_JIS のまま置かれた分のために fallback を残す (rust と同じ)
                let text = alc_csv_parser::decode_utf8_or_shift_jis(bytes);
                let (headers, rows) = parse_csv(&text);
                operations.push(EventsOperationJson {
                    unko_no: p.op.unko_no,
                    crew_role: p.op.crew_role,
                    departure_at: p.op.departure_at,
                    return_at: p.op.return_at,
                    headers,
                    rows,
                });
            }
            Err(e) => warnings.push(format!("{}: KUDGIVT 取得失敗 ({e})", p.op.unko_no)),
        }
    }
    operations.sort_by(|a, b| {
        a.departure_at
            .cmp(&b.departure_at)
            .then_with(|| a.unko_no.cmp(&b.unko_no))
    });
    warnings.sort();
    (operations, warnings)
}

/// `GET /dtako/events`。
pub async fn collect_events(
    state: &DtakoState,
    tenant_id: Uuid,
    q: EventsQuery,
) -> Result<EventsResponse, EventsError> {
    let period = EventsPeriod {
        date_from: q.date_from,
        date_to: q.date_to,
    };
    match q.driver_cd.clone() {
        Some(driver_cd) => {
            validate_range(q.date_from, q.date_to, MAX_RANGE_DAYS_SINGLE)?;
            if q.page_size.is_some() || q.after_driver_cd.is_some() {
                return Err(EventsError::BadRequest(
                    "page_size / after_driver_cd are only valid without driver_cd".to_owned(),
                ));
            }
            let resp = collect_single(state, tenant_id, driver_cd, period, &q).await?;
            Ok(EventsResponse::Single(Box::new(resp)))
        }
        None => {
            validate_range(q.date_from, q.date_to, MAX_RANGE_DAYS_ALL)?;
            let resp = collect_all(state, tenant_id, period, &q).await?;
            Ok(EventsResponse::All(Box::new(resp)))
        }
    }
}

async fn collect_single(
    state: &DtakoState,
    tenant_id: Uuid,
    driver_cd: String,
    period: EventsPeriod,
    q: &EventsQuery,
) -> Result<EventsSingleResponse, EventsError> {
    let (from, to) = widened(q.date_from, q.date_to);
    let found = {
        let mut client = state.pg.lock().await;
        pg::events_driver(&mut client, tenant_id, driver_cd.clone()).await
    };
    let found = found.map_err(|e| db_failure(state, "dtako-events driver", &e))?;
    let not_found = || EventsError::NotFound(format!("driver_cd not found: {driver_cd}"));
    let (driver_id, driver_name) = found.ok_or_else(not_found)?;
    let ops = {
        let mut client = state.pg.lock().await;
        pg::events_operations(&mut client, tenant_id, driver_id, from, to).await
    };
    let ops = ops.map_err(|e| db_failure(state, "dtako-events operations", &e))?;
    let mut plan = FetchPlan::default();
    let planned: Vec<PlannedOp> = ops
        .into_iter()
        .map(|op| plan.planned(tenant_id, op))
        .collect();
    let fetched = fetch_all(state, plan.keys).await;
    let (operations, warnings) = assemble(planned, &fetched);
    Ok(EventsSingleResponse {
        driver: EventsDriver {
            cd: driver_cd,
            name: driver_name,
        },
        period,
        operations,
        warnings,
    })
}

async fn collect_all(
    state: &DtakoState,
    tenant_id: Uuid,
    period: EventsPeriod,
    q: &EventsQuery,
) -> Result<EventsAllResponse, EventsError> {
    let (from, to) = widened(q.date_from, q.date_to);
    let page_size = q
        .page_size
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let drivers = {
        let mut client = state.pg.lock().await;
        let after = q.after_driver_cd.clone();
        pg::events_drivers(&mut client, tenant_id, from, to, after, page_size).await
    };
    let drivers = drivers.map_err(|e| db_failure(state, "dtako-events drivers", &e))?;
    // 1 ページ分ちょうど返ったときだけ次のページがあり得る
    let next_after_driver_cd = match drivers.len() as i64 == page_size {
        true => drivers.last().map(|d| d.driver_cd.clone()),
        false => None,
    };
    let driver_ids: Vec<Uuid> = drivers.iter().map(|d| d.driver_id).collect();
    let rows = {
        let mut client = state.pg.lock().await;
        pg::events_operations_for_drivers(&mut client, tenant_id, driver_ids.clone(), from, to)
            .await
    };
    let rows = rows.map_err(|e| db_failure(state, "dtako-events operations", &e))?;
    let slot: HashMap<Uuid, usize> = driver_ids
        .iter()
        .enumerate()
        .map(|(i, id)| (*id, i))
        .collect();
    let mut plan = FetchPlan::default();
    let mut per_driver: Vec<Vec<PlannedOp>> = drivers.iter().map(|_| Vec::new()).collect();
    for row in rows {
        // 直前の文を driver_ids で絞っているので必ず引ける (引けない行は飛ばす)
        if let Some(i) = slot.get(&row.driver_id).copied() {
            per_driver[i].push(plan.planned(tenant_id, row));
        }
    }
    let fetched = fetch_all(state, plan.keys).await;
    let mut groups = Vec::with_capacity(drivers.len());
    let mut warnings = Vec::new();
    for (driver, planned) in drivers.into_iter().zip(per_driver) {
        let (operations, warns) = assemble(planned, &fetched);
        warnings.extend(warns);
        groups.push(EventsDriverGroup {
            driver: EventsDriver {
                cd: driver.driver_cd,
                name: driver.driver_name,
            },
            operations,
        });
    }
    // 同じ運行が複数の乗務員に紐づくと同じ warning が並ぶのでまとめる
    warnings.sort();
    warnings.dedup();
    Ok(EventsAllResponse {
        period,
        drivers: groups,
        next_after_driver_cd,
        warnings,
    })
}

/// 運行NO の先頭 6 文字 (`YYMMDD`) から日ごとの LIST の prefix を作る。1 つでも数字の 6 文字が取れなければ `None`
/// (呼び手はテナント全体の LIST 1 本に倒す)。
pub fn derive_day_prefixes(unko_nos: &[String]) -> Option<Vec<String>> {
    let mut days: Vec<&str> = Vec::with_capacity(unko_nos.len());
    for unko_no in unko_nos {
        let day = unko_no.get(0..6)?;
        if !day.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        days.push(day);
    }
    days.sort_unstable();
    days.dedup();
    Some(days.into_iter().map(str::to_owned).collect())
}

/// LIST を投げる prefix。通常は日ごとの `{base}{YYMMDD}`、日で絞れなければ `base` 1 本 (遅いが正しい)。
fn list_targets(state: &DtakoState, base: &str, unko_nos: &[String]) -> Vec<String> {
    match derive_day_prefixes(unko_nos) {
        Some(days) => days.into_iter().map(|day| format!("{base}{day}")).collect(),
        None => {
            let message = "dtako-events-etags: day prefixes unavailable, listing the whole tenant";
            (state.log)(LogLevel::Warn, message);
            vec![base.to_owned()]
        }
    }
}

/// prefix ごとに LIST を同時 [`GET_CONCURRENCY`] 本で投げて 1 本にまとめる。1 つでも失敗したら 500
/// (一部だけ返すと呼び手の月の指紋が「消えた運行」を誤って検出するため)。
async fn list_all(
    state: &DtakoState,
    prefixes: Vec<String>,
) -> Result<Vec<ListedObject>, EventsError> {
    let store = &state.store;
    let pages: Vec<_> = stream::iter(prefixes)
        .map(|prefix| async move { store.list(&prefix).await })
        .buffer_unordered(GET_CONCURRENCY)
        .collect()
        .await;
    let mut all = Vec::new();
    for page in pages {
        match page {
            Ok(objects) => all.extend(objects),
            Err(e) => {
                (state.log)(LogLevel::Error, &format!("dtako-events-etags failed: {e}"));
                return Err(EventsError::Internal);
            }
        }
    }
    Ok(all)
}

/// `GET /dtako/events/etags`。
pub async fn collect_etags(
    state: &DtakoState,
    tenant_id: Uuid,
    q: EtagsQuery,
) -> Result<EtagsResponse, EventsError> {
    validate_range(q.date_from, q.date_to, MAX_RANGE_DAYS_ETAGS)?;
    let (from, to) = widened(q.date_from, q.date_to);
    let drivers = {
        let mut client = state.pg.lock().await;
        pg::events_drivers(
            &mut client,
            tenant_id,
            from,
            to,
            None,
            ALL_DRIVERS_UNLIMITED,
        )
        .await
    };
    let drivers = drivers.map_err(|e| db_failure(state, "dtako-events-etags drivers", &e))?;
    let cd_by_id: HashMap<Uuid, String> = drivers
        .iter()
        .map(|d| (d.driver_id, d.driver_cd.clone()))
        .collect();
    let driver_ids: Vec<Uuid> = drivers.iter().map(|d| d.driver_id).collect();
    let rows = {
        let mut client = state.pg.lock().await;
        pg::events_operations_for_drivers(&mut client, tenant_id, driver_ids, from, to).await
    };
    let rows = rows.map_err(|e| db_failure(state, "dtako-events-etags operations", &e))?;

    // 運行NO → 乗務員CD の集合 (運転手と副運転手で別の乗務員に紐づくことがある)
    let mut cds_by_unko: HashMap<String, Vec<String>> = HashMap::new();
    let mut unko_nos: Vec<String> = Vec::with_capacity(rows.len());
    for row in rows {
        let cds = cds_by_unko.entry(row.unko_no.clone()).or_default();
        cds.extend(cd_by_id.get(&row.driver_id).cloned());
        unko_nos.push(row.unko_no);
    }
    unko_nos.sort();
    unko_nos.dedup();
    for cds in cds_by_unko.values_mut() {
        cds.sort();
        cds.dedup();
    }

    // 保存先: 運行が在る日だけ LIST し、`{base}{unko_no}/KUDGIVT.csv` の ETag を拾う (運行が無ければ LIST しない)
    let base = format!("{tenant_id}/unko/");
    let mut etags: HashMap<String, String> = HashMap::new();
    if !unko_nos.is_empty() {
        let targets = list_targets(state, &base, &unko_nos);
        for object in list_all(state, targets).await? {
            let rest = object.key.strip_prefix(&base).unwrap_or_default();
            let Some(unko_no) = rest.strip_suffix("/KUDGIVT.csv") else {
                continue;
            };
            if let Some(etag) = object.etag {
                etags.insert(unko_no.to_owned(), etag);
            }
        }
    }

    // 突き合わせ: DB に在る運行NO だけを、ETag (LIST に無ければ null) と組にする
    let items = unko_nos
        .into_iter()
        .map(|unko_no| EtagItem {
            etag: etags.get(&unko_no).cloned(),
            driver_cds: cds_by_unko.remove(&unko_no).unwrap_or_default(),
            unko_no,
        })
        .collect();

    // 未分割の運行 (上限で切り、総数は切る前の数)
    let unsplit_rows = {
        let mut client = state.pg.lock().await;
        pg::unsplit_operations(&mut client, tenant_id, from, to).await
    };
    let mut unsplit_rows =
        unsplit_rows.map_err(|e| db_failure(state, "dtako-events-etags unsplit", &e))?;
    unsplit_rows.sort_by(|a, b| a.unko_no.cmp(&b.unko_no));
    let unsplit_total = unsplit_rows.len();
    unsplit_rows.truncate(UNSPLIT_DISPLAY_LIMIT);
    let unsplit = unsplit_rows
        .into_iter()
        .map(|r| UnsplitOperation {
            unko_no: r.unko_no,
            driver_cd: r.driver_cd,
            reading_date: r.reading_date,
        })
        .collect();
    let mut warnings = Vec::new();
    if unsplit_total > 0 {
        warnings.push(format!(
            "未 split の運行 {unsplit_total} 件 (has_kudgivt=FALSE、この期間のデータから欠けています)"
        ));
    }
    Ok(EtagsResponse {
        period: EventsPeriod {
            date_from: q.date_from,
            date_to: q.date_to,
        },
        items,
        unsplit,
        unsplit_total,
        warnings,
    })
}
