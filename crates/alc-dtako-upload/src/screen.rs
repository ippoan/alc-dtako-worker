//! 画面向けの読み取りと分類の更新 (W5c-1、Refs ippoan/rust-alc-api#680)。
//!
//! - `GET /drivers`・`GET /vehicles` → 乗務員・車両の一覧
//! - `GET /work-times`・`GET /daily-hours` → 日別のセグメント・労働時間のページ (`GET /daily-hours/{driver_id}/{date}/segments` で 1 日ぶん)
//! - `GET /operations`・`GET /internal/operations` (同じ口) → 運行の一覧のページ。`GET /operations/calendar` → 読取日ごとの件数
//! - `GET /operations/{unko_no}` → 運行の行 (crew_role ごと)。`DELETE /operations/{unko_no}` → 消して変更記録 (`manual_delete`) に残す
//! - `GET /operations/{unko_no}/csv/{csv_type}` → 保存先の CSV を `{headers, rows}` にして返す
//! - `GET /dtako/operation-changes` → 乗務員の運行の変更記録 (期間は運行の日付で絞る)
//! - `GET /event-classifications`・`PUT /event-classifications/{id}` → イベントの分類の一覧と更新
//! - `GET /scraper/history`・`POST /scraper/history` → スクレイプの履歴の一覧と記録
//! - `GET /dtako-logs/{current,current/select,by-date,by-date-range}` → 車両の現在地のログ (範囲は保存先の古いアーカイブも読む)
//!
//! **応答の形・既定値・上限・失敗の status は rust-alc-api の同じ口と同じ** (送り手を変えずに振り分けを切り替えるため)。
//! 違いは 2 つ: 失敗の本文に DB のエラー文を載せない (rust の `scraper/history` は載せていた)・DB はテナントでも絞って引く。
//! 原因は `log` に、段の名前と kind だけを出す。

use std::collections::HashMap;
use std::io::Read;

use alc_core_wasm::TenantId;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::pg::{
    self, CalendarDate, DailyHoursRow, DailySegmentRow, DayFilter, DtakologQuery, DtakologRow,
    OperationChangeRow, OperationFilter, OperationListRow, WorkTimeRow,
};
use crate::routes::DtakoState;
use crate::split::LogLevel;

/// ページの既定の件数と上限 (rust-alc-api と同じ)。
pub const DEFAULT_PER_PAGE: i64 = 50;
pub const MAX_PER_PAGE: i64 = 200;

/// スクレイプの履歴の既定の件数 (rust-alc-api と同じ。上限は無い)。
pub const DEFAULT_HISTORY_LIMIT: i64 = 50;

/// イベントの分類に使える値 (rust-alc-api と同じ)。
pub const CLASSIFICATIONS: [&str; 5] = ["drive", "cargo", "rest_split", "break", "ignore"];

/// 車両の現在地のログのアーカイブの目録 (保存先の key。テナント → 日付 → 置き場)。
pub const ARCHIVE_MANIFEST_KEY: &str = "archive/alc_api/dtakologs/_manifest.json";

pub fn router() -> Router<DtakoState> {
    Router::new()
        .route("/drivers", get(drivers))
        .route("/vehicles", get(vehicles))
        .route("/work-times", get(work_times))
        .route("/daily-hours", get(daily_hours))
        .route(
            "/daily-hours/{driver_id}/{date}/segments",
            get(daily_segments),
        )
        .route("/operations", get(operations))
        .route("/internal/operations", get(operations))
        .route("/operations/calendar", get(calendar))
        .route(
            "/operations/{unko_no}",
            get(operation).delete(delete_operation),
        )
        .route("/operations/{unko_no}/csv/{csv_type}", get(operation_csv))
        .route("/dtako/operation-changes", get(operation_changes))
        .route("/event-classifications", get(event_classifications))
        .route(
            "/event-classifications/{id}",
            axum::routing::put(update_event_classification),
        )
        .route(
            "/scraper/history",
            get(scrape_history).post(save_scrape_history),
        )
        .route("/dtako-logs/current", get(dtakologs_current))
        .route("/dtako-logs/current/select", get(dtakologs_select))
        .route("/dtako-logs/by-date", get(dtakologs_at))
        .route("/dtako-logs/by-date-range", get(dtakologs_range))
}

/// DB の失敗を 500 にし、段の名前と kind だけを `log` に出す。
fn db_failed(state: &DtakoState, what: &str, e: &tokio_postgres::Error) -> StatusCode {
    let kind = alc_worker_db::kind(e);
    (state.log)(LogLevel::Error, &format!("{what} failed: db ({kind})"));
    StatusCode::INTERNAL_SERVER_ERROR
}

/// ページの番号と件数を整える (1 未満は 1、件数は上限で切る)。返すのは (page, per_page, 飛ばす件数)。
pub fn paging(page: Option<i64>, per_page: Option<i64>) -> (i64, i64, i64) {
    let page = page.unwrap_or(1).max(1);
    let per_page = per_page.unwrap_or(DEFAULT_PER_PAGE).min(MAX_PER_PAGE);
    (page, per_page, (page - 1) * per_page)
}

/// 1 ページぶんの応答 (`items` の名前は口ごと。rust-alc-api の `*Response` の形)。
#[derive(Debug, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

async fn drivers(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<pg::DriverRow>>, StatusCode> {
    let mut client = state.pg.lock().await;
    pg::list_drivers(&mut client, tenant_id)
        .await
        .map(Json)
        .map_err(|e| db_failed(&state, "drivers", &e))
}

async fn vehicles(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<pg::VehicleRow>>, StatusCode> {
    let mut client = state.pg.lock().await;
    pg::list_vehicles(&mut client, tenant_id)
        .await
        .map(Json)
        .map_err(|e| db_failed(&state, "vehicles", &e))
}

/// `GET /work-times`・`GET /daily-hours` の絞り込み。
#[derive(Debug, Deserialize)]
pub struct DayQuery {
    pub driver_id: Option<Uuid>,
    pub date_from: Option<NaiveDate>,
    pub date_to: Option<NaiveDate>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

impl DayQuery {
    fn filter(&self) -> (i64, i64, DayFilter) {
        let (page, per_page, offset) = paging(self.page, self.per_page);
        let f = DayFilter {
            driver_id: self.driver_id,
            date_from: self.date_from,
            date_to: self.date_to,
            limit: per_page,
            offset,
        };
        (page, per_page, f)
    }
}

async fn work_times(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<DayQuery>,
) -> Result<Json<Page<WorkTimeRow>>, StatusCode> {
    let (page, per_page, f) = q.filter();
    let mut client = state.pg.lock().await;
    let (total, items) = pg::work_times(&mut client, tenant_id, f)
        .await
        .map_err(|e| db_failed(&state, "work-times", &e))?;
    Ok(Json(Page {
        items,
        total,
        page,
        per_page,
    }))
}

async fn daily_hours(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<DayQuery>,
) -> Result<Json<Page<DailyHoursRow>>, StatusCode> {
    let (page, per_page, f) = q.filter();
    let mut client = state.pg.lock().await;
    let (total, items) = pg::daily_hours(&mut client, tenant_id, f)
        .await
        .map_err(|e| db_failed(&state, "daily-hours", &e))?;
    Ok(Json(Page {
        items,
        total,
        page,
        per_page,
    }))
}

/// `GET /daily-hours/{driver_id}/{date}/segments` の応答。
#[derive(Debug, Serialize)]
pub struct SegmentsResponse {
    pub segments: Vec<DailySegmentRow>,
}

async fn daily_segments(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path((driver_id, date)): Path<(Uuid, NaiveDate)>,
) -> Result<Json<SegmentsResponse>, StatusCode> {
    let mut client = state.pg.lock().await;
    let segments = pg::daily_segments(&mut client, tenant_id, driver_id, date)
        .await
        .map_err(|e| db_failed(&state, "daily-segments", &e))?;
    Ok(Json(SegmentsResponse { segments }))
}

/// `GET /operations` の絞り込み。
#[derive(Debug, Deserialize)]
pub struct OperationsQuery {
    pub date_from: Option<NaiveDate>,
    pub date_to: Option<NaiveDate>,
    pub driver_cd: Option<String>,
    pub vehicle_cd: Option<String>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// `GET /operations` の応答 (一覧の名前は `operations`)。
#[derive(Debug, Serialize)]
pub struct OperationsResponse {
    pub operations: Vec<OperationListRow>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

async fn operations(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<OperationsQuery>,
) -> Result<Json<OperationsResponse>, StatusCode> {
    let (page, per_page, offset) = paging(q.page, q.per_page);
    let f = OperationFilter {
        date_from: q.date_from,
        date_to: q.date_to,
        driver_cd: q.driver_cd,
        vehicle_cd: q.vehicle_cd,
        limit: per_page,
        offset,
    };
    let mut client = state.pg.lock().await;
    let (total, operations) = pg::list_operations(&mut client, tenant_id, f)
        .await
        .map_err(|e| db_failed(&state, "operations", &e))?;
    Ok(Json(OperationsResponse {
        operations,
        total,
        page,
        per_page,
    }))
}

/// `GET /operations/calendar` の問い。
#[derive(Debug, Deserialize)]
pub struct CalendarQuery {
    pub year: i32,
    pub month: i32,
}

/// `GET /operations/calendar` の応答。
#[derive(Debug, Serialize)]
pub struct CalendarResponse {
    pub year: i32,
    pub month: u32,
    pub dates: Vec<CalendarDate>,
}

/// 月の初日と末日 (月が範囲外なら `None`。rust-alc-api と同じく月は `u32` に読み替える)。
pub fn month_range(year: i32, month: i32) -> Option<(NaiveDate, NaiveDate)> {
    let month = month as u32;
    let first = NaiveDate::from_ymd_opt(year, month, 1)?;
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let next = NaiveDate::from_ymd_opt(next_year, next_month, 1)?;
    Some((first, next.pred_opt()?))
}

async fn calendar(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<CalendarQuery>,
) -> Result<Json<CalendarResponse>, StatusCode> {
    let (from, to) = month_range(q.year, q.month).ok_or(StatusCode::BAD_REQUEST)?;
    let mut client = state.pg.lock().await;
    let dates = pg::calendar_dates(&mut client, tenant_id, from, to)
        .await
        .map_err(|e| db_failed(&state, "operations-calendar", &e))?;
    Ok(Json(CalendarResponse {
        year: q.year,
        month: q.month as u32,
        dates,
    }))
}

async fn operation(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(unko_no): Path<String>,
) -> Result<Json<Vec<pg::OperationRow>>, StatusCode> {
    let mut client = state.pg.lock().await;
    let rows = pg::operations_by_unko_no(&mut client, tenant_id, unko_no)
        .await
        .map_err(|e| db_failed(&state, "operation", &e))?;
    if rows.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(rows))
}

async fn delete_operation(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(unko_no): Path<String>,
) -> StatusCode {
    let mut client = state.pg.lock().await;
    match pg::delete_operations(&mut client, tenant_id, unko_no).await {
        Ok(0) => StatusCode::NOT_FOUND,
        Ok(_) => StatusCode::NO_CONTENT,
        Err(e) => db_failed(&state, "operation-delete", &e),
    }
}

/// `GET /operations/{unko_no}/csv/{csv_type}` の応答。
#[derive(Debug, Serialize)]
pub struct CsvJsonResponse {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// CSV の種類 → 保存先のファイル名 (大文字小文字は問わない。rust-alc-api と同じ別名)。
pub fn csv_filename(csv_type: &str) -> Option<&'static str> {
    match csv_type.to_lowercase().as_str() {
        "kudguri" => Some("KUDGURI.csv"),
        "kudgivt" | "events" => Some("KUDGIVT.csv"),
        "kudgfry" | "ferry" | "ferries" => Some("KUDGFRY.csv"),
        "kudgsir" | "tolls" => Some("KUDGSIR.csv"),
        "speed" | "sokudo" => Some("SOKUDODATA.csv"),
        _ => None,
    }
}

/// CSV の文字列を見出しと行に分ける (`,` で切って前後の空白を落とす。空の行は飛ばす。rust-alc-api と同じ)。
pub fn split_csv(text: &str) -> CsvJsonResponse {
    let mut lines = text.lines();
    let cells =
        |line: &str| -> Vec<String> { line.split(',').map(|f| f.trim().to_string()).collect() };
    let headers = cells(lines.next().unwrap_or(""));
    let rows = lines.filter(|l| !l.trim().is_empty()).map(cells).collect();
    CsvJsonResponse { headers, rows }
}

async fn operation_csv(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path((unko_no, csv_type)): Path<(String, String)>,
) -> Result<Json<CsvJsonResponse>, StatusCode> {
    let filename = csv_filename(&csv_type).ok_or(StatusCode::BAD_REQUEST)?;
    let prefix = {
        let mut client = state.pg.lock().await;
        pg::r2_key_prefix(&mut client, tenant_id, unko_no.clone())
            .await
            .map_err(|e| db_failed(&state, "operation-csv", &e))?
    };
    let key = match prefix {
        Some(prefix) => format!("{prefix}/{filename}"),
        None => format!("{tenant_id}/unko/{unko_no}/{filename}"),
    };
    // 無いのも読めないのも 404 (rust-alc-api と同じ)
    let bytes = match state.store.get(&key).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Err(StatusCode::NOT_FOUND),
        Err(e) => {
            (state.log)(
                LogLevel::Error,
                &format!("operation-csv failed: store ({})", e.stage()),
            );
            return Err(StatusCode::NOT_FOUND);
        }
    };
    Ok(Json(split_csv(&String::from_utf8_lossy(&bytes))))
}

/// 運行の日付。運行NO の先頭 6 桁 (YYMMDD) を正とし、読めなければ before / after の `departure_at` の日付 (rust-alc-api と同じ)。
pub fn operation_date_of(row: &OperationChangeRow) -> Option<NaiveDate> {
    let from_unko = row
        .unko_no
        .get(..6)
        .and_then(|p| NaiveDate::parse_from_str(&format!("20{p}"), "%Y%m%d").ok());
    let from_departure = || {
        [row.before.as_ref(), row.after.as_ref()]
            .into_iter()
            .flatten()
            .filter_map(|v| v.get("departure_at").and_then(|d| d.as_str()))
            .find_map(|d| NaiveDate::parse_from_str(d.get(..10)?, "%Y-%m-%d").ok())
    };
    from_unko.or_else(from_departure)
}

/// `GET /dtako/operation-changes` の問い。
#[derive(Debug, Deserialize)]
pub struct OperationChangesQuery {
    pub driver_cd: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
}

/// `GET /dtako/operation-changes` の応答。
#[derive(Debug, Serialize)]
pub struct OperationChangesResponse {
    pub driver_cd: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    /// 記録を始めた時点 (表の最古の記録時刻)。これより前の変更は記録に無い
    pub recording_since: Option<DateTime<Utc>>,
    pub changes: Vec<OperationChangeRow>,
}

async fn operation_changes(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<OperationChangesQuery>,
) -> Result<Json<OperationChangesResponse>, StatusCode> {
    if q.from > q.to {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut client = state.pg.lock().await;
    let (rows, recording_since) =
        pg::operation_changes(&mut client, tenant_id, q.driver_cd.clone())
            .await
            .map_err(|e| db_failed(&state, "operation-changes", &e))?;
    let changes = rows
        .into_iter()
        .filter(|r| operation_date_of(r).is_some_and(|d| d >= q.from && d <= q.to))
        .collect();
    Ok(Json(OperationChangesResponse {
        driver_cd: q.driver_cd,
        from: q.from,
        to: q.to,
        recording_since,
        changes,
    }))
}

async fn event_classifications(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<pg::EventClassificationRow>>, StatusCode> {
    let mut client = state.pg.lock().await;
    pg::list_event_classifications(&mut client, tenant_id)
        .await
        .map(Json)
        .map_err(|e| db_failed(&state, "event-classifications", &e))
}

/// `PUT /event-classifications/{id}` の本文。
#[derive(Debug, Deserialize)]
pub struct UpdateClassification {
    pub classification: String,
}

async fn update_event_classification(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateClassification>,
) -> Result<Json<pg::EventClassificationRow>, (StatusCode, String)> {
    if !CLASSIFICATIONS.contains(&body.classification.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Invalid classification '{}'. Must be one of: {}",
                body.classification,
                CLASSIFICATIONS.join(", ")
            ),
        ));
    }
    let mut client = state.pg.lock().await;
    let row = pg::update_event_classification(&mut client, tenant_id, id, body.classification)
        .await
        .map_err(|e| {
            (
                db_failed(&state, "event-classification-update", &e),
                "internal server error".to_string(),
            )
        })?;
    row.map(Json)
        .ok_or((StatusCode::NOT_FOUND, "Not found".to_string()))
}

/// `GET /scraper/history` の問い。
#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    #[serde(default = "default_history_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_history_limit() -> i64 {
    DEFAULT_HISTORY_LIMIT
}

/// `POST /scraper/history` の本文 (1 社ぶんのスクレイプの結果)。
#[derive(Debug, Deserialize)]
pub struct ScrapeHistoryEntry {
    pub target_date: NaiveDate,
    pub comp_id: String,
    pub status: String,
    #[serde(default)]
    pub message: Option<String>,
}

async fn scrape_history(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<HistoryQuery>,
) -> Result<Json<Vec<pg::ScrapeHistoryRow>>, (StatusCode, String)> {
    let mut client = state.pg.lock().await;
    pg::list_scrape_history(&mut client, tenant_id, q.limit, q.offset)
        .await
        .map(Json)
        .map_err(|e| {
            (
                db_failed(&state, "scraper-history", &e),
                "internal server error".to_string(),
            )
        })
}

async fn save_scrape_history(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(entry): Json<ScrapeHistoryEntry>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut client = state.pg.lock().await;
    pg::insert_scrape_history(
        &mut client,
        tenant_id,
        entry.target_date,
        entry.comp_id,
        entry.status,
        entry.message,
    )
    .await
    .map_err(|e| {
        (
            db_failed(&state, "scraper-history-save", &e),
            "internal server error".to_string(),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- 車両の現在地のログ ----

/// 画面の地図が読む形 (PascalCase。rust-alc-api の `DtakologView`)。
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DtakologView {
    #[serde(rename = "GPSDirection")]
    pub gps_direction: f64,
    #[serde(rename = "GPSLatitude")]
    pub gps_latitude: f64,
    #[serde(rename = "GPSLongitude")]
    pub gps_longitude: f64,
    #[serde(rename = "VehicleCD")]
    pub vehicle_cd: i32,
    pub vehicle_name: String,
    pub driver_name: Option<String>,
    #[serde(rename = "AddressDispC")]
    pub address_disp_c: Option<String>,
    pub data_date_time: String,
    #[serde(rename = "AddressDispP")]
    pub address_disp_p: Option<String>,
    #[serde(rename = "SubDriverCD")]
    pub sub_driver_cd: i32,
    pub all_state: String,
    pub recive_type_color_name: Option<String>,
    pub all_state_ex: Option<String>,
    pub state2: String,
    pub all_state_font_color: Option<String>,
    pub speed: serde_json::Value,
}

impl From<DtakologRow> for DtakologView {
    /// `State2` は運転・休憩・休息のときだけ載せ、速度は 0 なら空の文字列、それ以外は小数 1 桁 (rust-alc-api と同じ)。
    fn from(r: DtakologRow) -> Self {
        let all_state = r.all_state.unwrap_or_default();
        let state2 = if ["Drive", "Rest", "Break"].contains(&all_state.as_str()) {
            r.state2.unwrap_or_default()
        } else {
            String::new()
        };
        let speed = if r.speed == 0.0 {
            serde_json::Value::String(String::new())
        } else {
            serde_json::json!((r.speed as f64 * 10.0).round() / 10.0)
        };
        Self {
            gps_direction: r.gps_direction,
            gps_latitude: r.gps_latitude,
            gps_longitude: r.gps_longitude,
            vehicle_cd: r.vehicle_cd,
            vehicle_name: r.vehicle_name,
            driver_name: r.driver_name,
            address_disp_c: r.address_disp_c,
            data_date_time: r.data_date_time,
            address_disp_p: r.address_disp_p,
            sub_driver_cd: r.sub_driver_cd,
            all_state,
            recive_type_color_name: r.recive_type_color_name,
            all_state_ex: r.all_state_ex,
            state2,
            all_state_font_color: r.all_state_font_color,
            speed,
        }
    }
}

fn views(rows: Vec<DtakologRow>) -> Json<Vec<DtakologView>> {
    Json(rows.into_iter().map(DtakologView::from).collect())
}

async fn read_dtakologs(
    state: &DtakoState,
    tenant_id: Uuid,
    what: &str,
    q: DtakologQuery,
) -> Result<Vec<DtakologRow>, StatusCode> {
    let mut client = state.pg.lock().await;
    pg::dtakologs(&mut client, tenant_id, q)
        .await
        .map_err(|e| db_failed(state, what, &e))
}

async fn dtakologs_current(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<DtakologView>>, StatusCode> {
    let rows = read_dtakologs(
        &state,
        tenant_id,
        "dtako-logs-current",
        DtakologQuery::Current,
    )
    .await?;
    Ok(views(rows))
}

/// `GET /dtako-logs/current/select` の問い (車両CD は `,` 区切り。読めない欄は捨てる)。
#[derive(Debug, Deserialize)]
pub struct SelectQuery {
    pub address_disp_p: Option<String>,
    pub branch_cd: Option<i32>,
    pub vehicle_cds: Option<String>,
}

/// `,` 区切りの車両CD を読む (読めない欄は捨てる。1 つも無ければ絞らない = `None`)。
pub fn parse_vehicle_cds(raw: Option<&str>) -> Option<Vec<i32>> {
    let cds: Vec<i32> = raw
        .unwrap_or("")
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    (!cds.is_empty()).then_some(cds)
}

async fn dtakologs_select(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<SelectQuery>,
) -> Result<Json<Vec<DtakologView>>, StatusCode> {
    let query = DtakologQuery::CurrentSelect {
        vehicle_cds: parse_vehicle_cds(q.vehicle_cds.as_deref()),
        address_disp_p: q.address_disp_p,
        branch_cd: q.branch_cd,
    };
    let rows = read_dtakologs(&state, tenant_id, "dtako-logs-select", query).await?;
    Ok(views(rows))
}

/// `GET /dtako-logs/by-date` の問い。
#[derive(Debug, Deserialize)]
pub struct AtQuery {
    pub date_time: String,
    pub vehicle_cd: Option<i32>,
}

async fn dtakologs_at(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<AtQuery>,
) -> Result<Json<Vec<DtakologView>>, StatusCode> {
    let query = DtakologQuery::At {
        date_time: q.date_time,
        vehicle_cd: q.vehicle_cd,
    };
    let rows = read_dtakologs(&state, tenant_id, "dtako-logs-by-date", query).await?;
    Ok(views(rows))
}

/// `GET /dtako-logs/by-date-range` の問い。
#[derive(Debug, Deserialize)]
pub struct RangeQuery {
    pub start_date_time: String,
    pub end_date_time: String,
    pub vehicle_cd: Option<i32>,
}

/// 日時の文字列の日付の部分 (`YYYY-MM-DD…` なら先頭 10 文字、それ以外は先頭 10 文字をそのまま。rust-alc-api と同じ)。
pub fn date_part(datetime: &str) -> String {
    if datetime.len() >= 10 && datetime.as_bytes()[4] == b'-' {
        return datetime[..10].to_string();
    }
    datetime.chars().take(10).collect()
}

/// アーカイブの目録 (テナント → 日付 → 置き場)。
#[derive(Debug, Default, Deserialize)]
struct Manifest {
    #[serde(default)]
    archived_dates: HashMap<String, HashMap<String, ArchivedDate>>,
}

#[derive(Debug, Deserialize)]
struct ArchivedDate {
    #[serde(default)]
    r2_key: String,
}

/// アーカイブの 1 行 (JSON) を行の型に読む (欠けた欄は 0・空・NULL。rust-alc-api と同じ)。
pub fn archived_row(v: &serde_json::Value) -> DtakologRow {
    let f64_of = |k: &str| v.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
    let i32_of = |k: &str| v.get(k).and_then(|x| x.as_i64()).unwrap_or(0) as i32;
    let str_of = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_string);
    DtakologRow {
        gps_direction: f64_of("gps_direction"),
        gps_latitude: f64_of("gps_latitude"),
        gps_longitude: f64_of("gps_longitude"),
        vehicle_cd: i32_of("vehicle_cd"),
        vehicle_name: str_of("vehicle_name").unwrap_or_default(),
        driver_name: str_of("driver_name"),
        address_disp_c: str_of("address_disp_c"),
        data_date_time: str_of("data_date_time").unwrap_or_default(),
        address_disp_p: str_of("address_disp_p"),
        sub_driver_cd: i32_of("sub_driver_cd"),
        all_state: str_of("all_state"),
        recive_type_color_name: str_of("recive_type_color_name"),
        all_state_ex: str_of("all_state_ex"),
        state2: str_of("state2"),
        all_state_font_color: str_of("all_state_font_color"),
        speed: f64_of("speed") as f32,
    }
}

/// gzip の JSON Lines を行にする (見出しの行・読めない行・車両CD が合わない行は飛ばす)。展開に失敗したら `None`。
pub fn archived_rows(gz: &[u8], vehicle_cd: Option<i32>) -> Option<Vec<DtakologRow>> {
    let mut content = String::new();
    flate2::read::GzDecoder::new(gz)
        .read_to_string(&mut content)
        .ok()?;
    let rows = content
        .lines()
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|v| v.get("_archive_header").is_none())
        .filter(|v| {
            vehicle_cd
                .is_none_or(|vc| v.get("vehicle_cd").and_then(|x| x.as_i64()) == Some(vc as i64))
        })
        .map(|v| archived_row(&v))
        .collect();
    Some(rows)
}

/// 保存先のアーカイブから、テナントの日付の範囲の行を読む。目録が無い・読めない・テナントが無いは 0 行、
/// 日付ごとのファイルが読めない・展開できないはその日を飛ばす (どれも失敗にしない。rust-alc-api と同じ)。
pub async fn archived_dtakologs(
    state: &DtakoState,
    tenant_id: Uuid,
    start_date: &str,
    end_date: &str,
    vehicle_cd: Option<i32>,
) -> Vec<DtakologRow> {
    let manifest: Manifest = match state.store.get(ARCHIVE_MANIFEST_KEY).await {
        Ok(Some(bytes)) => serde_json::from_slice(&bytes).unwrap_or_default(),
        _ => return Vec::new(),
    };
    let Some(dates) = manifest.archived_dates.get(&tenant_id.to_string()) else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for (date, info) in dates {
        if date.as_str() < start_date || date.as_str() > end_date {
            continue;
        }
        let gz = match state.store.get(&info.r2_key).await {
            Ok(Some(gz)) => gz,
            _ => {
                (state.log)(LogLevel::Warn, "dtako-logs archive skipped: unreadable");
                continue;
            }
        };
        match archived_rows(&gz, vehicle_cd) {
            Some(day) => rows.extend(day),
            None => (state.log)(LogLevel::Warn, "dtako-logs archive skipped: not gzip"),
        }
    }
    rows
}

async fn dtakologs_range(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<RangeQuery>,
) -> Result<Json<Vec<DtakologView>>, StatusCode> {
    let start = date_part(&q.start_date_time);
    let end = date_part(&q.end_date_time);
    let query = DtakologQuery::Range {
        start: q.start_date_time,
        end: q.end_date_time,
        vehicle_cd: q.vehicle_cd,
    };
    let mut rows = read_dtakologs(&state, tenant_id, "dtako-logs-by-date-range", query).await?;
    rows.extend(archived_dtakologs(&state, tenant_id, &start, &end, q.vehicle_cd).await);
    rows.sort_by(|a, b| a.data_date_time.cmp(&b.data_date_time));
    rows.dedup_by(|a, b| a.data_date_time == b.data_date_time && a.vehicle_cd == b.vehicle_cd);
    Ok(views(rows))
}
