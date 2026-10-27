//! 車両のエラーの起票 (W5c-5、Refs ippoan/rust-alc-api#680)。表は migration 115 の `dtako_tickets`。
//!
//! - `POST /dtako/tickets` → 起票 (201)。email-receiver が内部用の proxy から呼ぶ
//! - `PATCH /dtako/tickets/{id}/scraped` → スクレイプの結果を反映して `scraped` に進める。同じく内部用
//! - `GET /dtako/tickets` → 一覧のページ (`status`・`vehicle_name` の部分一致で絞る)。`GET /dtako/tickets/{id}` → 1 件。画面用
//! - `POST /dtako/tickets/close` → close_token で閉じる (QR を読んだ画面から)。token からテナントを逆引きするので、呼び手のテナントでは絞らない
//!
//! **応答の形・既定値・上限・失敗の status は rust-alc-api の同じ口 (`alc_dtako::dtako_tickets`) と同じ** (失敗の本文は空)。
//! 違いは 2 つ: close_token は DB が作る (長さ・字種は同じ 64 桁の hex)・close も auth-worker を通ってテナント付きで届く
//! (rust は認証の無い口だった)。原因は `log` に、段の名前と kind だけを出す。

use alc_core_wasm::TenantId;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, patch, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::pg;
use crate::routes::DtakoState;
use crate::split::LogLevel;

/// 一覧の既定の件数と上限 (rust-alc-api と同じ)。
pub const DEFAULT_PER_PAGE: i64 = 50;
pub const MAX_PER_PAGE: i64 = 1000;

/// close_token の長さの上限 (rust-alc-api と同じ)。
pub const MAX_CLOSE_TOKEN_LEN: usize = 128;

pub fn router() -> Router<DtakoState> {
    Router::new()
        .route("/dtako/tickets", get(list).post(create))
        .route("/dtako/tickets/close", post(close))
        .route("/dtako/tickets/{id}", get(one))
        .route("/dtako/tickets/{id}/scraped", patch(scraped))
}

/// 起票 1 件 (rust-alc-api の `DtakoTicket`)。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Ticket {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub source: String,
    pub source_email_subject: Option<String>,
    pub source_email_from: Option<String>,
    pub source_email_message_id: Option<String>,
    pub source_email_received_at: DateTime<Utc>,
    pub vehicle_name: String,
    pub vehicle_code: Option<String>,
    pub error_kind: String,
    pub status: String,
    pub comp_id: Option<String>,
    pub unko_no: Option<String>,
    pub operation_started_at: Option<DateTime<Utc>>,
    pub operation_ended_at: Option<DateTime<Utc>>,
    pub scraped_payload: Option<Value>,
    pub settings_zip_r2_key: Option<String>,
    pub close_token: String,
    pub closed_at: Option<DateTime<Utc>>,
    pub closed_by: Option<String>,
    pub raw_email_text: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// `POST /dtako/tickets` の body (rust-alc-api の `DtakoTicketCreate`。`source` の既定は `email`)。
#[derive(Debug, Clone, Deserialize)]
pub struct TicketCreate {
    #[serde(default = "default_source")]
    pub source: String,
    pub source_email_subject: Option<String>,
    pub source_email_from: Option<String>,
    pub source_email_message_id: Option<String>,
    pub source_email_received_at: DateTime<Utc>,
    pub vehicle_name: String,
    pub vehicle_code: Option<String>,
    pub error_kind: String,
    pub raw_email_text: Option<String>,
}

fn default_source() -> String {
    "email".to_owned()
}

/// `PATCH /dtako/tickets/{id}/scraped` の body (null の欄は変えない)。
#[derive(Debug, Clone, Deserialize)]
pub struct TicketScrapedPatch {
    pub comp_id: Option<String>,
    pub unko_no: Option<String>,
    pub operation_started_at: Option<DateTime<Utc>>,
    pub operation_ended_at: Option<DateTime<Utc>>,
    pub settings_zip_r2_key: Option<String>,
    pub scraped_payload: Option<Value>,
}

/// `GET /dtako/tickets` の絞り込み。
#[derive(Debug, Deserialize)]
pub struct TicketFilter {
    pub status: Option<String>,
    pub vehicle_name: Option<String>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// 一覧の応答 (rust-alc-api の `DtakoTicketsResponse`)。
#[derive(Debug, Serialize)]
pub struct TicketsResponse {
    pub tickets: Vec<Ticket>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

/// `POST /dtako/tickets/close` の body。
#[derive(Debug, Deserialize)]
pub struct CloseRequest {
    pub close_token: String,
    pub closed_by: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CloseResponse {
    pub ticket_id: Uuid,
}

/// DB の失敗を 500 にし、段の名前と kind だけを `log` に出す。
fn db_failed(state: &DtakoState, what: &str, e: &tokio_postgres::Error) -> StatusCode {
    let kind = alc_worker_db::kind(e);
    (state.log)(
        LogLevel::Error,
        &format!("dtako tickets {what} failed: db ({kind})"),
    );
    StatusCode::INTERNAL_SERVER_ERROR
}

async fn create(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<TicketCreate>,
) -> Result<(StatusCode, Json<Ticket>), StatusCode> {
    if body.vehicle_name.trim().is_empty() || body.error_kind.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut client = state.pg.lock().await;
    let ticket = pg::insert_ticket(&mut client, tenant_id, body)
        .await
        .map_err(|e| db_failed(&state, "create", &e))?;
    Ok((StatusCode::CREATED, Json(ticket)))
}

async fn scraped(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<TicketScrapedPatch>,
) -> Result<Json<Ticket>, StatusCode> {
    let mut client = state.pg.lock().await;
    pg::update_ticket_scraped(&mut client, tenant_id, id, body)
        .await
        .map_err(|e| db_failed(&state, "scraped", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn list(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(filter): Query<TicketFilter>,
) -> Result<Json<TicketsResponse>, StatusCode> {
    let per_page = filter
        .per_page
        .unwrap_or(DEFAULT_PER_PAGE)
        .clamp(1, MAX_PER_PAGE);
    let page = filter.page.unwrap_or(1).max(1);
    let offset = (page - 1) * per_page;
    let mut client = state.pg.lock().await;
    let (total, tickets) = pg::list_tickets(
        &mut client,
        tenant_id,
        filter.status,
        filter.vehicle_name,
        per_page,
        offset,
    )
    .await
    .map_err(|e| db_failed(&state, "list", &e))?;
    Ok(Json(TicketsResponse {
        tickets,
        total,
        page,
        per_page,
    }))
}

async fn one(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Ticket>, StatusCode> {
    let mut client = state.pg.lock().await;
    pg::get_ticket(&mut client, tenant_id, id)
        .await
        .map_err(|e| db_failed(&state, "get", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn close(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CloseRequest>,
) -> Result<Json<CloseResponse>, StatusCode> {
    let token = body.close_token.trim();
    if token.is_empty() || token.len() > MAX_CLOSE_TOKEN_LEN {
        return Err(StatusCode::BAD_REQUEST);
    }
    let closed_by = body.closed_by.filter(|s| !s.is_empty());
    let mut client = state.pg.lock().await;
    pg::close_ticket_by_token(&mut client, tenant_id, token.to_owned(), closed_by)
        .await
        .map_err(|e| db_failed(&state, "close", &e))?
        .map(|ticket_id| Json(CloseResponse { ticket_id }))
        .ok_or(StatusCode::NOT_FOUND)
}
