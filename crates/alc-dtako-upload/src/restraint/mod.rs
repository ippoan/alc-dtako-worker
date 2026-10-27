//! 拘束時間の帳票 (W5c-3、Refs ippoan/rust-alc-api#680)。
//!
//! - `GET /restraint-report?driver_id=&year=&month=` → 乗務員 1 人の月の拘束時間管理表
//!   1. 乗務員の名前 ([`pg::restraint_driver_name`]。無ければ空の名前のまま続ける)
//!   2. 年月から読む範囲を決め ([`report::MonthRange`]。日付にならなければ 400 `invalid year/month`)
//!   3. 区間・日別・前日の運転・年度の累計・運行日ごとの始業と終業を 1 つの transaction で読み ([`pg::restraint_inputs`])、
//!      日の行・週の小計・月の合計にする ([`report::build_report`])
//! - `POST /restraint-report/compare-csv?driver_cd=` (multipart、最初の field が CSV) → 拘束時間管理表の CSV の乗務員ごとに、
//!   乗務員CD の当たる乗務員の帳票と比べた差分 ([`compare`])
//!
//! **組み立て (`report`・`compare`・`models`) は rust-alc-api の `alc_dtako::dtako_restraint_report` を写したもの**。
//! 応答の形・status・本文も rust と同じ。DB の失敗の本文は rust と同じ `internal server error` で、原因は `log` に段の名前と
//! kind だけを出す。PDF (`/restraint-report/pdf*`) は W5c-4 で移す (まだ rust-alc-api)。

pub mod compare;
pub mod models;
pub mod report;

use alc_compare::parse_restraint_csv;
use alc_core_wasm::TenantId;
use axum::extract::{DefaultBodyLimit, Multipart, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use uuid::Uuid;

use crate::pg;
use crate::routes::{DtakoState, UPLOAD_BODY_LIMIT};
use crate::split::LogLevel;
use models::{CompareQuery, CompareResult, RestraintReportFilter, RestraintReportResponse};
use report::{build_report, MonthRange};

type ApiError = (StatusCode, String);

pub fn router() -> Router<DtakoState> {
    Router::new()
        .route("/restraint-report", get(restraint_report))
        // CSV は 1 回で最大 20MB まで (rust の全体の上限と同じ。axum の既定の 2MB では足りない)
        .route(
            "/restraint-report/compare-csv",
            post(compare_csv).layer(DefaultBodyLimit::max(UPLOAD_BODY_LIMIT)),
        )
}

/// DB の失敗を 500 にし、段の名前と kind だけを `log` に出す (本文は rust と同じ `internal server error`)。
fn db_failed(state: &DtakoState, what: &str, e: &tokio_postgres::Error) -> ApiError {
    let kind = alc_worker_db::kind(e);
    (state.log)(
        LogLevel::Error,
        &format!("restraint-report {what} failed: db ({kind})"),
    );
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal server error".to_owned(),
    )
}

async fn restraint_report(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(filter): Query<RestraintReportFilter>,
) -> Result<Json<RestraintReportResponse>, ApiError> {
    let driver_name = {
        let mut client = state.pg.lock().await;
        pg::restraint_driver_name(&mut client, tenant_id, filter.driver_id)
            .await
            .map_err(|e| db_failed(&state, "driver", &e))?
            .unwrap_or_default()
    };
    report_for(
        &state,
        tenant_id,
        filter.driver_id,
        &driver_name,
        filter.year,
        filter.month,
    )
    .await
    .map(Json)
}

/// 名前の分かっている乗務員の月の帳票 (rust の `build_report_with_name`)。
async fn report_for(
    state: &DtakoState,
    tenant_id: Uuid,
    driver_id: Uuid,
    driver_name: &str,
    year: i32,
    month: u32,
) -> Result<RestraintReportResponse, ApiError> {
    let Some(range) = MonthRange::new(year, month) else {
        return Err((StatusCode::BAD_REQUEST, "invalid year/month".to_owned()));
    };
    let inputs = {
        let mut client = state.pg.lock().await;
        pg::restraint_inputs(&mut client, tenant_id, driver_id, range)
            .await
            .map_err(|e| db_failed(state, "inputs", &e))?
    };
    Ok(build_report(
        driver_id,
        driver_name,
        year,
        month,
        range,
        &inputs,
    ))
}

async fn compare_csv(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(query): Query<CompareQuery>,
    mut multipart: Multipart,
) -> Result<Json<Vec<CompareResult>>, ApiError> {
    // CSVファイルを受け取る
    let field = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("multipart error: {e}")))?
        .ok_or((StatusCode::BAD_REQUEST, "CSVファイルが空です".to_string()))?;
    let csv_bytes = field
        .bytes()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("field read error: {e}")))?;
    if csv_bytes.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "CSVファイルが空です".to_string()));
    }

    let csv_drivers = parse_restraint_csv(&csv_bytes).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let (year, month) = compare::csv_year_month(&csv_drivers);

    // 全ドライバー取得
    let db_drivers = {
        let mut client = state.pg.lock().await;
        pg::restraint_drivers_with_cd(&mut client, tenant_id)
            .await
            .map_err(|e| db_failed(&state, "drivers", &e))?
    };

    let mut results = Vec::new();
    for csv_d in &csv_drivers {
        // driver_cd フィルター
        if query
            .driver_cd
            .as_ref()
            .is_some_and(|cd| csv_d.driver_cd != *cd)
        {
            continue;
        }
        // driver_cd でマッチ
        let matched = match db_drivers
            .iter()
            .find(|d| d.driver_cd.as_deref() == Some(csv_d.driver_cd.as_str()))
        {
            Some(d) => {
                // システムのレポートを取得 (エラー時は帳票なし)
                let report = report_for(&state, tenant_id, d.id, &d.name, year, month)
                    .await
                    .ok();
                Some((d.id.to_string(), report))
            }
            None => None,
        };
        results.push(compare::compare_result(csv_d, matched));
    }
    Ok(Json(results))
}
