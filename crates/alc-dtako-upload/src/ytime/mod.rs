//! Y時間 export (W5c-2、Refs ippoan/rust-alc-api#680)。
//!
//! - `GET /dtako/y-time-export?driver_cd=&from=&to=` → 乗務員 1 人の期間の Y時間 シートの行
//!   1. 乗務員CD → 乗務員 ([`pg::events_driver`]。無ければ 404)
//!   2. 期間を前後 1 日広げて分割済みの運行を列挙 ([`pg::events_operations`]。読取日 **または** 運行日)
//!   3. 各運行の KUDGIVT.csv を同時 [`GET_CONCURRENCY`] 本で読み ([`crate::events::fetch_all`])、crew_role で絞る
//!   4. 休息で区切って区間にし ([`segments::build_segment_inputs`])、1 暦日 2 始業の規則で行にする ([`builder::build_y_time_rows`])
//! - `POST /dtako/y-time-rows` → 勤怠の勤務の列 (body) から同じ行を返す計算だけの口 ([`from_shifts::rows_from_shifts`]。DB も保存先も読まない)
//!
//! **行の組み立て (`builder`・`from_shifts`・`models`・`segments`) は rust-alc-api の `alc_dtako::dtako_y_time_export` を写したもの**。
//! 応答の形・status・本文も rust と同じ。違いは 2 つ: 保存先の同時の読みが Workers の同時接続の上限 (6) に合わせて少ない (rust は 16)・
//! 読めなかった運行の warning の括弧の中が固定の語 (`storage error: not found` など。rust は保存先のエラー文をそのまま載せていた)。
//! DB の失敗の本文は rust と同じ `internal error` で、原因は `log` に段の名前と kind だけを出す。

pub mod builder;
pub mod from_shifts;
pub mod models;
pub mod segments;

use alc_core_wasm::TenantId;
use alc_csv_parser::kudgivt::parse_kudgivt_for_crew;
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, NaiveDateTime, Utc};
use uuid::Uuid;

use crate::events::{fetch_all, kudgivt_key, widened};
use crate::pg;
use crate::routes::{DtakoState, UPLOAD_BODY_LIMIT};
use crate::split::LogLevel;
use builder::{build_y_time_rows, SegmentInput};
use models::{
    YTimeDriver, YTimeExportQuery, YTimeExportResponse, YTimePeriod, YTimeRowsRequest,
    YTimeRowsResponse,
};

pub fn router() -> Router<DtakoState> {
    Router::new()
        .route("/dtako/y-time-export", get(y_time_export))
        // 勤務の列は 1 回で最大約 16MB (rust と同じ上限。axum の既定の 2MB では足りない)
        .route(
            "/dtako/y-time-rows",
            post(y_time_rows).layer(DefaultBodyLimit::max(UPLOAD_BODY_LIMIT)),
        )
}

/// 勤怠の勤務の列から Y時間 の行を返す。計算だけで、保存値は読まない・書かない。
/// body が読めない (JSON でない・欄の欠け・時刻の形の違い) のも、検証の破れも 400。
async fn y_time_rows(
    body: Result<Json<YTimeRowsRequest>, JsonRejection>,
) -> Result<Json<YTimeRowsResponse>, (StatusCode, String)> {
    let Json(req) = body.map_err(|e| (StatusCode::BAD_REQUEST, e.body_text()))?;
    from_shifts::rows_from_shifts(req)
        .map(Json)
        .map_err(|m| (StatusCode::BAD_REQUEST, m))
}

async fn y_time_export(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(q): Query<YTimeExportQuery>,
) -> Result<Json<YTimeExportResponse>, (StatusCode, String)> {
    compute_y_time_export(&state, tenant_id, q).await.map(Json)
}

/// DB の失敗を 500 にし、段の名前と kind だけを `log` に出す (本文は rust と同じ `internal error`)。
fn db_failed(state: &DtakoState, what: &str, e: &tokio_postgres::Error) -> (StatusCode, String) {
    let kind = alc_worker_db::kind(e);
    (state.log)(
        LogLevel::Error,
        &format!("y-time-export {what} failed: db ({kind})"),
    );
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal error".to_owned(),
    )
}

/// `DateTime<Utc>` → 壁時計 (取り込みは JST の壁時計をそのまま UTC として保存している。rust と同じ読み方)。
fn naive(dt: DateTime<Utc>) -> NaiveDateTime {
    dt.naive_utc()
}

/// 乗務員 1 人の期間の行を組み立てる。
pub async fn compute_y_time_export(
    state: &DtakoState,
    tenant_id: Uuid,
    q: YTimeExportQuery,
) -> Result<YTimeExportResponse, (StatusCode, String)> {
    if q.from > q.to {
        return Err((StatusCode::BAD_REQUEST, "from > to".to_owned()));
    }
    let (driver_id, driver_name) = {
        let mut client = state.pg.lock().await;
        pg::events_driver(&mut client, tenant_id, q.driver_cd.clone())
            .await
            .map_err(|e| db_failed(state, "driver", &e))?
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    format!("driver_cd not found: {}", q.driver_cd),
                )
            })?
    };
    let operations = {
        let (from, to) = widened(q.from, q.to);
        let mut client = state.pg.lock().await;
        pg::events_operations(&mut client, tenant_id, driver_id, from, to)
            .await
            .map_err(|e| db_failed(state, "operations", &e))?
    };

    // 出発・帰着の欠けた運行は先に warning にして、残りだけを読む (rust と同じ順の warning)
    let mut warnings: Vec<String> = Vec::new();
    let mut targets = Vec::with_capacity(operations.len());
    for op in operations {
        match (op.departure_at, op.return_at) {
            (Some(d), Some(r)) => targets.push((op, d, r)),
            _ => warnings.push(format!(
                "{}: departure_at/return_at が不足、skip",
                op.unko_no
            )),
        }
    }
    let keys = targets
        .iter()
        .map(|(op, _, _)| kudgivt_key(tenant_id, &op.unko_no, op.r2_key_prefix.as_deref()))
        .collect();
    let fetched = fetch_all(state, keys).await;

    let mut all_segments: Vec<SegmentInput> = Vec::with_capacity(targets.len());
    for ((op, departure, ret), got) in targets.into_iter().zip(fetched) {
        let events = got
            .map_err(|e| format!("storage error: {e}"))
            .and_then(|bytes| {
                parse_kudgivt_for_crew(&bytes, op.crew_role)
                    .map_err(|e| format!("csv parse error: {e}"))
            });
        match events {
            Ok(events) => all_segments.extend(segments::build_segment_inputs(
                &events,
                naive(departure),
                naive(ret),
            )),
            Err(why) => warnings.push(format!("{}: KUDGIVT 取得失敗 ({why})", op.unko_no)),
        }
    }

    let (rows, build_warnings) = build_y_time_rows(all_segments, q.from, q.to);
    warnings.extend(build_warnings);
    Ok(YTimeExportResponse {
        driver: YTimeDriver {
            cd: q.driver_cd,
            name: driver_name,
        },
        period: YTimePeriod {
            from: q.from,
            to: q.to,
        },
        rows,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use crate::store::GET_CONCURRENCY;

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn get_concurrency_is_in_safe_range() {
        // 1 (= 逐次) は遅く、Workers の同時接続の上限 (6) を超えると待たされるだけ
        assert!(GET_CONCURRENCY >= 2);
        assert!(GET_CONCURRENCY <= 6);
    }
}
