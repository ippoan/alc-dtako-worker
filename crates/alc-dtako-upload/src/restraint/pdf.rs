//! 拘束時間管理表の PDF (W5c-4、Refs ippoan/rust-alc-api#680)。
//!
//! - `GET /restraint-report/pdf?year=&month=&driver_id=` → PDF (`application/pdf`、添付)。`driver_id` が無ければ、その月に運行のある乗務員
//!   (社員の全員ではない。乗務しない社員まで入れると約 400 人で Worker のメモリに収まらない)
//!   1. 年月が日付にならなければ 400 `invalid year/month`。乗務員を読み ([`pg::restraint_pdf_drivers`])、0 人なら 404 `ドライバーが見つかりません`
//!   2. 名前の空の乗務員を飛ばし、1 人ずつ帳票を組み立て (W5c-3 の [`super::report_for`]。失敗はその status と本文で返す)
//!   3. まとめて描画する ([`alc_pdf::generate_pdf`])
//! - `GET /restraint-report/pdf-stream?year=&month=` → event-stream。その月に運行のある乗務員ぶんの進み具合 (`progress`) と、
//!   最後に PDF を base64 で載せた `done` (年月の誤り・乗務員を読めなければ `error` 1 つ)。帳票を組み立てられなかった乗務員は飛ばす
//!
//! 形・status・本文・event の欄は rust-alc-api の `alc_dtako::dtako_restraint_report_pdf` と同じ (画面がそのまま読む)。
//! rust は乗務員の列名 `name` を欄 `driver_name` で読もうとして常に 500 だった — ここは列を位置で読む。
//! event-stream は `tokio::spawn` で裏に回さず、本文が読まれるたびに次の段 (乗務員 1 人の組み立て・描画) を進める
//! ([`futures_util::stream::unfold`])。

use alc_core_wasm::TenantId;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::Response;
use axum::Extension;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::models::{DriverWithCd, RestraintReportResponse};
use super::report::MonthRange;
use super::{db_failed, report_for, ApiError};
use crate::pg;
use crate::routes::DtakoState;
use crate::split::LogLevel;

#[derive(Debug, Deserialize)]
pub struct PdfFilter {
    pub year: i32,
    pub month: u32,
    pub driver_id: Option<Uuid>,
}

/// event-stream の 1 件 (rust と同じ欄。`data` と `message` は無ければ出さない)。
#[derive(Debug, Serialize)]
struct PdfProgressEvent {
    event: &'static str,
    current: Option<usize>,
    total: Option<usize>,
    driver_name: Option<String>,
    step: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

impl PdfProgressEvent {
    fn new(event: &'static str) -> Self {
        Self {
            event,
            current: None,
            total: None,
            driver_name: None,
            step: None,
            data: None,
            message: None,
        }
    }

    fn sse(&self) -> String {
        let json = serde_json::to_string(self).unwrap_or_default();
        format!("data: {json}\n\n")
    }
}

pub(super) async fn restraint_report_pdf(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(filter): Query<PdfFilter>,
) -> Result<Response, ApiError> {
    let Some(range) = MonthRange::new(filter.year, filter.month) else {
        return Err((StatusCode::BAD_REQUEST, "invalid year/month".to_owned()));
    };
    let drivers = {
        let mut client = state.pg.lock().await;
        pg::restraint_pdf_drivers(&mut client, tenant_id, filter.driver_id, range)
            .await
            .map_err(|e| db_failed(&state, "pdf drivers", &e))?
    };
    if drivers.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            "ドライバーが見つかりません".to_owned(),
        ));
    }

    let mut reports = Vec::new();
    let mut driver_cds = Vec::new();
    for driver in drivers.iter().filter(|d| !d.name.is_empty()) {
        let report = report_for(
            &state,
            tenant_id,
            driver.id,
            &driver.name,
            filter.year,
            filter.month,
        )
        .await?;
        driver_cds.push(driver.driver_cd.clone().unwrap_or_default());
        reports.push(report);
    }

    let pdf = alc_pdf::generate_pdf(&reports, &driver_cds, filter.year, filter.month);
    let filename = format!("restraint_report_{}_{:02}.pdf", filter.year, filter.month);
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "application/pdf")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        )
        .body(Body::from(pdf))
        .expect("valid response builder"))
}

pub(super) async fn restraint_report_pdf_stream(
    State(state): State<DtakoState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(filter): Query<PdfFilter>,
) -> Response {
    let run = StreamRun {
        state,
        tenant_id,
        year: filter.year,
        month: filter.month,
        phase: Phase::Start,
        drivers: Vec::new(),
        next: 0,
        reports: Vec::new(),
        driver_cds: Vec::new(),
    };
    let stream = futures_util::stream::unfold(run, |mut run| async move {
        let event = run.step().await?;
        Some((Ok::<_, std::convert::Infallible>(event.sse()), run))
    });
    Response::builder()
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header("X-Accel-Buffering", "no")
        .body(Body::from_stream(stream))
        .expect("valid response builder")
}

enum Phase {
    /// 乗務員をまだ読んでいない
    Start,
    /// `next` 人目まで進み具合を出した (`next` > 0 なら、その人の帳票をまだ組み立てていない)
    Drivers,
    /// 全員の帳票を組み立てた (次は描画して `done`)
    Render,
    /// 出し終えた
    Finished,
}

struct StreamRun {
    state: DtakoState,
    tenant_id: Uuid,
    year: i32,
    month: u32,
    phase: Phase,
    drivers: Vec<DriverWithCd>,
    next: usize,
    reports: Vec<RestraintReportResponse>,
    driver_cds: Vec<String>,
}

impl StreamRun {
    /// 次の 1 件を作る (出し終えたら `None`)。
    async fn step(&mut self) -> Option<PdfProgressEvent> {
        if let Phase::Start = self.phase {
            let Some(range) = MonthRange::new(self.year, self.month) else {
                self.phase = Phase::Finished;
                let mut evt = PdfProgressEvent::new("error");
                evt.message = Some("invalid year/month".to_owned());
                return Some(evt);
            };
            let read = {
                let mut client = self.state.pg.lock().await;
                pg::restraint_pdf_drivers(&mut client, self.tenant_id, None, range).await
            };
            match read {
                Ok(drivers) => {
                    self.drivers = drivers.into_iter().filter(|d| !d.name.is_empty()).collect();
                    self.phase = Phase::Drivers;
                }
                Err(e) => {
                    let _ = db_failed(&self.state, "pdf-stream drivers", &e);
                    self.phase = Phase::Finished;
                    let mut evt = PdfProgressEvent::new("error");
                    evt.message = Some(format!(
                        "ドライバー取得エラー: db ({})",
                        alc_worker_db::kind(&e)
                    ));
                    return Some(evt);
                }
            }
        }
        let total = self.drivers.len();
        match self.phase {
            Phase::Start | Phase::Finished => None,
            Phase::Drivers => {
                // 前の回に進み具合を出した人の帳票を組み立てる
                if self.next > 0 {
                    self.build(self.next - 1).await;
                }
                if self.next < total {
                    self.next += 1;
                    let mut evt = PdfProgressEvent::new("progress");
                    evt.current = Some(self.next);
                    evt.total = Some(total);
                    evt.driver_name = Some(self.drivers[self.next - 1].name.clone());
                    evt.step = Some("fetch");
                    return Some(evt);
                }
                self.phase = Phase::Render;
                let mut evt = PdfProgressEvent::new("progress");
                evt.current = Some(total);
                evt.total = Some(total);
                evt.step = Some("render");
                evt.message = Some("PDF生成中...".to_owned());
                Some(evt)
            }
            Phase::Render => {
                self.phase = Phase::Finished;
                let pdf =
                    alc_pdf::generate_pdf(&self.reports, &self.driver_cds, self.year, self.month);
                let mut evt = PdfProgressEvent::new("done");
                evt.current = Some(total);
                evt.total = Some(total);
                evt.step = Some("save");
                evt.data = Some(base64::engine::general_purpose::STANDARD.encode(&pdf));
                Some(evt)
            }
        }
    }

    /// `i` 人目の帳票を組み立てて足す (組み立てられなければ飛ばし、status だけを `log` に出す)。
    async fn build(&mut self, i: usize) {
        let driver = &self.drivers[i];
        match report_for(
            &self.state,
            self.tenant_id,
            driver.id,
            &driver.name,
            self.year,
            self.month,
        )
        .await
        {
            Ok(report) => {
                self.driver_cds
                    .push(driver.driver_cd.clone().unwrap_or_default());
                self.reports.push(report);
            }
            Err((status, _)) => (self.state.log)(
                LogLevel::Warn,
                &format!("restraint-report pdf-stream skip driver: {status}"),
            ),
        }
    }
}
