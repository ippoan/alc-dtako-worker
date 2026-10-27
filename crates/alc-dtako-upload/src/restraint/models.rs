//! 拘束時間の帳票の型 (W5c-3)。
//!
//! 応答の型 (`RestraintReportResponse` …) は rust-alc-api の `alc_pdf::types` を、CSV 比較の型
//! (`CompareResult` …) と DB の行の型は `alc_dtako::dtako_restraint_report` / `alc_core::repository::dtako_restraint_report`
//! を写したもの。欄の名前・順・型は rust と同じ (画面がそのまま読む)。

use alc_compare::{CsvDriverData, DiffItem};
use alc_worker_db::TxOutput;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `GET /restraint-report` の query。
#[derive(Debug, Deserialize)]
pub struct RestraintReportFilter {
    pub driver_id: Uuid,
    pub year: i32,
    pub month: u32,
}

/// `POST /restraint-report/compare-csv` の query。
#[derive(Debug, Deserialize)]
pub struct CompareQuery {
    pub driver_cd: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RestraintReportResponse {
    pub driver_id: Uuid,
    pub driver_name: String,
    pub year: i32,
    pub month: u32,
    pub max_restraint_minutes: i32,
    pub days: Vec<RestraintDayRow>,
    pub weekly_subtotals: Vec<WeeklySubtotal>,
    pub monthly_total: MonthlyTotal,
}

#[derive(Debug, Serialize)]
pub struct RestraintDayRow {
    pub date: NaiveDate,
    pub is_holiday: bool,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub operations: Vec<OperationDetail>,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub break_minutes: i32,
    pub restraint_total_minutes: i32,
    pub restraint_cumulative_minutes: i32,
    pub drive_average_minutes: f64,
    pub rest_period_minutes: Option<i32>,
    pub remarks: String,
    // CSV互換フィールド
    pub overlap_drive_minutes: i32,
    pub overlap_cargo_minutes: i32,
    pub overlap_break_minutes: i32,
    pub overlap_restraint_minutes: i32,
    pub restraint_main_minutes: i32,
    pub drive_avg_before: Option<i32>,
    pub drive_avg_after: Option<i32>,
    pub actual_work_minutes: i32,
    pub overtime_minutes: i32,
    pub late_night_minutes: i32,
    pub overtime_late_night_minutes: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperationDetail {
    pub unko_no: String,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub break_minutes: i32,
    pub restraint_minutes: i32,
}

#[derive(Debug, Serialize)]
pub struct WeeklySubtotal {
    pub week_end_date: NaiveDate,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub break_minutes: i32,
    pub restraint_minutes: i32,
}

#[derive(Debug, Serialize)]
pub struct MonthlyTotal {
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub break_minutes: i32,
    pub restraint_minutes: i32,
    pub fiscal_year_cumulative_minutes: i32,
    pub fiscal_year_total_minutes: i32,
    // CSV互換フィールド
    pub overlap_drive_minutes: i32,
    pub overlap_cargo_minutes: i32,
    pub overlap_break_minutes: i32,
    pub overlap_restraint_minutes: i32,
    pub actual_work_minutes: i32,
    pub overtime_minutes: i32,
    pub late_night_minutes: i32,
    pub overtime_late_night_minutes: i32,
}

// === CSV比較 ===

#[derive(Debug, Serialize)]
pub struct CompareResult {
    pub driver_name: String,
    pub driver_cd: String,
    pub driver_id: Option<String>,
    pub csv: CsvDriverData,
    pub system: Option<SystemDriverData>,
    pub diffs: Vec<DiffItem>,
    pub known_bug_diffs: usize,
    pub unknown_diffs: usize,
}

#[derive(Debug, Serialize)]
pub struct SystemDriverData {
    pub days: Vec<SystemDayRow>,
    pub total_drive: String,
    pub total_overlap_drive: String,
    pub total_restraint: String,
    pub total_actual_work: String,
    pub total_overtime: String,
    pub total_late_night: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SystemDayRow {
    pub date: String,
    pub start_time: String,
    pub end_time: String,
    pub drive: String,
    pub overlap_drive: String,
    pub cargo: String,
    pub overlap_cargo: String,
    pub subtotal: String,
    pub overlap_subtotal: String,
    pub total: String,
    pub cumulative: String,
    pub actual_work: String,
    pub overtime: String,
    pub late_night: String,
}

// === DB の行 (`crate::pg` が詰める) ===

/// 区間 1 行 (`repo::sql::LIST_RESTRAINT_SEGMENTS`)。
#[derive(Debug, Clone)]
pub struct SegmentRow {
    pub work_date: NaiveDate,
    pub unko_no: String,
    pub start_at: DateTime<Utc>,
    pub end_at: DateTime<Utc>,
    pub work_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
}

/// 運行日ごとの始業・終業 (`repo::sql::LIST_RESTRAINT_OPERATION_TIMES`)。
#[derive(Debug, Clone)]
pub struct OpTimesRow {
    pub operation_date: NaiveDate,
    pub first_departure: DateTime<Utc>,
    pub last_seg_end: DateTime<Utc>,
}

/// 日別 1 行 (`repo::sql::LIST_RESTRAINT_DAILY_WORK_HOURS`)。
#[derive(Debug, Clone)]
pub struct DailyWorkHoursRow {
    pub work_date: NaiveDate,
    pub start_time: NaiveTime,
    pub total_work_minutes: i32,
    pub total_rest_minutes: Option<i32>,
    pub late_night_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub overlap_drive_minutes: i32,
    pub overlap_cargo_minutes: i32,
    pub overlap_break_minutes: i32,
    pub overlap_restraint_minutes: i32,
    pub ot_late_night_minutes: i32,
}

/// 帳票 1 か月ぶんの DB の入力 (`crate::pg::restraint_inputs` が 1 つの transaction で読む)。
#[derive(Debug, Clone, Default)]
pub struct ReportInputs {
    pub segments: Vec<SegmentRow>,
    pub daily_work_hours: Vec<DailyWorkHoursRow>,
    /// 前日の最初の区間の運転時間 (前運転平均の 1 日目に使う)
    pub prev_day_drive: Option<i32>,
    /// 年度の始めから前月末までの拘束の合計 (4 月は 0)
    pub fiscal_cumulative: i32,
    pub operation_times: Vec<OpTimesRow>,
}

impl TxOutput for ReportInputs {}

/// 乗務員CD の在る乗務員 1 人 (`repo::sql::LIST_RESTRAINT_DRIVERS_WITH_CD`)。
#[derive(Debug, Clone)]
pub struct DriverWithCd {
    pub id: Uuid,
    pub driver_cd: Option<String>,
    pub name: String,
}

impl TxOutput for DriverWithCd {}
