//! 車両の現在地のログ (dtakologs) の一括 upsert の入力と応答 (W5a、Refs ippoan/rust-alc-api#680)。
//!
//! 送り手は nuxt-dtako-admin の dtako-scraper-relay (auth-worker の RPC `forwardAlcTenantData` 越しに
//! `POST /api/dtako-logs/bulk`)。**入力の JSON の形 (PascalCase・既定値) と応答の形は rust-alc-api の
//! `alc_core::models::DtakologInput`・`BulkUpsertResponse` と同じ** (送り手を変えずに振り分けを切り替えるため)。
//! DB に入れるのは [`crate::pg::dtakologs_bulk_upsert`]。

use serde::{Deserialize, Serialize};

/// 1 文で upsert する行の数 (rust-alc-api と同じ。超えたぶんは次の文に回す)。
pub const CHUNK_SIZE: usize = 500;

/// `DataDateTime` が無い行に入れる日時 (rust-alc-api と同じ。スクレイパーは GPS 未取得の車両で null を送る)。
pub const DEFAULT_DATA_DATE_TIME: &str = "2020-01-01T00:00:00+09:00";

/// 1 行ぶん (PascalCase の JSON)。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DtakologInput {
    // 主キー (DataDateTime は null 許容)
    pub data_date_time: Option<String>,
    #[serde(rename = "VehicleCD")]
    pub vehicle_cd: i32,

    // 必須 (無ければ既定値)
    #[serde(rename = "__type", default)]
    pub r#type: String,
    #[serde(default)]
    pub all_state_font_color_index: i32,
    #[serde(default = "default_transparent")]
    pub all_state_ryout_color: String,
    #[serde(rename = "BranchCD", default)]
    pub branch_cd: i32,
    #[serde(default)]
    pub branch_name: String,
    #[serde(rename = "CurrentWorkCD", default)]
    pub current_work_cd: i32,
    #[serde(default)]
    pub data_filter_type: i32,
    #[serde(default)]
    pub disp_flag: i32,
    #[serde(rename = "DriverCD", default)]
    pub driver_cd: i32,
    #[serde(rename = "GPSDirection", default)]
    pub gps_direction: f64,
    #[serde(rename = "GPSEnable", default)]
    pub gps_enable: i32,
    #[serde(rename = "GPSLatitude", default)]
    pub gps_latitude: f64,
    #[serde(rename = "GPSLongitude", default)]
    pub gps_longitude: f64,
    #[serde(rename = "GPSSatelliteNum", default)]
    pub gps_satellite_num: i32,
    #[serde(default)]
    pub operation_state: i32,
    #[serde(default)]
    pub recive_event_type: i32,
    #[serde(default)]
    pub recive_packet_type: i32,
    #[serde(rename = "ReciveWorkCD", default)]
    pub recive_work_cd: i32,
    #[serde(default)]
    pub revo: i32,
    #[serde(default)]
    pub setting_temp: String,
    #[serde(default)]
    pub setting_temp1: String,
    #[serde(default)]
    pub setting_temp3: String,
    #[serde(default)]
    pub setting_temp4: String,
    #[serde(default)]
    pub speed: f32,
    #[serde(rename = "SubDriverCD", default)]
    pub sub_driver_cd: i32,
    #[serde(default)]
    pub temp_state: i32,
    #[serde(default)]
    pub vehicle_name: String,

    // 省ける
    #[serde(rename = "AddressDispC")]
    pub address_disp_c: Option<String>,
    #[serde(rename = "AddressDispP")]
    pub address_disp_p: Option<String>,
    pub all_state: Option<String>,
    pub all_state_ex: Option<String>,
    pub all_state_font_color: Option<String>,
    pub comu_date_time: Option<String>,
    pub current_work_name: Option<String>,
    pub driver_name: Option<String>,
    pub event_val: Option<String>,
    #[serde(rename = "GPSLatiAndLong")]
    pub gps_lati_and_long: Option<String>,
    #[serde(rename = "ODOMeter")]
    pub odometer: Option<String>,
    pub recive_type_color_name: Option<String>,
    pub recive_type_name: Option<String>,
    pub start_work_date_time: Option<String>,
    pub state: Option<String>,
    pub state1: Option<String>,
    pub state2: Option<String>,
    pub state3: Option<String>,
    pub state_flag: Option<String>,
    pub temp1: Option<String>,
    pub temp2: Option<String>,
    pub temp3: Option<String>,
    pub temp4: Option<String>,
    pub vehicle_icon_color: Option<String>,
    pub vehicle_icon_label_for_datetime: Option<String>,
    pub vehicle_icon_label_for_driver: Option<String>,
    pub vehicle_icon_label_for_vehicle: Option<String>,
}

fn default_transparent() -> String {
    "Transparent".to_owned()
}

/// `POST /dtako-logs/bulk` の応答 (rust-alc-api と同じ形)。
#[derive(Debug, Serialize)]
pub struct BulkUpsertResponse {
    pub success: bool,
    pub records_added: i32,
    pub total_records: i32,
    pub message: String,
}

impl BulkUpsertResponse {
    /// 空の入力 (DB を引かない)。
    pub fn empty() -> Self {
        Self {
            success: true,
            records_added: 0,
            total_records: 0,
            message: "No records provided".to_owned(),
        }
    }

    /// `affected` = upsert で触れた行数、`total` = 入力の行数。
    pub fn upserted(affected: u64, total: usize) -> Self {
        Self {
            success: true,
            records_added: affected as i32,
            total_records: total as i32,
            message: format!("Upserted {affected} records"),
        }
    }
}
