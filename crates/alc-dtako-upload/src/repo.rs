//! デジタコの運行 CSV のアップロードの SQL の定数 (Refs ippoan/rust-alc-api#725)。表は alc-migrations の migration 054
//! (`dtako_upload_history`・`dtako_operations`・`dtako_offices`・`dtako_vehicles`・`dtako_event_classifications`・
//! `dtako_daily_work_hours`・`dtako_daily_work_segments`) と
//! 152 (`dtako_operation_changes`)、乗務員は `employees`。
//!
//! ここに在るのは SQL の定数だけで、流すのは [`crate::pg`] の 1 か所 (worker と `tests/sql_db.rs` が同じものを使う)。
//! テナントを設定した接続 (RLS) で流し、加えて `WHERE tenant_id` でも絞る。

/// SQL の定数 (placeholder は `$n`。引数の型の並びは `crate::pg`)。
pub mod sql {
    /// アップロード 1 件の ZIP の R2 の key。$1 id / $2 tenant_id → r2_zip_key (NULL のことがある。行が無ければ 0 行)。
    pub const SELECT_UPLOAD_ZIP_KEY: &str =
        "SELECT r2_zip_key FROM alc_api.dtako_upload_history WHERE id = $1 AND tenant_id = $2";

    /// 運行NO の運行に「KUDGIVT を分割済み」の印を付ける。$1 tenant_id / $2 unko_no の配列 (TEXT[]) →
    /// 印を付けた行の unko_no (同じ運行NO が乗務員ごとに複数行あれば、その数だけ返る)。
    pub const MARK_HAS_KUDGIVT: &str = "UPDATE alc_api.dtako_operations SET has_kudgivt = TRUE WHERE tenant_id = $1 AND unko_no = ANY($2) RETURNING unko_no";

    /// 分割がまだの運行がテナントに 1 件でも在るとき、分割の元にできる (completed で ZIP の key が在る)
    /// アップロードの一覧 (新しい順)。$1 tenant_id → id, filename, created_at。
    /// `SELECT DISTINCT` は `ORDER BY` の列が select list に要るので created_at も選ぶ (呼び手は使わない)。
    pub const LIST_UPLOADS_NEEDING_SPLIT: &str = r#"SELECT DISTINCT uh.id, uh.filename, uh.created_at
               FROM alc_api.dtako_operations o
               JOIN alc_api.dtako_upload_history uh ON uh.tenant_id = o.tenant_id
               WHERE o.tenant_id = $1 AND o.has_kudgivt = FALSE
                 AND uh.status = 'completed'
                 AND uh.r2_zip_key IS NOT NULL
               ORDER BY uh.created_at DESC, uh.id DESC"#;

    // ---- 履歴の読み取り (一覧 2 つ・zip のダウンロード) ----

    /// テナントの履歴の一覧 (新しい順に 50 件。同じ時刻は id の降順)。
    /// $1 tenant_id → id, filename, status, error_message (NULL 可), created_at, r2_zip_key (NULL 可)。
    pub const LIST_UPLOADS: &str = r#"SELECT id, filename, status, error_message, created_at, r2_zip_key
               FROM alc_api.dtako_upload_history WHERE tenant_id = $1
               ORDER BY created_at DESC, id DESC LIMIT 50"#;

    /// テナントの、やり直し待ち (`pending_retry`) か失敗 (`failed`) の履歴の一覧 (新しい順に 50 件。同じ時刻は id の降順)。
    /// $1 tenant_id → id, tenant_id, filename, status, error_message (NULL 可), created_at。
    pub const LIST_PENDING_UPLOADS: &str = r#"SELECT id, tenant_id, filename, status, error_message, created_at
               FROM alc_api.dtako_upload_history
               WHERE tenant_id = $1 AND status IN ('pending_retry', 'failed')
               ORDER BY created_at DESC, id DESC LIMIT 50"#;

    /// 履歴 1 件の zip の key と filename (ダウンロード用)。$1 id / $2 tenant_id → r2_zip_key (NULL 可), filename (行が無ければ 0 行)。
    pub const SELECT_UPLOAD_DOWNLOAD: &str =
        "SELECT r2_zip_key, filename FROM alc_api.dtako_upload_history WHERE id = $1 AND tenant_id = $2";

    // ---- 再計算 ----

    /// 月の再計算の対象の運行 (運行日か読取日が範囲に入る行) と、その乗務員CD。$1 tenant_id / $2 月初 / $3 月末の翌日 →
    /// unko_no, reading_date, operation_date, departure_at, return_at, driver_cd (NULL 可), total_distance,
    /// drive_time_general, drive_time_highway, drive_time_bypass (読取日・運行NO の順)。
    pub const LIST_OPERATIONS_FOR_RECALC: &str = r#"SELECT DISTINCT o.unko_no, o.reading_date, o.operation_date,
                      o.departure_at, o.return_at,
                      d.driver_cd,
                      o.total_distance,
                      o.drive_time_general, o.drive_time_highway, o.drive_time_bypass
               FROM alc_api.dtako_operations o
               LEFT JOIN alc_api.employees d ON d.id = o.driver_id AND d.tenant_id = o.tenant_id
               WHERE o.tenant_id = $1
                 AND (o.operation_date >= $2 AND o.operation_date <= $3
                      OR o.reading_date >= $2 AND o.reading_date <= $3)
               ORDER BY o.reading_date, o.unko_no"#;

    // ---- アップロードの取り込み (履歴・営業所と車輌と乗務員の解決・運行の入れ替え・変更記録・分類) ----

    /// アップロードの履歴を作る (status は `processing`)。$1 tenant_id / $2 filename → id (DB の既定値)。
    pub const INSERT_UPLOAD_HISTORY: &str = r#"INSERT INTO alc_api.dtako_upload_history (tenant_id, uploaded_by, filename, status)
    VALUES ($1, NULL, $2, 'processing')
    RETURNING id"#;

    /// 履歴に zip の R2 の key を記録する。$1 r2_zip_key / $2 id / $3 tenant_id。
    pub const UPDATE_UPLOAD_ZIP_KEY: &str =
        "UPDATE alc_api.dtako_upload_history SET r2_zip_key = $1 WHERE id = $2 AND tenant_id = $3";

    /// 履歴に失敗の印を付ける。$1 error_message / $2 id / $3 tenant_id。
    pub const MARK_UPLOAD_FAILED: &str =
        "UPDATE alc_api.dtako_upload_history SET status = 'failed', error_message = $1 WHERE id = $2 AND tenant_id = $3";

    /// 営業所を cd で引き当てる (無ければ作り、在れば名前を更新)。$1 tenant_id / $2 office_cd / $3 office_name → id。
    pub const UPSERT_OFFICE: &str = r#"INSERT INTO alc_api.dtako_offices (tenant_id, office_cd, office_name)
    VALUES ($1, $2, $3)
    ON CONFLICT (tenant_id, office_cd) DO UPDATE SET office_name = EXCLUDED.office_name
    RETURNING id"#;

    /// 車輌を cd で引き当てる (無ければ作り、在れば名前を更新)。$1 tenant_id / $2 vehicle_cd / $3 vehicle_name → id。
    pub const UPSERT_VEHICLE: &str = r#"INSERT INTO alc_api.dtako_vehicles (tenant_id, vehicle_cd, vehicle_name)
    VALUES ($1, $2, $3)
    ON CONFLICT (tenant_id, vehicle_cd) DO UPDATE SET vehicle_name = EXCLUDED.vehicle_name
    RETURNING id"#;

    /// 乗務員の解決 ①: 乗務員CD を `code` 列で引く (生存行)。$1 tenant_id / $2 driver_cd → id, driver_cd (NULL のことがある)。
    pub const SELECT_EMPLOYEE_BY_CODE: &str =
        "SELECT id, driver_cd FROM alc_api.employees WHERE tenant_id = $1 AND code = $2 AND deleted_at IS NULL";

    /// 乗務員の解決 ②: ① の行の `driver_cd` が NULL なら埋める。同じ driver_cd を持つ別の生存行が在れば埋めない (0 行)。
    /// $1 tenant_id / $2 driver_cd / $3 id → 更新した行数。
    pub const FILL_EMPLOYEE_DRIVER_CD: &str = r#"UPDATE alc_api.employees SET driver_cd = $2, updated_at = NOW()
    WHERE id = $3 AND tenant_id = $1
        AND driver_cd IS NULL AND deleted_at IS NULL
        AND NOT EXISTS (
            SELECT 1 FROM alc_api.employees o
            WHERE o.tenant_id = $1 AND o.driver_cd = $2
                AND o.deleted_at IS NULL AND o.id <> $3
        )"#;

    /// 乗務員の解決 ③・⑤: `driver_cd` 列で引く (生存行)。$1 tenant_id / $2 driver_cd → id。
    pub const SELECT_EMPLOYEE_BY_DRIVER_CD: &str =
        "SELECT id FROM alc_api.employees WHERE tenant_id = $1 AND driver_cd = $2 AND deleted_at IS NULL";

    /// 乗務員の解決 ④: 新しい乗務員を作る (`code` は入れない)。一意の制約に当たれば何もしない (0 行)。
    /// $1 tenant_id / $2 driver_cd / $3 name → id。
    pub const INSERT_EMPLOYEE: &str = r#"INSERT INTO alc_api.employees (tenant_id, driver_cd, name)
    VALUES ($1, $2, $3)
    ON CONFLICT DO NOTHING
    RETURNING id"#;

    /// 運行 (運行NO・crew_role) が在るか。$1 tenant_id / $2 unko_no / $3 crew_role → bool。
    pub const SELECT_OPERATION_EXISTS: &str =
        "SELECT EXISTS (SELECT 1 FROM alc_api.dtako_operations WHERE tenant_id = $1 AND unko_no = $2 AND crew_role = $3)";

    /// 運行の snapshot (変更記録の before / after に使う形)。日時は UTC の文字列にする。
    /// $1 tenant_id / $2 unko_no / $3 crew_role (NULL なら全 crew_role) → crew_role, `{driver_cd, departure_at, return_at}` (crew_role の順)。
    pub const SELECT_OPERATION_SNAPSHOT: &str = r#"SELECT o.crew_role,
        jsonb_build_object(
            'driver_cd', COALESCE(e.driver_cd, e.code),
            'departure_at', to_char(o.departure_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'return_at', to_char(o.return_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        )
    FROM alc_api.dtako_operations o
    LEFT JOIN alc_api.employees e ON e.id = o.driver_id
    WHERE o.tenant_id = $1 AND o.unko_no = $2 AND ($3::INTEGER IS NULL OR o.crew_role = $3)
    ORDER BY o.crew_role"#;

    /// 運行を消す (入れ直しの前)。$1 tenant_id / $2 unko_no / $3 crew_role。
    pub const DELETE_OPERATION: &str =
        "DELETE FROM alc_api.dtako_operations WHERE tenant_id = $1 AND unko_no = $2 AND crew_role = $3";

    /// 運行を入れる。$1 tenant_id / $2 unko_no / $3 crew_role / $4 reading_date / $5 operation_date / $6 office_id / $7 vehicle_id /
    /// $8 driver_id / $9 departure_at / $10 return_at / $11 garage_out_at / $12 garage_in_at / $13 meter_start / $14 meter_end /
    /// $15 total_distance / $16 drive_time_general / $17 drive_time_highway / $18 drive_time_bypass / $19 safety_score /
    /// $20 economy_score / $21 total_score / $22 raw_data (JSONB) / $23 r2_key_prefix。
    pub const INSERT_OPERATION: &str = r#"INSERT INTO alc_api.dtako_operations (
        tenant_id, unko_no, crew_role, reading_date, operation_date,
        office_id, vehicle_id, driver_id,
        departure_at, return_at, garage_out_at, garage_in_at,
        meter_start, meter_end, total_distance,
        drive_time_general, drive_time_highway, drive_time_bypass,
        safety_score, economy_score, total_score,
        raw_data, r2_key_prefix
    ) VALUES (
        $1, $2, $3, $4, $5,
        $6, $7, $8,
        $9, $10, $11, $12,
        $13, $14, $15,
        $16, $17, $18,
        $19, $20, $21,
        $22, $23
    )"#;

    /// 運行の変更記録を 1 件足す (追記だけ。UPDATE・DELETE はしない)。$1 tenant_id / $2 unko_no / $3 crew_role / $4 driver_cd /
    /// $5 upload_id / $6 reason / $7 before (JSONB) / $8 after (JSONB)。
    pub const INSERT_OPERATION_CHANGE: &str = r#"INSERT INTO alc_api.dtako_operation_changes
        (tenant_id, unko_no, crew_role, driver_cd, upload_id, reason, before, after)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#;

    /// テナントのイベントの分類の一覧。$1 tenant_id → event_cd, classification。
    pub const SELECT_EVENT_CLASSIFICATIONS: &str =
        "SELECT event_cd, classification FROM alc_api.dtako_event_classifications WHERE tenant_id = $1";

    /// 未登録のイベントCD の分類を足す (在れば何もしない)。$1 tenant_id / $2 event_cd / $3 event_name / $4 classification。
    pub const INSERT_EVENT_CLASSIFICATION: &str = r#"INSERT INTO alc_api.dtako_event_classifications (tenant_id, event_cd, event_name, classification)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (tenant_id, event_cd) DO NOTHING"#;

    // ---- 日別の労働時間とセグメントの保存・完了の印 ----

    /// 日別の保存先の乗務員を引く ①: 乗務員CD を `code` 列で引く (生存行。**更新しない**)。$1 tenant_id / $2 driver_cd → id。
    /// 無ければ [`SELECT_EMPLOYEE_BY_DRIVER_CD`] で引く。
    pub const SELECT_EMPLOYEE_ID_BY_CODE: &str =
        "SELECT id FROM alc_api.employees WHERE tenant_id = $1 AND code = $2 AND deleted_at IS NULL";

    /// 乗務員のセグメントを、運行NO の配列で消す (上げ直しで帰属日が変わっても古い行が残らないように)。
    /// $1 tenant_id / $2 driver_id / $3 unko_no の配列 (TEXT[])。
    pub const DELETE_SEGMENTS_BY_UNKO_NOS: &str =
        "DELETE FROM alc_api.dtako_daily_work_segments WHERE tenant_id = $1 AND driver_id = $2 AND unko_no = ANY($3)";

    /// 乗務員の日別のうち、`unko_nos` 列が運行NO の配列と 1 つでも重なる行を消す。
    /// $1 tenant_id / $2 driver_id / $3 unko_no の配列 (TEXT[])。
    pub const DELETE_DAILY_HOURS_BY_UNKO_NOS: &str =
        "DELETE FROM alc_api.dtako_daily_work_hours WHERE tenant_id = $1 AND driver_id = $2 AND unko_nos && $3";

    /// 日別 1 行を (乗務員, 日, 開始時刻) で消す (入れ直しの前)。$1 tenant_id / $2 driver_id / $3 work_date / $4 start_time。
    pub const DELETE_DAILY_HOURS_EXACT: &str =
        "DELETE FROM alc_api.dtako_daily_work_hours WHERE tenant_id = $1 AND driver_id = $2 AND work_date = $3 AND start_time = $4";

    /// 日別 1 行を入れる。$1 tenant_id / $2 driver_id / $3 work_date / $4 start_time / $5 total_work_minutes /
    /// $6 total_drive_minutes / $7 total_rest_minutes / $8 late_night_minutes / $9 drive_minutes / $10 cargo_minutes /
    /// $11 total_distance / $12 operation_count / $13 unko_nos (TEXT[]) / $14 overlap_drive_minutes /
    /// $15 overlap_cargo_minutes / $16 overlap_break_minutes / $17 overlap_restraint_minutes / $18 ot_late_night_minutes。
    pub const INSERT_DAILY_WORK_HOURS: &str = r#"INSERT INTO alc_api.dtako_daily_work_hours (
                tenant_id, driver_id, work_date, start_time,
                total_work_minutes, total_drive_minutes, total_rest_minutes,
                late_night_minutes, drive_minutes, cargo_minutes,
                total_distance, operation_count, unko_nos,
                overlap_drive_minutes, overlap_cargo_minutes,
                overlap_break_minutes, overlap_restraint_minutes,
                ot_late_night_minutes
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)"#;

    /// 乗務員のその日のセグメントを消す (入れ直しの前)。$1 tenant_id / $2 driver_id / $3 work_date。
    pub const DELETE_SEGMENTS_BY_DATE: &str =
        "DELETE FROM alc_api.dtako_daily_work_segments WHERE tenant_id = $1 AND driver_id = $2 AND work_date = $3";

    /// セグメント 1 行を入れる。$1 tenant_id / $2 driver_id / $3 work_date / $4 unko_no / $5 segment_index / $6 start_at /
    /// $7 end_at / $8 work_minutes / $9 labor_minutes / $10 late_night_minutes / $11 drive_minutes / $12 cargo_minutes。
    pub const INSERT_SEGMENT: &str = r#"INSERT INTO alc_api.dtako_daily_work_segments (
                tenant_id, driver_id, work_date, unko_no, segment_index,
                start_at, end_at, work_minutes, labor_minutes, late_night_minutes,
                drive_minutes, cargo_minutes
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"#;

    /// 履歴に完了の印を付ける。$1 operations_count / $2 id / $3 tenant_id。
    pub const MARK_UPLOAD_COMPLETED: &str =
        "UPDATE alc_api.dtako_upload_history SET status = 'completed', operations_count = $1 WHERE id = $2 AND tenant_id = $3";
}
