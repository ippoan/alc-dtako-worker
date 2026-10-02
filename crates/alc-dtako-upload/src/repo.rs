//! デジタコの運行 CSV のアップロード履歴 (`dtako_upload_history`) と運行 (`dtako_operations`) の SQL の定数
//! (alc-migrations の migration 054。Refs ippoan/rust-alc-api#725)。
//!
//! ここに在るのは SQL の定数だけで、流すのは [`crate::pg`] の 1 か所 (worker と `tests/sql_db.rs` が同じものを使う)。
//! テナントを設定した接続 (RLS) で流し、加えて `WHERE tenant_id` でも絞る。

/// `dtako_upload_history`・`dtako_operations` の SQL (placeholder は `$n`。引数の型の並びは `crate::pg`)。
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
}
