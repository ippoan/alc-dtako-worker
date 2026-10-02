//! [`crate::repo::sql`] の定数を流す tokio-postgres の実装 (ippoan/alc-vein-worker の `pg.rs` の形。
//! Refs ippoan/rust-alc-api#725)。直下の worker と、組み込みの PostgreSQL に流すテスト (`tests/sql_db.rs`) が、
//! 同じこの実装を使う。**接続は持たない** (張るのは worker とテスト)。ログも出さない (出すのは呼び手)。
//!
//! SQL は [`crate::repo::sql`] の定数だけを、共通 crate `alc-worker-db` の `TenantTx` が出す
//! **型付きの名前なしの文** (`query_typed` 系) で流す。名前付き prepared statement は
//! Hyperdrive 経由で接続が切れるので使わない (`TenantTx` に口が無い)。引数の型の並びが書かれるのは、
//! 下の自由関数 3 つの 1 か所だけ (`$n` の意味は `repo::sql` の各定数の doc)。
//!
//! ## RLS (関数 1 回 = 1 トランザクション)
//!
//! 各関数は `PgClient::tenant_tx` を 1 回開く。順は `BEGIN` → `alc_worker_db::SET_TENANT`
//! (`set_config('app.current_tenant_id', $1, true)` と search_path) → 本文 → `COMMIT`。
//! 戻り値は `TxOutput` (owned な型) に限られ、`Row` を transaction の外へは持ち出せない。

use alc_worker_db::PgClient;
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::repo::sql;

/// [`sql::SELECT_UPLOAD_ZIP_KEY`]。行が無い・`r2_zip_key` が NULL は、どちらも `Ok(None)`。
pub async fn upload_zip_key(
    pg: &mut PgClient,
    tenant_id: Uuid,
    upload_id: Uuid,
) -> Result<Option<String>, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::SELECT_UPLOAD_ZIP_KEY,
                    &[(&upload_id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(row.and_then(|r| r.get(0)))
        })
    })
    .await
}

/// [`sql::MARK_HAS_KUDGIVT`]。返すのは `RETURNING` の値そのまま (重複を除かない。呼び手が集合にする)。
/// `unko_nos` が空なら transaction を開かずに空を返す。
pub async fn mark_has_kudgivt(
    pg: &mut PgClient,
    tenant_id: Uuid,
    unko_nos: Vec<String>,
) -> Result<Vec<String>, tokio_postgres::Error> {
    if unko_nos.is_empty() {
        return Ok(Vec::new());
    }
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::MARK_HAS_KUDGIVT,
                    &[(&tenant_id, Type::UUID), (&unko_nos, Type::TEXT_ARRAY)],
                )
                .await?;
            Ok(rows.into_iter().map(|r| r.get(0)).collect())
        })
    })
    .await
}

/// [`sql::LIST_UPLOADS_NEEDING_SPLIT`]。`(id, filename)` を新しい順に返す。
pub async fn uploads_needing_split(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<(Uuid, String)>, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_UPLOADS_NEEDING_SPLIT, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
        })
    })
    .await
}
