//! テストの中で起こす組み込みの PostgreSQL (`pglite-oxide`)。docker も外の DB も要らない
//! (ippoan/alc-vein-worker の `crates/alc-vein/tests/embedded/mod.rs` を dtako 用に作り替えたもの)。
//!
//! 制約: **同時に張れる接続は 1 本** (2 本目は 1 本目が閉じるまで待たされる)。別の接続を張るときは、
//! [`Held::close`] で閉じ切ってから次を張る。
//! `ALTER DATABASE … SET search_path` が効かないので、migration を流す接続だけ options で search_path を渡す
//! (テストの接続には渡さない — `alc_worker_db::SET_TENANT` が transaction ごとに設定する)。
//!
//! migration を流す順は ippoan/alc-vein-worker の `container/start.sh` (staging の DB の image) と同じ ([`Embedded::migrate`])。

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use alc_worker_db::PgClient;
use pglite_oxide::PgliteServer;
use tokio::task::JoinHandle;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

/// 表の所有者でない・NOBYPASSRLS のロール (`local_app_grants.sql` が権限を付ける)。
pub const APP_ROLE: &str = "alc_api_app";
/// この crate が読み書きする表 (全部 RLS が効く前提。所有者でないこと・テナントなしで読めないことを検査する)。
pub const TABLES: [&str; 9] = [
    "dtako_upload_history",
    "dtako_operations",
    "dtako_offices",
    "dtako_vehicles",
    "dtako_event_classifications",
    "dtako_operation_changes",
    "dtako_daily_work_hours",
    "dtako_daily_work_segments",
    "employees",
];
const SUPERUSER: &str = "postgres";
const SOCKET_FILE: &str = ".s.PGSQL.5432";
const SEARCH_PATH: &str = "-c search_path=alc_api,public";

static SEQ: AtomicUsize = AtomicUsize::new(0);

/// `scripts/fetch-migrations.sh` が取り出す先 (版は `scripts/ALC_MIGRATIONS_REV`)。
fn migrations_dir() -> PathBuf {
    let dir = PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../.alc-migrations"
    ));
    assert!(
        dir.join("migrations").is_dir(),
        ".alc-migrations が無い。先に `bash scripts/fetch-migrations.sh` を流す"
    );
    dir
}

/// unix socket の置き場。`sockaddr_un` は 108 byte までなので、TMPDIR が長いときは相対パス
/// (cwd = crate の直下) に置く。
fn socket_dir() -> PathBuf {
    let name = format!(
        "dtako-pg-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    );
    let abs = std::env::temp_dir().join(&name);
    if abs.as_os_str().len() + 1 + SOCKET_FILE.len() < 100 {
        abs
    } else {
        PathBuf::from(format!(".{name}"))
    }
}

/// 接続 1 本 (`T` = `Client`・`PgClient`) と、その接続の task。
pub struct Held<T> {
    pub inner: T,
    task: JoinHandle<()>,
}

impl<T> Held<T> {
    /// 接続を閉じ切る (次の接続を張る前に必ず呼ぶ)。`inner` の写しが残っていると閉じないので、時間切れで落とす。
    pub async fn close(self) {
        drop(self.inner);
        tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .expect("接続が閉じない (Client の写しが残っている)")
            .unwrap();
    }

    /// 接続の task を止める (= 接続が切れた状態にする)。`inner` はそのまま返す。
    pub async fn sever(self) -> T {
        self.task.abort();
        let _ = self.task.await;
        self.inner
    }
}

pub struct Embedded {
    server: Option<PgliteServer>,
    socket_dir: PathBuf,
}

impl Embedded {
    /// 起動 → `init_local_db.sql` → 空の `_sqlx_migrations` → migrations (1 ファイル 1 transaction) →
    /// `local_app_grants.sql` → 接続ロールの検査。
    pub async fn start() -> Self {
        let socket_dir = socket_dir();
        std::fs::create_dir_all(&socket_dir).unwrap();
        let server = PgliteServer::builder()
            .temporary()
            .database("postgres")
            .unix(socket_dir.join(SOCKET_FILE))
            .start()
            .unwrap();
        let this = Self {
            server: Some(server),
            socket_dir,
        };
        this.migrate().await;
        // 全テストがここを通る: 以降の接続ロールが RLS を素通りしない・表の所有者でないこと
        let mut app = this.client(APP_ROLE).await;
        assert_rls_applies(&mut app.inner).await;
        app.close().await;
        this
    }

    async fn connect(&self, user: &str, options: Option<&str>) -> Held<Client> {
        let mut config = tokio_postgres::Config::new();
        config
            .host_path(&self.socket_dir)
            .port(5432)
            .user(user)
            .dbname("postgres");
        if let Some(options) = options {
            config.options(options);
        }
        let (client, connection) = config.connect(NoTls).await.unwrap();
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        Held {
            inner: client,
            task,
        }
    }

    /// superuser の接続 (migration と、テスト用のロールを作る準備だけに使う)。
    pub async fn superuser(&self) -> Held<Client> {
        self.connect(SUPERUSER, Some(SEARCH_PATH)).await
    }

    /// 流す順は ippoan/alc-vein-worker の `container/start.sh` (staging の DB の image) と同じ。
    /// 種と PgBouncer は無い (テストが自分でテナントと行を作り、直結で流す)。
    async fn migrate(&self) {
        let dir = migrations_dir();
        let read = |p: PathBuf| std::fs::read_to_string(p).unwrap();
        let su = self.superuser().await;
        su.inner
            .batch_execute(&read(dir.join("scripts/init_local_db.sql")))
            .await
            .unwrap();
        su.inner
            .batch_execute("CREATE TABLE IF NOT EXISTS alc_api._sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)")
            .await
            .unwrap();
        let mut files: Vec<PathBuf> = std::fs::read_dir(dir.join("migrations"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "sql"))
            .collect();
        files.sort();
        assert!(!files.is_empty());
        for file in files {
            let name = file.file_name().unwrap().to_owned();
            let sql = read(file);
            if let Err(e) = su
                .inner
                .batch_execute(&format!("BEGIN;\n{sql}\n;COMMIT;"))
                .await
            {
                panic!("migration {name:?}: {:?}", e.code());
            }
        }
        su.inner
            .batch_execute(&read(dir.join("scripts/local_app_grants.sql")))
            .await
            .unwrap();
        // RLS が効く alc_api_app で繋がせる (init_local_db.sql では NOLOGIN)
        su.inner
            .batch_execute("ALTER ROLE alc_api_app LOGIN")
            .await
            .unwrap();
        su.close().await;
    }

    /// `role` で繋いだ `PgClient` (worker が `src/db.rs` で作るものと同じ型。準備にも、検査する関数にも渡す)。
    pub async fn client(&self, role: &str) -> Held<PgClient> {
        let Held { inner, task } = self.connect(role, None).await;
        Held {
            inner: PgClient::new(inner),
            task,
        }
    }

    /// `role` で繋いだ `PgClient` を、口の State と同じ持ち方 (async の Mutex) にしたもの。
    /// 準備の helper には `lock().await` で `&mut PgClient` を渡す (接続は 1 本なので、口と準備で同じものを使う)。
    pub async fn shared(&self, role: &str) -> Held<Arc<futures_util::lock::Mutex<PgClient>>> {
        let Held { inner, task } = self.client(role).await;
        Held {
            inner: Arc::new(futures_util::lock::Mutex::new(inner)),
            task,
        }
    }

    /// `role` で繋いだ素の接続 (テナントを設定しない。`PgClient` に包まない)。
    pub async fn raw(&self, role: &str) -> Held<Client> {
        self.connect(role, None).await
    }

    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        if let Some(server) = self.server.take() {
            server.shutdown().unwrap();
        }
        let _ = std::fs::remove_dir_all(&self.socket_dir);
    }
}

impl Drop for Embedded {
    fn drop(&mut self) {
        self.stop();
    }
}

/// superuser / BYPASSRLS で繋ぐと RLS を素通りして、テナント分離のテストが意味を失う。
/// この crate が触る表 ([`TABLES`]) には FORCE ROW LEVEL SECURITY が無いので、表の所有者でも同じ。
async fn assert_rls_applies(db: &mut PgClient) {
    let tables: Vec<String> = TABLES.iter().map(|t| t.to_string()).collect();
    let (bypass, known, owned): (bool, i64, i64) = db
        .tenant_tx(Uuid::new_v4(), move |tx| {
            Box::pin(async move {
                let row = tx
                    .query_typed_one(
                        "SELECT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user), \
                         (SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'alc_api' AND tablename = ANY($1)), \
                         (SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'alc_api' \
                          AND tablename = ANY($1) AND tableowner = current_user)",
                        &[(&tables, Type::TEXT_ARRAY)],
                    )
                    .await?;
                Ok((row.get(0), row.get(1), row.get(2)))
            })
        })
        .await
        .unwrap();
    assert!(!bypass, "RLS を素通りするロールで繋いでいる");
    assert_eq!(known, TABLES.len() as i64, "検査の対象の表が見つからない");
    assert_eq!(owned, 0, "表の所有者で繋いでいる");
}

pub async fn tenant(db: &mut PgClient, name: &str) -> Uuid {
    let tenant_id = Uuid::new_v4();
    let name = name.to_owned();
    let n = db
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                tx.execute_typed(
                    "INSERT INTO tenants (id, name) VALUES ($1, $2)",
                    &[(&tenant_id, Type::UUID), (&name, Type::TEXT)],
                )
                .await
            })
        })
        .await
        .unwrap();
    assert_eq!(n, 1, "INSERT INTO tenants");
    tenant_id
}

/// アップロード履歴 1 行。`age_secs` = 何秒前に作られたことにするか (並び順の検査用)。
pub async fn upload(
    db: &mut PgClient,
    tenant_id: Uuid,
    filename: &str,
    status: &str,
    r2_zip_key: Option<&str>,
    age_secs: f64,
) -> Uuid {
    let id = Uuid::new_v4();
    let filename = filename.to_owned();
    let status = status.to_owned();
    let r2_zip_key = r2_zip_key.map(str::to_owned);
    let n = db
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                tx.execute_typed(
                    "INSERT INTO dtako_upload_history (id, tenant_id, filename, status, r2_zip_key, created_at) \
                     VALUES ($1, $2, $3, $4, $5, now() - $6 * interval '1 second')",
                    &[
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&filename, Type::TEXT),
                        (&status, Type::TEXT),
                        (&r2_zip_key, Type::TEXT),
                        (&age_secs, Type::FLOAT8),
                    ],
                )
                .await
            })
        })
        .await
        .unwrap();
    assert_eq!(n, 1, "INSERT INTO dtako_upload_history");
    id
}

/// 運行 1 行 (`UNIQUE (tenant_id, unko_no, crew_role)`。同じ運行NO を乗務員 2 人ぶん作るときは `crew_role` を変える)。
pub async fn operation(
    db: &mut PgClient,
    tenant_id: Uuid,
    unko_no: &str,
    crew_role: i32,
    has_kudgivt: bool,
) {
    let unko_no = unko_no.to_owned();
    let n = db
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                tx.execute_typed(
                    "INSERT INTO dtako_operations (tenant_id, unko_no, crew_role, reading_date, has_kudgivt) \
                     VALUES ($1, $2, $3, DATE '2026-01-01', $4)",
                    &[
                        (&tenant_id, Type::UUID),
                        (&unko_no, Type::TEXT),
                        (&crew_role, Type::INT4),
                        (&has_kudgivt, Type::BOOL),
                    ],
                )
                .await
            })
        })
        .await
        .unwrap();
    assert_eq!(n, 1, "INSERT INTO dtako_operations");
}

/// 運行を `n` 行まとめて作る (運行NO は `<prefix>1` 〜 `<prefix>n`、未分割)。運行NO の一覧を返す。
pub async fn operations(db: &mut PgClient, tenant_id: Uuid, prefix: &str, n: i32) -> Vec<String> {
    let owned_prefix = prefix.to_owned();
    let inserted = db
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                tx.execute_typed(
                    "INSERT INTO dtako_operations (tenant_id, unko_no, reading_date) \
                     SELECT $1, $2 || g::text, DATE '2026-01-01' FROM generate_series(1, $3) g",
                    &[
                        (&tenant_id, Type::UUID),
                        (&owned_prefix, Type::TEXT),
                        (&n, Type::INT4),
                    ],
                )
                .await
            })
        })
        .await
        .unwrap();
    assert_eq!(inserted, n as u64, "INSERT INTO dtako_operations");
    (1..=n).map(|g| format!("{prefix}{g}")).collect()
}

/// テナントの運行の `(unko_no, crew_role, has_kudgivt)` (運行NO・crew_role の順)。
pub async fn kudgivt_flags(db: &mut PgClient, tenant_id: Uuid) -> Vec<(String, i32, bool)> {
    db.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    "SELECT unko_no, crew_role, has_kudgivt FROM dtako_operations \
                     WHERE tenant_id = $1 ORDER BY unko_no, crew_role",
                    &[(&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(rows
                .into_iter()
                .map(|r| (r.get(0), r.get(1), r.get(2)))
                .collect())
        })
    })
    .await
    .unwrap()
}

/// 任意の文を流す (`$1` = tenant_id を必ず使う文)。影響した行数を返す。値はテストの固定値を文に直に書く。
pub async fn exec(db: &mut PgClient, tenant_id: Uuid, sql: &str) -> u64 {
    let sql = sql.to_owned();
    db.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move { tx.execute_typed(&sql, &[(&tenant_id, Type::UUID)]).await })
    })
    .await
    .unwrap()
}

/// `query` (`$1` = tenant_id を必ず使う SELECT) の結果を、行ごとの JSON にして返す (列名 → 値)。
pub async fn rows_json(db: &mut PgClient, tenant_id: Uuid, query: &str) -> Vec<serde_json::Value> {
    let sql = format!("SELECT to_jsonb(t)::text FROM ({query}) t");
    let rows: Vec<String> = db
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                let rows = tx.query_typed(&sql, &[(&tenant_id, Type::UUID)]).await?;
                Ok(rows.into_iter().map(|r| r.get(0)).collect())
            })
        })
        .await
        .unwrap();
    let parse = |text: &String| serde_json::from_str(text).unwrap();
    rows.iter().map(parse).collect()
}

/// 乗務員 1 行。`code` (社員番号) と `driver_cd` はどちらも無しにできる。`deleted` なら論理削除済みにする。
pub async fn employee(
    db: &mut PgClient,
    tenant_id: Uuid,
    code: Option<&str>,
    driver_cd: Option<&str>,
    name: &str,
    deleted: bool,
) -> Uuid {
    let id = Uuid::new_v4();
    let (code, driver_cd) = (code.map(str::to_owned), driver_cd.map(str::to_owned));
    let name = name.to_owned();
    let n = db
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                tx.execute_typed(
                    "INSERT INTO employees (id, tenant_id, code, driver_cd, name, deleted_at) \
                     VALUES ($1, $2, $3, $4, $5, CASE WHEN $6 THEN now() END)",
                    &[
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&code, Type::TEXT),
                        (&driver_cd, Type::TEXT),
                        (&name, Type::TEXT),
                        (&deleted, Type::BOOL),
                    ],
                )
                .await
            })
        })
        .await
        .unwrap();
    assert_eq!(n, 1, "INSERT INTO employees");
    id
}
