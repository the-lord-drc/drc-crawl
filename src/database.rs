use anyhow::Result;
use chrono::{Duration, Utc};
use sqlx::{postgres::PgPoolOptions, sqlite::SqlitePoolOptions, Pool, Postgres, Sqlite};
use std::path::Path;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::crawler::DiscoveredLink;

pub trait DbAdapter: Send + Sync {
    async fn setup_schema(&self) -> Result<()>;
    async fn save_links(&self, job_id: &Uuid, links: &[DiscoveredLink], expire_days: i64) -> Result<()>;
    async fn get_known_urls(&self) -> Result<Vec<String>>;
}

pub struct PostgresAdapter {
    pool: Pool<Postgres>,
}

impl PostgresAdapter {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(50)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }
}

impl DbAdapter for PostgresAdapter {
    async fn setup_schema(&self) -> Result<()> {
        let query = r#"
            CREATE TABLE IF NOT EXISTS crawled_links (
                id UUID PRIMARY KEY,
                job_id UUID NOT NULL,
                url TEXT NOT NULL UNIQUE,
                status_code INT NOT NULL,
                content_type TEXT NOT NULL,
                response_time_ms BIGINT NOT NULL,
                expire_date TIMESTAMP WITH TIME ZONE NOT NULL,
                discovered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_url_lookup ON crawled_links(url);
        "#;
        sqlx::query(query).execute(&self.pool).await?;
        Ok(())
    }

    async fn save_links(&self, job_id: &Uuid, links: &[DiscoveredLink], expire_days: i64) -> Result<()> {
        let expire_date = Utc::now() + Duration::days(expire_days);
        for link in links {
            let id = Uuid::new_v4();
            let content_type_str = format!("{:?}", link.content_type);
            sqlx::query(
                r#"
                INSERT INTO crawled_links (id, job_id, url, status_code, content_type, response_time_ms, expire_date)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (url) DO UPDATE 
                SET status_code = EXCLUDED.status_code,
                    expire_date = EXCLUDED.expire_date,
                    response_time_ms = EXCLUDED.response_time_ms
                "#,
            )
            .bind(id)
            .bind(job_id)
            .bind(&link.url)
            .bind(link.status_code as i32)
            .bind(&content_type_str)
            .bind(link.response_time_ms as i64)
            .bind(expire_date)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn get_known_urls(&self) -> Result<Vec<String>> {
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT url FROM crawled_links WHERE expire_date > $1"
        )
        .bind(Utc::now())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }
}

pub struct SqliteAdapter {
    pool: Pool<Sqlite>,
}

impl SqliteAdapter {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }
}

impl DbAdapter for SqliteAdapter {
    async fn setup_schema(&self) -> Result<()> {
        let query = r#"
            CREATE TABLE IF NOT EXISTS crawled_links (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                status_code INTEGER NOT NULL,
                content_type TEXT NOT NULL,
                response_time_ms INTEGER NOT NULL,
                expire_date TEXT NOT NULL,
                discovered_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_url_lookup ON crawled_links(url);
        "#;
        sqlx::query(query).execute(&self.pool).await?;
        Ok(())
    }

    async fn save_links(&self, job_id: &Uuid, links: &[DiscoveredLink], expire_days: i64) -> Result<()> {
        let expire_date = (Utc::now() + Duration::days(expire_days)).to_rfc3339();
        let job_id_str = job_id.to_string();
        for link in links {
            let id = Uuid::new_v4().to_string();
            let content_type_str = format!("{:?}", link.content_type);
            sqlx::query(
                r#"
                INSERT INTO crawled_links (id, job_id, url, status_code, content_type, response_time_ms, expire_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    status_code = excluded.status_code,
                    expire_date = excluded.expire_date
                "#,
            )
            .bind(&id)
            .bind(&job_id_str)
            .bind(&link.url)
            .bind(link.status_code as i32)
            .bind(&content_type_str)
            .bind(link.response_time_ms as i64)
            .bind(&expire_date)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn get_known_urls(&self) -> Result<Vec<String>> {
        let now = Utc::now().to_rfc3339();
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT url FROM crawled_links WHERE expire_date > ?"
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }
}

pub struct JsonLinesAdapter {
    filepath: String,
}

impl JsonLinesAdapter {
    pub fn new(filepath: &str) -> Self {
        Self {
            filepath: filepath.to_string(),
        }
    }
}

impl DbAdapter for JsonLinesAdapter {
    async fn setup_schema(&self) -> Result<()> {
        Ok(())
    }

    async fn save_links(&self, job_id: &Uuid, links: &[DiscoveredLink], expire_days: i64) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.filepath)
            .await?;
        let expire_date = Utc::now() + Duration::days(expire_days);
        for link in links {
            let record = serde_json::json!({
                "id": Uuid::new_v4().to_string(),
                "job_id": job_id.to_string(),
                "url": link.url,
                "status_code": link.status_code,
                "content_type": format!("{:?}", link.content_type),
                "response_time_ms": link.response_time_ms,
                "expire_date": expire_date.to_rfc3339(),
                "timestamp": Utc::now().to_rfc3339(),
            });
            let mut line = serde_json::to_string(&record)?;
            line.push('\n');
            file.write_all(line.as_bytes()).await?;
        }
        Ok(())
    }

    async fn get_known_urls(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }
}

pub enum Database {
    Postgres(PostgresAdapter),
    Sqlite(SqliteAdapter),
    JsonLines(JsonLinesAdapter),
}

impl Database {
    pub async fn setup_schema(&self) -> Result<()> {
        match self {
            Database::Postgres(db) => db.setup_schema().await,
            Database::Sqlite(db) => db.setup_schema().await,
            Database::JsonLines(db) => db.setup_schema().await,
        }
    }

    pub async fn save_links(&self, job_id: &Uuid, links: &[DiscoveredLink], expire_days: i64) -> Result<()> {
        match self {
            Database::Postgres(db) => db.save_links(job_id, links, expire_days).await,
            Database::Sqlite(db) => db.save_links(job_id, links, expire_days).await,
            Database::JsonLines(db) => db.save_links(job_id, links, expire_days).await,
        }
    }

    pub async fn get_known_urls(&self) -> Result<Vec<String>> {
        match self {
            Database::Postgres(db) => db.get_known_urls().await,
            Database::Sqlite(db) => db.get_known_urls().await,
            Database::JsonLines(db) => db.get_known_urls().await,
        }
    }

    pub async fn clear_links(&self) -> Result<()> {
        match self {
            Database::Postgres(db) => {
                sqlx::query("DELETE FROM crawled_links")
                    .execute(&db.pool)
                    .await?;
            }
            Database::Sqlite(db) => {
                sqlx::query("DELETE FROM crawled_links")
                    .execute(&db.pool)
                    .await?;
            }
            Database::JsonLines(db) => {
                let _ = tokio::fs::write(&db.filepath, "").await;
            }
        }
        Ok(())
    }
}

pub async fn initialize_database(db_type: &str, connection_string: &str) -> Result<Database> {
    let db = match db_type.to_lowercase().as_str() {
        "postgres" => Database::Postgres(PostgresAdapter::new(connection_string).await?),
        "sqlite" => {
            let path_part = connection_string
                .strip_prefix("sqlite:")
                .unwrap_or(connection_string);
            let file_path = path_part.split('?').next().unwrap_or(path_part);
            if let Some(parent) = Path::new(file_path).parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            Database::Sqlite(SqliteAdapter::new(connection_string).await?)
        },
        "jsonl" => {
            if let Some(parent) = Path::new(connection_string).parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            Database::JsonLines(JsonLinesAdapter::new(connection_string))
        },
        _ => return Err(anyhow::anyhow!("Unsupported database type: {}", db_type)),
    };
    db.setup_schema().await?;
    Ok(db)
}