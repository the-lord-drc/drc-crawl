mod crawler;
mod database;
mod sitemap;

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use clap::{Parser, Subcommand};
use inquire::{Confirm, Select, Text};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tower_http::cors::CorsLayer;
use uuid::Uuid;

use crawler::{CrawlConfig, CrawlerEngine, DiscoveredLink};
use database::{initialize_database, Database};

#[derive(Parser)]
#[command(name = "drc-crawl", version = "1.0", about = "CrabSitemap - High Performance Crawler")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the crawler in web panel or direct CLI mode
    Run {
        #[command(subcommand)]
        mode: RunMode,
    },
    /// Manage configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Manage background service (Linux systemd)
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
}

#[derive(Subcommand)]
enum RunMode {
    /// Interactive setup then start web dashboard
    Web,
    /// Single crawl with optional URL (interactive if not provided)
    Cli {
        /// Target URL to crawl
        url: Option<String>,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Delete config.json and data directory (if exists)
    Reset,
    /// Show current configuration
    Show,
}

#[derive(Subcommand)]
enum ServiceAction {
    /// Install drc-crawl as a systemd service
    Install,
    /// Uninstall the systemd service
    Uninstall,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

struct CrawlJob {
    id: Uuid,
    url: String,
    status: JobStatus,
    links: Vec<DiscoveredLink>,
    total_requests: usize,
    failed_requests: usize,
    total_time_ms: u128,
}

struct AppState {
    jobs: RwLock<HashMap<Uuid, CrawlJob>>,
    db: Arc<Database>,
    config: Config,
    sitemap_path: Option<String>,
}

type SharedState = Arc<AppState>;

#[derive(Serialize, Deserialize, Clone)]
struct Config {
    db_type: String,
    db_url: String,
    sitemap_path: String,
    schedule_interval: Option<String>,
    #[serde(default = "default_expire_days")]
    expire_days: i64,
}

fn default_expire_days() -> i64 {
    30
}

fn load_config() -> Option<Config> {
    if let Ok(content) = fs::read_to_string("config.json") {
        serde_json::from_str(&content).ok()
    } else {
        None
    }
}

fn save_config(config: &Config) {
    if let Ok(json) = serde_json::to_string_pretty(config) {
        let _ = fs::write("config.json", json);
    }
}

#[derive(serde::Deserialize)]
struct StartCrawlRequest {
    url: String,
    max_depth: usize,
    concurrency_limit: usize,
    max_total_requests: Option<usize>,
    timeout_seconds: Option<u64>,
    allowed_domains: Option<Vec<String>>,
    blacklist_patterns: Option<Vec<String>>,
    track_images: Option<bool>,
    track_videos: Option<bool>,
    track_documents: Option<bool>,
}

#[derive(serde::Serialize)]
struct JobStatusResponse {
    id: String,
    url: String,
    status: JobStatus,
    total_found_links: usize,
    total_requests: usize,
    failed_requests: usize,
    total_time_ms: u128,
}

// ----- API: get current configuration -----
async fn config_handler(State(state): State<SharedState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "db_type": state.config.db_type,
        "db_url": state.config.db_url,
        "sitemap_path": state.config.sitemap_path,
        "schedule_interval": state.config.schedule_interval,
        "expire_days": state.config.expire_days,
    }))
}

// ----- API: update configuration -----
#[derive(Deserialize)]
struct UpdateConfigRequest {
    sitemap_path: Option<String>,
    schedule_interval: Option<String>,
    expire_days: Option<i64>,
}

async fn update_config_handler(
    State(_state): State<SharedState>,
    Json(payload): Json<UpdateConfigRequest>,
) -> impl IntoResponse {
    let mut cfg = match load_config() {
        Some(c) => c,
        None => return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error":"No config file found"}))).into_response(),
    };
    if let Some(p) = payload.sitemap_path {
        cfg.sitemap_path = p;
    }
    if let Some(s) = payload.schedule_interval {
        cfg.schedule_interval = if s.is_empty() { None } else { Some(s) };
    }
    if let Some(d) = payload.expire_days {
        if d > 0 {
            cfg.expire_days = d;
        }
    }
    save_config(&cfg);
    (StatusCode::OK, Json(serde_json::json!({ "status": "updated" }))).into_response()
}

// ----- API: reset configuration -----
async fn reset_config_handler(State(_state): State<SharedState>) -> impl IntoResponse {
    let _ = fs::remove_file("config.json");
    (StatusCode::OK, Json(serde_json::json!({ "status": "reset" })))
}

// ----- API: clear database -----
async fn clear_database_handler(State(state): State<SharedState>) -> impl IntoResponse {
    match state.db.clear_links().await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({ "status": "cleared" }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

// ----- Crawl handlers -----
async fn start_crawl_handler(
    State(state): State<SharedState>,
    Json(payload): Json<StartCrawlRequest>,
) -> impl IntoResponse {
    let job_id = Uuid::new_v4();

    let known_urls = state.db.get_known_urls().await.unwrap_or_default();

    let config = CrawlConfig {
        start_url: payload.url.clone(),
        allowed_domains: payload.allowed_domains.unwrap_or_default(),
        known_urls,
        max_depth: payload.max_depth,
        max_total_requests: payload.max_total_requests,
        timeout_seconds: payload.timeout_seconds,
        concurrency_limit: payload.concurrency_limit,
        user_agent: "CrabWebEngine/1.0".to_string(),
        blacklist_patterns: payload.blacklist_patterns.unwrap_or_default(),
        track_images: payload.track_images.unwrap_or(false),
        track_videos: payload.track_videos.unwrap_or(false),
        track_documents: payload.track_documents.unwrap_or(false),
    };

    {
        let mut lock = state.jobs.write().unwrap();
        lock.insert(
            job_id,
            CrawlJob {
                id: job_id,
                url: payload.url,
                status: JobStatus::Pending,
                links: vec![],
                total_requests: 0,
                failed_requests: 0,
                total_time_ms: 0,
            },
        );
    }

    let state_clone = state.clone();
    let expire_days = state.config.expire_days;
    tokio::spawn(async move {
        {
            let mut lock = state_clone.jobs.write().unwrap();
            if let Some(j) = lock.get_mut(&job_id) {
                j.status = JobStatus::Running;
            }
        }

        if let Ok(engine) = CrawlerEngine::new(config) {
            match engine.run().await {
                Ok(report) => {
                    {
                        let mut lock = state_clone.jobs.write().unwrap();
                        if let Some(j) = lock.get_mut(&job_id) {
                            j.status = JobStatus::Completed;
                            j.links = report.links.clone();
                            j.total_requests = report.total_requests;
                            j.failed_requests = report.failed_requests;
                            j.total_time_ms = report.total_time_ms;
                        }
                    }
                    let _ = state_clone.db.save_links(&job_id, &report.links, expire_days).await;
                    if let Some(ref path) = state_clone.sitemap_path {
                        let _ = sitemap::merge_and_save_sitemap(path, &report.links);
                    }
                }
                Err(e) => {
                    eprintln!("❌ Crawl failed: {:?}", e);
                    let mut lock = state_clone.jobs.write().unwrap();
                    if let Some(j) = lock.get_mut(&job_id) {
                        j.status = JobStatus::Failed;
                    }
                }
            }
        }
    });

    (StatusCode::ACCEPTED, Json(serde_json::json!({ "job_id": job_id.to_string() })))
}

async fn get_status_handler(
    Path(id_str): Path<String>,
    State(state): State<SharedState>,
) -> impl IntoResponse {
    let job_id = match Uuid::parse_str(&id_str) {
        Ok(id) => id,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid UUID format").into_response(),
    };

    let lock = state.jobs.read().unwrap();
    if let Some(job) = lock.get(&job_id) {
        let response = JobStatusResponse {
            id: job.id.to_string(),
            url: job.url.clone(),
            status: job.status.clone(),
            total_found_links: job.links.len(),
            total_requests: job.total_requests,
            failed_requests: job.failed_requests,
            total_time_ms: job.total_time_ms,
        };
        (StatusCode::OK, Json(response)).into_response()
    } else {
        (StatusCode::NOT_FOUND, "Job not found").into_response()
    }
}

async fn export_sitemap_handler(
    Path(id_str): Path<String>,
    State(state): State<SharedState>,
) -> Response {
    let job_id = match Uuid::parse_str(&id_str) {
        Ok(id) => id,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid UUID format").into_response(),
    };

    let lock = state.jobs.read().unwrap();
    if let Some(job) = lock.get(&job_id) {
        if !matches!(job.status, JobStatus::Completed) {
            return (StatusCode::BAD_REQUEST, "Sitemap not ready or crawl failed").into_response();
        }

        let xml = sitemap::generate_sitemap_xml(&job.links);
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/xml; charset=utf-8")
            .header("Content-Disposition", "attachment; filename=\"sitemap.xml\"")
            .body(axum::body::Body::from(xml))
            .unwrap()
    } else {
        (StatusCode::NOT_FOUND, "Job not found").into_response()
    }
}

fn prompt_database_config() -> Result<(String, String)> {
    let db_types = vec!["sqlite", "postgres", "jsonl"];
    let db_type = Select::new("Select database type:", db_types.clone())
        .prompt()
        .unwrap_or("sqlite");

    let default_conn = match db_type {
        "sqlite" => "sqlite:data/crawler.db?mode=rwc".to_string(),
        "postgres" => {
            let (db_name, user, pass) = generate_random_pg_creds();
            format!("postgres://{}:{}@localhost/{}", user, pass, db_name)
        }
        _ => "output/links.jsonl".to_string(),
    };

    let conn_str = Text::new(&format!("Connection string ({}):", db_type))
        .with_default(&default_conn)
        .prompt()?;

    Ok((db_type.to_string(), conn_str))
}

fn prompt_sitemap_path() -> String {
    let default_path = "./sitemap.xml".to_string();
    Text::new("Where to save/update sitemap.xml (press Enter for current directory):")
        .with_default(&default_path)
        .prompt()
        .unwrap_or(default_path)
}

fn prompt_schedule() -> Option<String> {
    let enable = Confirm::new("Enable scheduled background crawling?")
        .with_default(false)
        .prompt()
        .unwrap_or(false);

    if enable {
        let interval = Text::new("Interval (e.g., 24h, 6h, 30m, 1m):")
            .with_default("24h")
            .prompt()
            .unwrap_or_else(|_| "24h".to_string());
        Some(interval)
    } else {
        None
    }
}

fn prompt_expire_days() -> i64 {
    let days = Text::new("Link expiration (days, default 30):")
        .with_default("30")
        .prompt()
        .unwrap_or_else(|_| "30".to_string());
    days.parse::<i64>().unwrap_or(30)
}

fn prompt_cli_crawl_params() -> Result<(String, CrawlConfig)> {
    let url = Text::new("Target URL:").prompt()?;
    let depth = Text::new("Max crawl depth (1-10):")
        .with_default("3")
        .prompt()?
        .parse::<usize>()
        .unwrap_or(3);
    let concurrency = Text::new("Concurrency limit (1-100):")
        .with_default("10")
        .prompt()?
        .parse::<usize>()
        .unwrap_or(10);
    let max_requests = Text::new("Max total requests (optional, press Enter to skip):")
        .prompt()
        .ok()
        .and_then(|s| s.parse::<usize>().ok());
    let timeout = Text::new("Global timeout in seconds (optional):")
        .prompt()
        .ok()
        .and_then(|s| s.parse::<u64>().ok());

    let config = CrawlConfig {
        start_url: url.clone(),
        allowed_domains: vec![],
        known_urls: vec![],
        max_depth: depth,
        max_total_requests: max_requests,
        timeout_seconds: timeout,
        concurrency_limit: concurrency,
        user_agent: "CrabSitemapCLI/1.0".into(),
        blacklist_patterns: vec![],
        track_images: true,
        track_videos: false,
        track_documents: false,
    };
    Ok((url, config))
}

fn parse_duration(input: &str) -> Option<Duration> {
    let input = input.trim().to_lowercase();
    if let Some(suffix_pos) = input.find(|c: char| !c.is_ascii_digit() && c != '.') {
        let number: f64 = input[..suffix_pos].parse().ok()?;
        let suffix = &input[suffix_pos..];
        let seconds = match suffix {
            "s" => number,
            "m" => number * 60.0,
            "h" => number * 3600.0,
            _ => return None,
        };
        Some(Duration::from_secs_f64(seconds))
    } else {
        None
    }
}

async fn run_crawl_and_update(
    config: CrawlConfig,
    db: Arc<Database>,
    sitemap_path: &str,
    expire_days: i64,
) -> Result<()> {
    let engine = CrawlerEngine::new(config)?;
    let report = engine.run().await?;
    println!(
        "✅ Crawl completed: {} links found ({} requests, {} failed) in {}ms",
        report.links.len(),
        report.total_requests,
        report.failed_requests,
        report.total_time_ms
    );
    let job_id = Uuid::new_v4();
    db.save_links(&job_id, &report.links, expire_days).await?;
    println!("💾 Saved to database.");
    sitemap::merge_and_save_sitemap(sitemap_path, &report.links)?;
    println!("📄 Sitemap updated at: {}", sitemap_path);
    Ok(())
}

// ----- Service helpers -----
fn install_systemd_service() -> Result<()> {
    let exe = std::env::current_exe()?;
    let exe_path = exe.to_string_lossy().to_string();
    let working_dir = exe.parent().map(|p| p.to_string_lossy().to_string()).unwrap_or_else(|| "/".to_string());

    let unit_content = format!(
        r#"[Unit]
Description=DRC Crawl Web Panel
After=network.target

[Service]
ExecStart={exe} run web
WorkingDirectory={dir}
Restart=always
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"#,
        exe = exe_path,
        dir = working_dir
    );

    let unit_path = PathBuf::from("/etc/systemd/system/drc-crawl.service");

    // Check if running as root
    if !nix::unistd::Uid::effective().is_root() {
        anyhow::bail!("This command requires root privileges. Please run with sudo.");
    }

    // Write the unit file
    let mut file = fs::File::create(&unit_path)?;
    file.write_all(unit_content.as_bytes())?;

    // Reload systemd and enable/start service
    std::process::Command::new("systemctl")
        .args(["daemon-reload"])
        .status()?;
    std::process::Command::new("systemctl")
        .args(["enable", "drc-crawl"])
        .status()?;
    std::process::Command::new("systemctl")
        .args(["start", "drc-crawl"])
        .status()?;

    println!("✅ Service installed and started.");
    Ok(())
}

fn uninstall_systemd_service() -> Result<()> {
    if !nix::unistd::Uid::effective().is_root() {
        anyhow::bail!("This command requires root privileges. Please run with sudo.");
    }

    // Stop and disable
    std::process::Command::new("systemctl")
        .args(["stop", "drc-crawl"])
        .status()?;
    std::process::Command::new("systemctl")
        .args(["disable", "drc-crawl"])
        .status()?;

    let unit_path = PathBuf::from("/etc/systemd/system/drc-crawl.service");
    if unit_path.exists() {
        fs::remove_file(&unit_path)?;
    }
    // Reload systemd
    std::process::Command::new("systemctl")
        .args(["daemon-reload"])
        .status()?;
    println!("✅ Service stopped and removed.");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { mode } => match mode {
            RunMode::Web => {
                println!("\n🧛‍♂️ DRC Sitemap Web Setup\n");

                let config = if let Some(cfg) = load_config() {
                    println!("📂 Loaded existing configuration from config.json");
                    println!("   To reconfigure, delete config.json and restart.\n");
                    cfg
                } else {
                    let (db_type, db_url) = prompt_database_config()?;
                    let sitemap_path = prompt_sitemap_path();
                    let schedule_interval = prompt_schedule();
                    let expire_days = prompt_expire_days();

                    let cfg = Config {
                        db_type,
                        db_url,
                        sitemap_path,
                        schedule_interval,
                        expire_days,
                    };
                    save_config(&cfg);
                    cfg
                };

                let db = Arc::new(initialize_database(&config.db_type, &config.db_url).await?);
                println!("✅ Database connected.");

                let state = Arc::new(AppState {
                    jobs: RwLock::new(HashMap::new()),
                    db: db.clone(),
                    config: config.clone(),
                    sitemap_path: Some(config.sitemap_path.clone()),
                });

                // Background scheduler
                if let Some(ref interval_str) = config.schedule_interval {
                    let interval_owned = interval_str.clone();
                    if let Some(duration) = parse_duration(&interval_owned) {
                        let state_clone = state.clone();
                        let path_clone = config.sitemap_path.clone();
                        let expire_days = config.expire_days;
                        tokio::spawn(async move {
                            loop {
                                tokio::time::sleep(duration).await;
                                println!(
                                    "⏰ Scheduled crawl triggered (interval: {})",
                                    interval_owned
                                );
                                if let Ok(known_urls) = state_clone.db.get_known_urls().await {
                                    if let Some(first_url) = known_urls.first() {
                                        let crawl_config = CrawlConfig {
                                            start_url: first_url.clone(),
                                            allowed_domains: vec![],
                                            known_urls,
                                            max_depth: 3,
                                            max_total_requests: None,
                                            timeout_seconds: None,
                                            concurrency_limit: 5,
                                            user_agent: "CrabScheduler/1.0".into(),
                                            blacklist_patterns: vec![],
                                            track_images: true,
                                            track_videos: false,
                                            track_documents: false,
                                        };
                                        if let Err(e) = run_crawl_and_update(
                                            crawl_config,
                                            state_clone.db.clone(),
                                            &path_clone,
                                            expire_days,
                                        )
                                        .await
                                        {
                                            eprintln!("❌ Scheduled crawl failed: {}", e);
                                        }
                                    } else {
                                        eprintln!(
                                            "⚠️ No known URLs to crawl. Skipping scheduled crawl."
                                        );
                                    }
                                }
                            }
                        });
                        println!("⏱️ Background scheduler enabled: every {}", interval_str);
                    } else {
                        eprintln!("⚠️ Invalid interval format: '{}'", interval_str);
                    }
                }

                let app = Router::new()
                    .route("/", get(|| async { Html(include_str!("../index.html")) }))
                    .route("/api/config", get(config_handler).post(update_config_handler).delete(reset_config_handler))
                    .route("/api/database", delete(clear_database_handler))
                    .route("/api/crawl/start", post(start_crawl_handler))
                    .route("/api/crawl/status/:id", get(get_status_handler))
                    .route("/api/crawl/export/:id", get(export_sitemap_handler))
                    .layer(CorsLayer::permissive())
                    .with_state(state);

                let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 786));
                println!("⚡️ Web Panel ready on http://localhost:786");
                let listener = tokio::net::TcpListener::bind(addr).await?;
                axum::serve(listener, app).await?;
            }

            RunMode::Cli { url } => {
                println!("\n🧛‍♂️ DRC Sitemap CLI\n");

                let (db_type, db_url, expire_days) = if let Some(cfg) = load_config() {
                    println!("📂 Loaded saved configuration.");
                    (cfg.db_type, cfg.db_url, cfg.expire_days)
                } else {
                    let (db_type, db_url) = prompt_database_config()?;
                    let expire_days = prompt_expire_days();
                    (db_type, db_url, expire_days)
                };

                let db = Arc::new(initialize_database(&db_type, &db_url).await?);
                println!("✅ Database connected.");

                let sitemap_path = if let Some(cfg) = load_config() {
                    cfg.sitemap_path
                } else {
                    prompt_sitemap_path()
                };

                let (url, config) = if let Some(ref u) = url {
                    let cfg = CrawlConfig {
                        start_url: u.clone(),
                        allowed_domains: vec![],
                        known_urls: db.get_known_urls().await.unwrap_or_default(),
                        max_depth: 3,
                        max_total_requests: None,
                        timeout_seconds: None,
                        concurrency_limit: 10,
                        user_agent: "CrabSitemapCLI/1.0".into(),
                        blacklist_patterns: vec![],
                        track_images: true,
                        track_videos: false,
                        track_documents: false,
                    };
                    (u.clone(), cfg)
                } else {
                    let (u, mut cfg) = prompt_cli_crawl_params()?;
                    cfg.known_urls = db.get_known_urls().await.unwrap_or_default();
                    (u, cfg)
                };

                println!("🚀 Starting crawl for: {}", url);
                run_crawl_and_update(config, db, &sitemap_path, expire_days).await?;
            }
        },
        Commands::Config { action } => match action {
            ConfigAction::Reset => {
                let _ = fs::remove_file("config.json");
                let _ = fs::remove_dir_all("data");
                println!("🧹 Configuration reset. Restart to run setup again.");
            }
            ConfigAction::Show => {
                if let Some(cfg) = load_config() {
                    println!("{}", serde_json::to_string_pretty(&cfg).unwrap());
                } else {
                    println!("No configuration found. Run setup first.");
                }
            }
        },
        Commands::Service { action } => match action {
            ServiceAction::Install => {
                install_systemd_service()?;
            }
            ServiceAction::Uninstall => {
                uninstall_systemd_service()?;
            }
        },
    }

    Ok(())
}

fn generate_random_pg_creds() -> (String, String, String) {
    let mut rng = rand::thread_rng();
    let suffix: String = (0..4).map(|_| format!("{:x}", rng.gen::<u8>())).collect();
    let db_name = format!("crab_sitemap_{}", suffix);
    let user = format!("crab_admin_{}", suffix);
    let pass: String = (0..16).map(|_| format!("{:x}", rng.gen::<u8>())).collect();
    (db_name, user, pass)
}