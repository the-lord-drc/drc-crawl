use anyhow::Result;
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::HashSet;
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::timeout;
use url::Url;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ContentType {
    Html,
    Image,
    Video,
    Document,
    Archive,
    Audio,
    Unknown,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DiscoveredLink {
    pub url: String,
    pub status_code: u16,
    pub content_type: ContentType,
    pub depth: usize,
    pub response_time_ms: u128,
    pub last_modified: Option<String>,
    pub content_length: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CrawlReport {
    pub links: Vec<DiscoveredLink>,
    pub total_requests: usize,
    pub failed_requests: usize,
    pub total_time_ms: u128,
}

#[derive(Debug, Clone)]
pub struct CrawlConfig {
    pub start_url: String,
    pub allowed_domains: Vec<String>,
    pub known_urls: Vec<String>,
    pub max_depth: usize,
    pub max_total_requests: Option<usize>,
    pub timeout_seconds: Option<u64>,
    pub concurrency_limit: usize,
    pub user_agent: String,
    pub blacklist_patterns: Vec<String>,
    pub track_images: bool,
    pub track_videos: bool,
    pub track_documents: bool,
}

pub struct CrawlerEngine {
    client: Client,
    config: CrawlConfig,
    start_url_parsed: Url,
}

impl CrawlerEngine {
    pub fn new(config: CrawlConfig) -> Result<Self> {
        let start_url_parsed = Url::parse(&config.start_url)?;
        let client = Client::builder()
            .user_agent(&config.user_agent)
            .timeout(Duration::from_secs(15))
            .build()?;
        Ok(Self { client, config, start_url_parsed })
    }

    pub async fn run(&self) -> Result<CrawlReport> {
        let start_time = Instant::now();
        let (tx, mut rx) = mpsc::channel(1000);
        let semaphore = Arc::new(Semaphore::new(self.config.concurrency_limit));

        let requests_made = Arc::new(AtomicUsize::new(0));
        let failed_requests = Arc::new(AtomicUsize::new(0));

        let mut visited_urls = HashSet::new();
        let mut results = Vec::new();

        for known in &self.config.known_urls {
            if let Ok(parsed) = Url::parse(known) {
                visited_urls.insert(Self::normalize_url(parsed));
            }
        }

        let root_url = self.start_url_parsed.clone();
        visited_urls.insert(Self::normalize_url(root_url.clone()));

        let tx_clone = tx.clone();
        let client_clone = self.client.clone();
        let sem_clone = semaphore.clone();
        let config_clone = self.config.clone();
        let req_counter = requests_made.clone();

        req_counter.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let _permit = sem_clone.acquire().await.unwrap();
            let _ = Self::crawl_page(client_clone, root_url, 0, tx_clone, config_clone).await;
        });

        let mut active_tasks = 1;

        let crawl_logic = async {
            while active_tasks > 0 {
                if let Some(msg) = rx.recv().await {
                    match msg {
                        EngineMessage::LinkProcessed { link_info, found_hrefs } => {
                            active_tasks -= 1;
                            results.push(link_info.clone());

                            let current_reqs = requests_made.load(Ordering::SeqCst);
                            if let Some(max_req) = self.config.max_total_requests {
                                if current_reqs >= max_req {
                                    continue;
                                }
                            }

                            if link_info.content_type == ContentType::Html && link_info.depth < self.config.max_depth {
                                for href in found_hrefs {
                                    if let Ok(parsed_url) = Url::parse(&href) {
                                        if self.is_domain_allowed(&parsed_url) {
                                            let normalized = Self::normalize_url(parsed_url);
                                            if !visited_urls.contains(&normalized) && !self.is_blacklisted(&normalized) {
                                                visited_urls.insert(normalized.clone());
                                                active_tasks += 1;
                                                requests_made.fetch_add(1, Ordering::SeqCst);

                                                let tx_clone = tx.clone();
                                                let client_clone = self.client.clone();
                                                let sem_clone = semaphore.clone();
                                                let config_clone = self.config.clone();
                                                let next_url = Url::parse(&normalized).unwrap();
                                                let next_depth = link_info.depth + 1;

                                                tokio::spawn(async move {
                                                    let _permit = sem_clone.acquire().await.unwrap();
                                                    let _ = Self::crawl_page(client_clone, next_url, next_depth, tx_clone, config_clone).await;
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        EngineMessage::ProcessFailed { url } => {
                            eprintln!("❌ Failed to crawl: {}", url);
                            active_tasks -= 1;
                            failed_requests.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
        };

        if let Some(timeout_secs) = self.config.timeout_seconds {
            let _ = timeout(Duration::from_secs(timeout_secs), crawl_logic).await;
        } else {
            crawl_logic.await;
        }

        Ok(CrawlReport {
            links: results,
            total_requests: requests_made.load(Ordering::SeqCst),
            failed_requests: failed_requests.load(Ordering::SeqCst),
            total_time_ms: start_time.elapsed().as_millis(),
        })
    }

    async fn crawl_page(
        client: Client,
        url: Url,
        depth: usize,
        tx: mpsc::Sender<EngineMessage>,
        config: CrawlConfig,
    ) -> Result<()> {
        let request_start = Instant::now();
        let response_result = client.get(url.as_str()).send().await;

        let response = match response_result {
            Ok(resp) => resp,
            Err(_) => {
                let _ = tx.send(EngineMessage::ProcessFailed { url: url.to_string() }).await;
                return Ok(());
            }
        };

        let status_code = response.status().as_u16();
        let response_time_ms = request_start.elapsed().as_millis();

        if status_code >= 400 {
            let _ = tx.send(EngineMessage::ProcessFailed { url: url.to_string() }).await;
            return Ok(());
        }

        let headers = response.headers();
        let last_modified = headers.get("last-modified").and_then(|h| h.to_str().ok()).map(|s| s.to_string());
        let content_length = headers.get("content-length").and_then(|h| h.to_str().ok()).and_then(|s| s.parse::<u64>().ok());

        let mut content_type = ContentType::Unknown;
        if let Some(ct_header) = headers.get("content-type") {
            let ct_str = ct_header.to_str().unwrap_or("").to_lowercase();
            if ct_str.contains("text/html") {
                content_type = ContentType::Html;
            } else if ct_str.contains("image/") {
                content_type = ContentType::Image;
            } else if ct_str.contains("video/") {
                content_type = ContentType::Video;
            } else if ct_str.contains("audio/") {
                content_type = ContentType::Audio;
            } else if ct_str.contains("application/pdf") || ct_str.contains("document") {
                content_type = ContentType::Document;
            } else if ct_str.contains("zip") || ct_str.contains("rar") || ct_str.contains("tar") {
                content_type = ContentType::Archive;
            }
        }

        let mut found_hrefs = Vec::new();

        if content_type == ContentType::Html {
            if let Ok(body) = response.text().await {
                let document = Html::parse_document(&body);

                let a_selector = Selector::parse("a[href]").unwrap();
                for element in document.select(&a_selector) {
                    if let Some(href) = element.value().attr("href") {
                        if let Ok(abs_url) = url.join(href) {
                            found_hrefs.push(abs_url.to_string());
                        }
                    }
                }

                if config.track_images {
                    let img_selector = Selector::parse("img[src]").unwrap();
                    for element in document.select(&img_selector) {
                        if let Some(src) = element.value().attr("src") {
                            if let Ok(abs_url) = url.join(src) {
                                found_hrefs.push(abs_url.to_string());
                            }
                        }
                    }
                }

                if config.track_videos {
                    let vid_selector = Selector::parse("video source[src], iframe[src]").unwrap();
                    for element in document.select(&vid_selector) {
                        if let Some(src) = element.value().attr("src") {
                            if let Ok(abs_url) = url.join(src) {
                                found_hrefs.push(abs_url.to_string());
                            }
                        }
                    }
                }

                if config.track_documents {
                    let doc_selector = Selector::parse("a[href$='.pdf'], a[href$='.doc'], a[href$='.docx'], a[href$='.zip'], a[href$='.rar']").unwrap();
                    for element in document.select(&doc_selector) {
                        if let Some(href) = element.value().attr("href") {
                            if let Ok(abs_url) = url.join(href) {
                                found_hrefs.push(abs_url.to_string());
                            }
                        }
                    }
                }
            }
        }

        let link_info = DiscoveredLink {
            url: url.to_string(),
            status_code,
            content_type,
            depth,
            response_time_ms,
            last_modified,
            content_length,
        };

        let _ = tx.send(EngineMessage::LinkProcessed { link_info, found_hrefs }).await;
        Ok(())
    }

    fn is_domain_allowed(&self, url: &Url) -> bool {
        let host = match url.host_str() {
            Some(h) => h,
            None => return false,
        };
        if self.config.allowed_domains.is_empty() {
            return host == self.start_url_parsed.host_str().unwrap_or("");
        }
        self.config.allowed_domains.iter().any(|allowed| host.ends_with(allowed))
    }

    fn is_blacklisted(&self, url: &str) -> bool {
        self.config.blacklist_patterns.iter().any(|pattern| url.contains(pattern))
    }

    fn normalize_url(mut url: Url) -> String {
        url.set_fragment(None);
        let mut path = url.path().to_string();
        if path.ends_with('/') && path.len() > 1 {
            path.pop();
            url.set_path(&path);
        }
        url.to_string()
    }
}

enum EngineMessage {
    LinkProcessed {
        link_info: DiscoveredLink,
        found_hrefs: Vec<String>,
    },
    ProcessFailed {
        url: String,
    },
}