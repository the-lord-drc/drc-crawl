```markdown
# 🦇 DRC Crawl

**High-Performance Sitemap Crawler with Web Dashboard & Background Scheduling**

[![Rust](https://img.shields.io/badge/built%20with-Rust-orange)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

DRC Crawl is a blazing-fast, self-hosted sitemap generator built in Rust. It crawls websites, merges new links with existing sitemaps, and keeps your sitemap up‑to‑date automatically. It comes with a web dashboard, multiple database backends, and a simple background scheduler.

---

## ✨ Features

- 🚀 **Blazing Fast** – Async Rust engine crawls thousands of URLs in seconds.
- 🕸️ **Sitemap Generation** – Produces standard sitemap.xml with `lastmod`, `priority`, and `changefreq`.
- 🔄 **Delta Crawling** – Only fetches new or changed links on subsequent runs.
- 🗄️ **Multi‑Database** – Supports SQLite, PostgreSQL, and JSON Lines out of the box.
- 📊 **Web Dashboard** – Real‑time telemetry, start/stop crawls, and download sitemaps.
- ⏱️ **Background Scheduler** – Set a simple interval (e.g., `24h`, `30m`) and let it crawl automatically.
- ⚙️ **Persistent Configuration** – First‑run setup saves your settings; later runs start instantly.
- 🖥️ **CLI & Web Modes** – Use it interactively in the terminal or as a background service.
- 🐧 **systemd Integration** – Install as a persistent service with a single command.
- 📦 **Single Binary** – No dependencies; just download and run.

---

## 📥 Installation

### Pre‑built binaries (recommended)
Download the latest release for your platform from the [Releases page](https://github.com/the-lord-drc/drc-crawl/releases).  
Extract and run:
```bash
chmod +x drc-crawl
./drc-crawl run web
```

Using Cargo (Rust developers)

```bash
cargo install drc-crawl
drc-crawl run web
```

Build from source

```bash
git clone https://github.com/the-lord-drc/drc-crawl.git
cd drc-crawl
cargo build --release
./target/release/drc-crawl run web
```

---

🚦 Quick Start

1. Start the web panel (interactive setup):
   ```bash
   drc-crawl run web
   ```
   You’ll be asked to choose a database, sitemap path, and optional schedule.
2. Open your browser at http://localhost:786.
3. Enter a target URL and click Ignite Engine.
4. Watch the live telemetry – download the sitemap when finished.

CLI Mode

Crawl a site directly:

```bash
drc-crawl run cli https://example.com
```

Or start an interactive CLI session:

```bash
drc-crawl run cli
```

---

🗃️ Database Options

Type Connection string example
SQLite sqlite:data/crawler.db?mode=rwc
PostgreSQL postgres://user:pass@localhost/dbname
JSON Lines output/links.jsonl

The database stores previously crawled URLs and respects an expiration period (configurable in days).

---

⏱️ Background Service

On Linux (systemd)

Install as a service that survives reboots:

```bash
sudo drc-crawl service install
```

Remove it anytime:

```bash
sudo drc-crawl service uninstall
```

The service uses your existing config.json and starts the web panel automatically.

---

⚙️ Configuration

All settings are stored in config.json (auto‑created on first run).
You can edit it directly or use the web dashboard’s Settings panel.

Example:

```json
{
  "db_type": "sqlite",
  "db_url": "sqlite:data/crawler.db?mode=rwc",
  "sitemap_path": "./sitemap.xml",
  "schedule_interval": "24h",
  "expire_days": 30
}
```

---

🌐 Web Panel API

Endpoint Method Description
/api/config GET View current configuration
/api/config POST Update configuration
/api/config DELETE Reset configuration
/api/database DELETE Clear all crawled links
/api/crawl/start POST Start a new crawl job
/api/crawl/status/:id GET Get status of a crawl job
/api/crawl/export/:id GET Download sitemap XML for a job

---

📄 Sitemap Output

DRC Crawl generates a standard sitemap.xml.
It automatically merges new links with any existing sitemap file, preserving old entries.

Example:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com</loc>
    <lastmod>2025-01-01</lastmod>
    <changefreq>daily</changefreq>
    <priority>1.0</priority>
  </url>
</urlset>
```

---

🛠️ Tech Stack

· Language: Rust (stable)
· Web framework: Axum
· Async runtime: Tokio
· Database: SQLx (SQLite/PostgreSQL), JSON Lines
· HTML parsing: scraper
· CLI: Clap + Inquire
· Frontend: Vanilla HTML/CSS/JS (embedded in binary)

---

🤝 Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

---

📜 License

This project is licensed under the MIT License – see the LICENSE file for details.

---

Made with 🖤 by Alamdar Developer 🦇

```
