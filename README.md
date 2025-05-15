![Python](https://img.shields.io/badge/Python-3.10-blue?style=flat-square&logo=python)
![Selenium](https://img.shields.io/badge/Selenium-Automation-brightgreen?style=flat-square&logo=selenium)
![yt-dlp](https://img.shields.io/badge/yt--dlp-Video%20Downloader-red?style=flat-square&logo=youtube)
![Web Scraping](https://img.shields.io/badge/Web%20Scraping-Data%20Extraction-teal?style=flat-square&logo=webcomponents.org)
![YouTube Scraper](https://img.shields.io/badge/YouTube--Scraper-YouTube%20Data-critical?style=flat-square&logo=youtube)
![Async Python](https://img.shields.io/badge/Async--Python-asyncio-blueviolet?style=flat-square&logo=python)
![SQLite](https://img.shields.io/badge/SQLite-Database-lightgrey?style=flat-square&logo=sqlite)
![Data Mining](https://img.shields.io/badge/Data%20Mining-Analysis-orange?style=flat-square&logo=minutemailer)
![VSCode](https://img.shields.io/badge/VSCode-IDE-007ACC?style=flat-square&logo=visual-studio-code)
![MIT License](https://img.shields.io/badge/License-MIT-purple?style=flat-square)
![Automation](https://img.shields.io/badge/Automation-Robotic-yellowgreen?style=flat-square&logo=automation)
![YouTube Comments](https://img.shields.io/badge/YouTube%20Comments-Scraped%20Data-red?style=flat-square&logo=youtube)
![Video Metadata](https://img.shields.io/badge/Video%20Metadata-Info-informational?style=flat-square&logo=youtube)



# 🎯 YouTube Channel Scraper with AsyncIO, Selenium & yt_dlp

A fast, asynchronous YouTube scraper that extracts channel metadata, videos (including Shorts), comments, and replies using `asyncio`, `yt_dlp`, and `selenium`. The data is stored in a structured **SQLite** database with support for checkpointing, logging, and resumable scraping.

---

## 📌 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Database Schema](#database-schema)
- [Installation](#installation)
- [Usage](#usage)
- [Checkpointing](#checkpointing)
- [Logging](#logging)
- [Folder Structure](#folder-structure)
- [Disclaimer](#disclaimer)
- [License](#license)
- [Contributing](#contributing)
- [GitHub Tags / Topics](#github-tags--topics)

---

## 🔍 Overview

This scraper allows you to extract **comprehensive YouTube channel data**, including:

- Channel metadata
- Videos and Shorts metadata
- Comments and threaded replies
- Views, likes, timestamps, and more

Designed for **reliability and scalability**, it uses `asyncio` for concurrent tasks and stores everything in a relational database for easy analysis.

---

## 🚀 Features

✅ Scrapes full channel data  
✅ Supports both **Videos** and **Shorts**  
✅ Captures **comments** and **nested replies**  
✅ Built-in checkpointing (resumable scraping)  
✅ Structured **SQLite** storage  
✅ Real-time **logging** (to console and file)  
✅ Handles rate limiting and connection issues gracefully  
✅ Uses `uuid` and `hashlib` for content uniqueness  

---

## 🏗️ Architecture

```plaintext
            +-------------------+
            |  Channel URL(s)   |
            +-------------------+
                     |
                     v
        +--------------------------+
        | yt_dlp: Extract Metadata |
        +--------------------------+
                     |
                     v
      +-------------------------------+
      | Selenium: Scrape Comments UI  |
      +-------------------------------+
                     |
                     v
      +-------------------------------+
      | asyncio: Parallel Fetching    |
      +-------------------------------+
                     |
                     v
        +-------------------------+
        | SQLite: Structured Data |
        +-------------------------+
                     |
                     v
        +--------------------------+
        | Logs & Checkpoint Files  |
        +--------------------------+
````

---

## 🛠️ Technology Stack

* **Python 3.8+**
* [`asyncio`](https://docs.python.org/3/library/asyncio.html)
* [`yt_dlp`](https://github.com/yt-dlp/yt-dlp)
* [`selenium`](https://www.selenium.dev/)
* [`sqlite3`](https://docs.python.org/3/library/sqlite3.html)
* `uuid`, `hashlib`, `psutil`, `json`, `re`, etc.

---

## 🗃️ Database Schema

The scraper uses the following normalized SQLite tables:

### `Channel_Info`

* `channel_id`, `channel_name`, `subscribers`, `total_videos`, `creation_date`, `channel_url`, etc.

### `Videos` and `Shorts`

* `video_id`, `title`, `published_time`, `views`, `likes`, `duration`, `description`, etc.

### `Videos_Comments`, `Videos_Replies`, `Shorts_Comments`, `Shorts_Replies`

* `comment_id`, `text`, `author`, `likes`, `published_time`, `parent_id`, `video_id`, etc.

---

## 📦 Installation

1. **Clone the repository**

```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
cd YOUR_REPO_NAME
```

2. **Install dependencies**

```bash
pip install -r requirements.txt
```

3. **Ensure Edge browser is installed** (for Selenium)

---

## ▶️ Usage

Modify or run the entry script:

```bash
python scraper.py
```

Example structure inside your script:

```python
from scraper import YouTubeScraper

scraper = YouTubeScraper(channel_urls=[
    'https://www.youtube.com/@examplechannel'
])
scraper.run()
```

---

## ♻️ Checkpointing

The scraper creates a `.json` checkpoint file for each channel. If interrupted, it resumes from the last successful index without re-scraping.

Example:

```
checkpoints/UC1234abcd.json
```

---

## 📋 Logging

* Console and file logging supported via Python's `logging` module
* Errors, scraping progress, and completion status are tracked

---

## 📁 Folder Structure

```plaintext
├── checkpoints/              # Channel progress trackers
├── database/                 # SQLite files
├── logs/                     # Log files
├── scraper/                  # Core scraping logic
│   ├── video_scraper.py
│   ├── comment_scraper.py
│   └── utils.py
├── main.py                   # Main runner script
├── requirements.txt
└── README.md
```

---

## ⚠️ Disclaimer

This tool is intended for **educational and research** use only. Scraping YouTube may violate their [Terms of Service](https://www.youtube.com/t/terms). Use responsibly and ethically.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

## 🤝 Contributing

We welcome contributions! Please open an issue or pull request with any improvements or bug fixes.

---

## 🏷 GitHub Tags / Topics

```text
youtube-scraper
asynchronous-python
selenium
yt-dlp
sqlite
web-crawler
youtube-comments
youtube-shorts
data-mining
video-metadata
checkpointing
web-scraping
```

---

## 📫 Contact

Created by **@momina02**. For questions or collaboration, feel free to reach out!

