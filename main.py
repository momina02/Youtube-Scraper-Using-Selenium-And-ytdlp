import asyncio
import aiohttp
import yt_dlp
import json
import logging
import os
import sqlite3
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from dateutil.parser import parse
import uuid
import time
import psutil
import traceback
from datetime import datetime as dt
import concurrent.futures
from selenium.common.exceptions import TimeoutException, WebDriverException
import hashlib

# Configure logging
log_file = f"youtube_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
MAX_CONCURRENT_REQUESTS = 60
RETRY_LIMIT = 3
TIMEOUT = 10
TASK_TIMEOUT = 30
CHECKPOINT_INTERVAL = 10
DATABASE_NAME = "youtube_data.db"
YDL_OPTS = {
    'quiet': True,
    'extract_flat': True,
    'no_warnings': True,
    'getcomments': True,
    'http_headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0.0.0 Safari/537.36'},
    'retries': 3,
    'sleep_interval': 0.5,
    'writeinfojson': False,
    'skip_download': True,
    'ignoreerrors': True,
}

def init_database():
    """Initialize SQLite database and create tables."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Channel_Info table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Channel_Info (
            channel_id TEXT PRIMARY KEY,
            channel_title TEXT,
            subscribers INTEGER,
            total_views INTEGER,
            joined_date TEXT,
            total_videos INTEGER,
            origin TEXT,
            channel_description TEXT,
            description_links TEXT,
            monetized INTEGER,
            fetched_at TEXT
        )
    ''')

    # Videos table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Videos (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT,
            title TEXT,
            description TEXT,
            views INTEGER,
            duration INTEGER,
            upload_date TEXT,
            likes INTEGER,
            comment_count INTEGER,
            fetched_at TEXT,
            FOREIGN KEY (channel_id) REFERENCES Channel_Info (channel_id)
        )
    ''')

    # Shorts table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Shorts (
            short_id TEXT PRIMARY KEY,
            channel_id TEXT,
            title TEXT,
            description TEXT,
            views INTEGER,
            duration INTEGER,
            upload_date TEXT,
            likes INTEGER,
            comment_count INTEGER,
            fetched_at TEXT,
            FOREIGN KEY (channel_id) REFERENCES Channel_Info (channel_id)
        )
    ''')

    # Videos_Comments table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Videos_Comments (
            comment_id TEXT PRIMARY KEY,
            video_id TEXT,
            text TEXT,
            author TEXT,
            channel_id TEXT,
            timestamp TEXT,
            fetched_at TEXT,
            FOREIGN KEY (video_id) REFERENCES Videos (video_id)
        )
    ''')

    # Videos_Replies table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Videos_Replies (
            reply_id TEXT PRIMARY KEY,
            comment_id TEXT,
            text TEXT,
            author TEXT,
            timestamp TEXT,
            fetched_at TEXT,
            FOREIGN KEY (comment_id) REFERENCES Videos_Comments (comment_id)
        )
    ''')

    # Shorts_Comments table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Shorts_Comments (
            comment_id TEXT PRIMARY KEY,
            short_id TEXT,
            text TEXT,
            author TEXT,
            channel_id TEXT,
            timestamp TEXT,
            fetched_at TEXT,
            FOREIGN KEY (short_id) REFERENCES Shorts (short_id)
        )
    ''')

    # Shorts_Replies table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Shorts_Replies (
            reply_id TEXT PRIMARY KEY,
            comment_id TEXT,
            text TEXT,
            author TEXT,
            timestamp TEXT,
            fetched_at TEXT,
            FOREIGN KEY (comment_id) REFERENCES Shorts_Comments (comment_id)
        )
    ''')

    conn.commit()
    conn.close()
    logger.info(f"Database initialized: {DATABASE_NAME}")

def get_db_connection():
    """Get a new database connection."""
    return sqlite3.connect(DATABASE_NAME)

def convert_to_int(value):
    """Convert subscriber/view counts to integer."""
    if not value:
        return None
    value = value.lower().replace(' subscribers', '').replace(' views', '').strip()
    try:
        if 'k' in value:
            return int(float(value.replace('k', '')) * 1000)
        elif 'm' in value:
            return int(float(value.replace('m', '')) * 1000000)
        elif 'b' in value:
            return int(float(value.replace('b', '')) * 1000000000)
        return int(value.replace(',', ''))
    except ValueError:
        return None

def sanitize_log_message(message):
    """Replace problematic Unicode characters for logging."""
    if not isinstance(message, str):
        message = str(message)
    return message.encode('ascii', 'replace').decode('ascii')

async def fetch_page(session, url, retries=RETRY_LIMIT):
    """Fetch a page asynchronously with retry logic."""
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=TIMEOUT) as response:
                if response.status == 200:
                    return await response.text()
                logger.warning(f"Failed to fetch {url}, status: {response.status}, attempt: {attempt + 1}")
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}, attempt: {attempt + 1}")
        await asyncio.sleep(1 * (attempt + 1))
    logger.error(f"Failed to fetch {url} after {retries} attempts")
    return None

def load_checkpoint(channel_id):
    """Load checkpoint data if it exists."""
    checkpoint_file = f"{sanitize_filename(channel_id)}_checkpoint.json"
    default_checkpoint = {
        "channel_info_scraped": False,
        "videos_processed": 0,
        "shorts_processed": 0,
        "videos": [],
        "shorts": []
    }
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                for key in default_checkpoint:
                    if key not in checkpoint:
                        checkpoint[key] = default_checkpoint[key]
                return checkpoint
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
    return default_checkpoint

def save_checkpoint(channel_id, checkpoint_data):
    """Save checkpoint data."""
    checkpoint_file = f"{sanitize_filename(channel_id)}_checkpoint.json"
    try:
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, indent=4, ensure_ascii=False)
        logger.info(f"Checkpoint saved to {checkpoint_file}")
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")

def load_channel_info(channel_id):
    """Load channel info from database if it exists."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            SELECT * FROM Channel_Info WHERE channel_id = ?
        ''', (channel_id,))
        row = cursor.fetchone()
        if row:
            return {
                "channel_id": row[0],
                "channel_title": row[1],
                "subscribers": row[2],
                "totalviews": row[3],
                "joined_date": row[4],
                "total_videos": row[5],
                "origin": row[6],
                "channel_description": row[7],
                "descriptionlinks": row[8],
                "monitized": row[9],
                "fetched_at": row[10]
            }
    except Exception as e:
        logger.error(f"Error loading channel info from database: {e}")
    finally:
        conn.close()
    return None

def save_channel_info(channel_info):
    """Save channel info to database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO Channel_Info (
                channel_id, channel_title, subscribers, total_views, joined_date,
                total_videos, origin, channel_description, description_links, monetized, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            channel_info["channel_id"],
            channel_info["channel_title"],
            channel_info["subscribers"],
            channel_info["totalviews"],
            channel_info["joined_date"],
            channel_info["total_videos"],
            channel_info["origin"],
            channel_info["channel_description"],
            channel_info["descriptionlinks"],
            channel_info["monitized"],
            channel_info["fetched_at"]
        ))
        conn.commit()
        logger.info(f"Channel info saved to database for {channel_info['channel_title']}")
    except Exception as e:
        logger.error(f"Error saving channel info to database: {e}")
    finally:
        conn.close()

def save_video_or_short(content_type, item, channel_id):
    """Save video or short to database."""
    table_name = "Videos" if content_type == "videos" else "Shorts"
    id_field = "video_id" if content_type == "videos" else "short_id"
    video_id = item.get("video_id", str(uuid.uuid4()))
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f'''
            INSERT OR REPLACE INTO {table_name} (
                {id_field}, channel_id, title, description, views, duration,
                upload_date, likes, comment_count, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            video_id,
            channel_id,
            item["title"],
            item["description"],
            item["views"],
            item["duration"],
            item["upload_date"],
            item["likes"],
            item["comment_count"],
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        conn.commit()
        logger.info(f"Saved {content_type[:-1]} {item['title'][:50]}... to database")
        
        # Save comments and replies
        comments_table = "Videos_Comments" if content_type == "videos" else "Shorts_Comments"
        replies_table = "Videos_Replies" if content_type == "videos" else "Shorts_Replies"
        for comment in item["comments"]:
            cursor.execute(f'''
                INSERT OR REPLACE INTO {comments_table} (
                    comment_id, {id_field}, text, author, channel_id, timestamp, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                comment["comment_id"],
                video_id,
                comment["text"],
                comment["author"],
                comment["channel_id"],
                comment["timestamp"],
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
            for reply in comment["replies"]:
                cursor.execute(f'''
                    INSERT OR REPLACE INTO {replies_table} (
                        reply_id, comment_id, text, author, timestamp, fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    reply["reply_id"],
                    comment["comment_id"],
                    reply["text"],
                    reply["author"],
                    reply["timestamp"],
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ))
        conn.commit()
        logger.info(f"Saved {len(item['comments'])} comments and {sum(len(c['replies']) for c in item['comments'])} replies for {content_type[:-1]} {video_id}")
    except Exception as e:
        logger.error(f"Error saving {content_type[:-1]} to database: {e}")
    finally:
        conn.close()
    return video_id

def scrape_channel_info_selenium(channel_url):
    """Scrape channel info using Selenium with improved error handling."""
    logger.info("Starting channel info scraping with Selenium")
    start_time = time.time()
    
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--log-level=3')
    service = Service(EdgeChromiumDriverManager().install())
    driver = None
    
    channel_id = hashlib.md5(channel_url.encode()).hexdigest()  # Unique ID based on URL
    data = {
        "channel_id": channel_id,
        "channel_title": None,
        "subscribers": None,
        "totalviews": None,
        "joined_date": None,
        "total_videos": None,
        "origin": None,
        "channel_description": None,
        "descriptionlinks": None,
        "monitized": 0,
        "fetched_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    for attempt in range(RETRY_LIMIT):
        try:
            logger.info(f"Attempt {attempt + 1}/{RETRY_LIMIT} to load {channel_url}")
            driver = webdriver.Edge(service=service, options=options)
            driver.set_page_load_timeout(20)
            driver.get(channel_url)
            WebDriverWait(driver, 5).until(lambda d: d.execute_script('return document.readyState') == 'complete')
            logger.info("Channel page loaded successfully")
            break
        except (TimeoutException, WebDriverException) as e:
            logger.error(f"Failed to load page on attempt {attempt + 1}: {e}")
            if driver:
                driver.quit()
            if attempt + 1 == RETRY_LIMIT:
                logger.error("Max retries reached, returning default data")
                return data
            time.sleep(2 * (attempt + 1))

    try:
        # Channel title
        try:
            title_element = driver.find_element(By.CSS_SELECTOR, "meta[name='title']")
            data["channel_title"] = title_element.get_attribute("content").replace(" - YouTube", "")
            logger.info(f"Extracted channel title: {sanitize_log_message(data['channel_title'])}")
        except Exception as e:
            data["channel_title"] = driver.title.replace(" - YouTube", "")
            logger.info(f"Fallback to driver title: {sanitize_log_message(data['channel_title'])}")

        # Description tab
        try:
            description_button = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, 'Description')]"))
            )
            driver.execute_script("arguments[0].click();", description_button)
            logger.info("Clicked description tab")
            WebDriverWait(driver, 2).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#additional-info-container > table"))
            )

            # Extract table data
            table = driver.find_element(By.CSS_SELECTOR, "#additional-info-container > table")
            rows = table.find_elements(By.CSS_SELECTOR, "tr.description-item")
            logger.debug(f"Found {len(rows)} table rows")
            for row in rows:
                try:
                    value_cell = row.find_element(By.CSS_SELECTOR, "td:nth-child(2)")
                    row_text = value_cell.text.strip()
                    row_lower = row_text.lower()

                    if "subscribers" in row_lower:
                        data["subscribers"] = convert_to_int(row_text)
                        logger.info(f"Extracted subscribers: {data['subscribers']}")
                    elif "video" in row_lower:
                        digits = ''.join(filter(str.isdigit, row_text))
                        data["total_videos"] = int(digits) if digits else None
                        logger.info(f"Extracted total videos: {data['total_videos']}")
                    elif "views" in row_lower:
                        data["totalviews"] = convert_to_int(row_text)
                        logger.info(f"Extracted total views: {data['totalviews']}")
                    elif any(keyword in row_lower for keyword in ["joined", "date"]):
                        data["joined_date"] = row_text
                        logger.info(f"Extracted joined date: {sanitize_log_message(data['joined_date'])}")
                    elif row_text in ["United States", "India", "Canada", "United Kingdom"]:
                        data["origin"] = row_text
                        logger.info(f"Extracted origin: {data['origin']}")
                except Exception as e:
                    logger.debug(f"Error processing row: {e}")

            # Description
            try:
                data["channel_description"] = driver.find_element(By.CSS_SELECTOR, "#description-container > span").text.strip()
                logger.info(f"Extracted description: {sanitize_log_message(data['channel_description'][:50])}...")
            except Exception as e:
                logger.warning(f"Could not extract channel description: {e}")

            # Links
            try:
                data["descriptionlinks"] = driver.find_element(By.CSS_SELECTOR, "div#link-list-container").text.strip()
                logger.info(f"Extracted description links: {sanitize_log_message(data['descriptionlinks'][:50])}...")
            except Exception as e:
                logger.warning(f"Could not extract description links: {e}")

            # Monetization
            if 'badge-style-type-verified' in driver.page_source:
                data["monitized"] = 1
                logger.info("Channel is monetized")
        except Exception as e:
            logger.error(f"Error extracting description data: {e}")

        # Fallback: Scrape total videos from Videos tab
        if data["total_videos"] is None:
            try:
                videos_button = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, 'Videos')]"))
                )
                driver.execute_script("arguments[0].click();", videos_button)
                logger.info("Clicked videos tab")
                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#video-count, #metadata-line, yt-formatted-string[aria-label*='videos']"))
                )

                video_count = None
                selectors = ["#video-count", "#metadata-line", "yt-formatted-string[aria-label*='videos']"]
                for selector in selectors:
                    try:
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        for element in elements:
                            text = element.text.lower()
                            logger.debug(f"Checking selector {selector}: {text}")
                            if "video" in text:
                                digits = ''.join(filter(str.isdigit, text))
                                if digits:
                                    video_count = int(digits)
                                    logger.info(f"Extracted total videos from selector {selector}: {video_count}")
                                    break
                        if video_count is not None:
                            break
                    except Exception as e:
                        logger.debug(f"Selector {selector} not found: {e}")

                data["total_videos"] = video_count if video_count is not None else 0
                logger.info(f"Final total videos: {data['total_videos']}")
            except Exception as e:
                logger.error(f"Error extracting total videos: {e}")
                data["total_videos"] = 0

    finally:
        if driver:
            driver.quit()
        logger.info(f"Channel info scraping completed in {time.time() - start_time:.2f} seconds")

    return data

async def scrape_videos_shorts(channel_url, content_type, session, channel_id, start_index=0, checkpoint_data=None):
    """Scrape videos or shorts using yt-dlp with checkpoint and database support."""
    logger.info(f"Starting {content_type} scraping for {channel_url} from index {start_index}")
    start_time = time.time()
    url = f"{channel_url}/{content_type}"
    data = {"total": 0, content_type: []}
    metadata_cache = {}
    checkpoint_data = checkpoint_data or {
        "videos": [],
        "shorts": [],
        "channel_info_scraped": False,
        "videos_processed": 0,
        "shorts_processed": 0
    }

    with yt_dlp.YoutubeDL(YDL_OPTS) as ydl:
        try:
            logger.info(f"Fetching playlist info for {content_type}")
            info = ydl.extract_info(url, download=False)
            entries = info.get('entries', []) or []
            data["total"] = len(entries)
            logger.info(f"Found {data['total']} {content_type}")

            entries = entries[start_index:]
            logger.info(f"Processing {len(entries)} {content_type} starting from index {start_index}")

            semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
            async def limited_task(task):
                async with semaphore:
                    try:
                        return await asyncio.wait_for(task, timeout=TASK_TIMEOUT)
                    except asyncio.TimeoutError:
                        logger.error(f"Task for {content_type} timed out after {TASK_TIMEOUT} seconds")
                        return None
                    except Exception as e:
                        logger.error(f"Task for {content_type} failed: {e}")
                        return None

            batch_size = CHECKPOINT_INTERVAL
            for batch_start in range(0, len(entries), batch_size):
                batch = entries[batch_start:batch_start + batch_size]
                tasks = []
                for idx, entry in enumerate(batch, start_index + batch_start + 1):
                    video_url = entry.get('url')
                    if video_url:
                        logger.info(f"Queueing {content_type[:-1]} {idx}/{data['total']}: {video_url}")
                        tasks.append(limited_task(process_video(session, video_url, entry, idx, data['total'], content_type, metadata_cache)))
                
                logger.info(f"Processing batch of {len(tasks)} {content_type} tasks")
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for idx, result in enumerate(results, start_index + batch_start + 1):
                    if isinstance(result, dict) and result:
                        video_id = save_video_or_short(content_type, result, channel_id)
                        result["video_id"] = video_id
                        data[content_type].append(result)
                        checkpoint_data[content_type].append(result)
                        logger.info(f"Processed {content_type[:-1]} {idx}/{data['total']}: {sanitize_log_message(result['title'][:50])}... | Comments: {len(result['comments'])}")
                    elif isinstance(result, Exception):
                        logger.error(f"Error in {content_type[:-1]} {idx}/{data['total']}: {result}")
                    else:
                        logger.warning(f"Skipped {content_type[:-1]} {idx}/{data['total']}: No data returned")

                if checkpoint_data.get('channel_info'):
                    checkpoint_data['videos_processed'] = start_index + batch_start + len(batch) if content_type == 'videos' else checkpoint_data['videos_processed']
                    checkpoint_data['shorts_processed'] = start_index + batch_start + len(batch) if content_type == 'shorts' else checkpoint_data['shorts_processed']
                    save_checkpoint(channel_id, checkpoint_data)

        except Exception as e:
            logger.error(f"Error scraping {content_type}: {e}\n{traceback.format_exc()}")

    logger.info(f"{content_type.capitalize()} scraping completed in {time.time() - start_time:.2f} seconds")
    logger.info(f"Memory usage: {psutil.Process().memory_info().rss / 1024**2:.2f} MB")
    return data, checkpoint_data

async def process_video(session, video_url, entry, idx, total, content_type, metadata_cache):
    """Process a single video/short with metadata and comments."""
    logger.info(f"Processing {content_type[:-1]} {idx}/{total}: {video_url}")
    try:
        with yt_dlp.YoutubeDL(YDL_OPTS) as ydl:
            for attempt in range(RETRY_LIMIT):
                try:
                    logger.debug(f"Attempt {attempt + 1}/{RETRY_LIMIT} to fetch {video_url}")
                    info = ydl.extract_info(video_url, download=False)
                    if not info:
                        logger.warning(f"No info returned for {video_url}")
                        return None
                    metadata_cache[video_url] = info
                    break
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1}/{RETRY_LIMIT} failed for {video_url}: {e}")
                    if attempt + 1 == RETRY_LIMIT:
                        logger.error(f"Failed to process {video_url} after {RETRY_LIMIT} attempts")
                        return None
                    await asyncio.sleep(1 * (attempt + 1))

        comments = []
        comment_map = {}
        for comment in info.get('comments', []):
            comment_id = comment.get('id', str(uuid.uuid4()))
            parent_id = comment.get('parent')
            comment_data = {
                'comment_id': comment_id,
                'text': comment.get('text', 'N/A'),
                'author': comment.get('author', 'Unknown'),
                'channel_id': comment.get('channel_id', 'N/A'),
                'timestamp': parse_timestamp(comment.get('timestamp', 'N/A')),
                'replies': []
            }
            comment_map[comment_id] = comment_data
            if not parent_id or parent_id == 'root':
                comments.append(comment_data)
            else:
                parent_comment = comment_map.get(parent_id)
                if parent_comment:
                    parent_comment['replies'].append({
                        'reply_id': comment_id,
                        'text': comment.get('text', 'N/A'),
                        'author': comment.get('author', 'Unknown'),
                        'timestamp': parse_timestamp(comment.get('timestamp', 'N/A'))
                    })
                else:
                    logger.warning(f"Orphan reply {comment_id} for parent {parent_id}, treating as comment")
                    comments.append(comment_data)

        logger.info(f"Processed {len(comments)} comments with {sum(len(c['replies']) for c in comments)} replies for {video_url}")

        return {
            'video_id': info.get('id', str(uuid.uuid4())),
            'title': info.get('title', entry.get('title', 'N/A')),
            'description': info.get('description', '') if content_type != "shorts" else info.get('title', ''),
            'views': info.get('view_count', 0),
            'duration': info.get('duration', 0),
            'upload_date': datetime.strptime(info.get('upload_date', '19700101'), '%Y%m%d').strftime('%Y-%m-%d') if info.get('upload_date') else 'N/A',
            'likes': info.get('like_count', 0),
            'comment_count': info.get('comment_count', 0),
            'comments': comments
        }
    except Exception as e:
        logger.error(f"Error processing {content_type[:-1]} {idx}/{total} ({video_url}): {e}\n{traceback.format_exc()}")
        return None

def parse_timestamp(timestamp):
    """Convert timestamp to ISO format."""
    logger.debug(f"Parsing timestamp: {timestamp}")
    try:
        if isinstance(timestamp, (int, float)):
            return dt.fromtimestamp(timestamp).isoformat()
        return parse(timestamp, fuzzy=True).isoformat()
    except Exception as e:
        logger.warning(f"Failed to parse timestamp {timestamp}: {e}, returning default")
        return "1970-01-01T00:00:00"

def sanitize_filename(name):
    """Sanitize filename for saving."""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, '')
    return name.replace(' ', '_')

async def main(channel_url):
    """Main function to scrape channel data with checkpoints and database storage."""
    start_time = time.time()
    logger.info(f"Starting scraping for channel: {channel_url}")
    logger.info(f"Initial memory usage: {psutil.Process().memory_info().rss / 1024**2:.2f} MB")

    # Initialize database
    init_database()

    # Generate channel_id from URL
    channel_id = hashlib.md5(channel_url.encode()).hexdigest()
    checkpoint_data = {
        "videos": [],
        "shorts": [],
        "channel_info_scraped": False,
        "videos_processed": 0,
        "shorts_processed": 0
    }
    
    # Load checkpoint
    loaded_checkpoint = load_checkpoint(channel_id)
    checkpoint_data.update(loaded_checkpoint)
    
    # Load or scrape channel info
    if checkpoint_data.get("channel_info_scraped", False):
        channel_info = load_channel_info(channel_id)
        if channel_info:
            logger.info("Loaded existing channel info from database")
            checkpoint_data["channel_info"] = channel_info
        else:
            logger.info("Channel info checkpoint exists but not in database, re-scraping")
            selenium_start = time.time()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                channel_info_future = executor.submit(scrape_channel_info_selenium, channel_url)
                channel_info = channel_info_future.result()
            save_channel_info(channel_info)
            checkpoint_data["channel_info"] = channel_info
            checkpoint_data["channel_info_scraped"] = True
            save_checkpoint(channel_id, checkpoint_data)
            logger.info(f"Selenium scraping took {time.time() - selenium_start:.2f} seconds")
    else:
        selenium_start = time.time()
        channel_info = scrape_channel_info_selenium(channel_url)
        save_channel_info(channel_info)
        checkpoint_data["channel_info"] = channel_info
        checkpoint_data["channel_info_scraped"] = True
        save_checkpoint(channel_id, checkpoint_data)
        logger.info(f"Selenium scraping took {time.time() - selenium_start:.2f} seconds")

    async with aiohttp.ClientSession() as session:
        logger.info("Starting concurrent video and shorts scraping")
        video_start = time.time()
        videos_data, checkpoint_data = await scrape_videos_shorts(
            channel_url, "videos", session, channel_id,
            start_index=checkpoint_data.get("videos_processed", 0),
            checkpoint_data=checkpoint_data
        )
        logger.info(f"Video scraping took {time.time() - video_start:.2f} seconds")
        
        shorts_start = time.time()
        shorts_data, checkpoint_data = await scrape_videos_shorts(
            channel_url, "shorts", session, channel_id,
            start_index=checkpoint_data.get("shorts_processed", 0),
            checkpoint_data=checkpoint_data
        )
        logger.info(f"Shorts scraping took {time.time() - shorts_start:.2f} seconds")

    combined_data = {
        "channel_info": channel_info,
        "videos": videos_data,
        "shorts": shorts_data,
        "scraped_at": datetime.now().isoformat()
    }

    file_name = f"{sanitize_filename(channel_info['channel_title'])}_new_data.json"
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(combined_data, f, indent=4, ensure_ascii=False)
    logger.info(f"Data saved to {file_name}")

    total_time = time.time() - start_time
    logger.info(f"Total scraping took {total_time:.2f} seconds")
    logger.info(f"Final memory usage: {psutil.Process().memory_info().rss / 1024**2:.2f} MB")
    logger.info(f"Log file: {log_file}")

if __name__ == "__main__":
    channel_url = "https://www.youtube.com/@tariqjamilofficial"
    asyncio.run(main(channel_url))