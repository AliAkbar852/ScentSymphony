import logging
import time
import random
from datetime import datetime
from utilities.file_utils import read_urls_from_csv, load_scraped_urls, save_scraped_urls
from scraper.CloudflareBypasser import get_page_html
from scraper.extractor import Extractor
from utilities.dbmanager import DBManager
from config import DB_CONNECTION_STRING

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BATCH_SIZE = 50
SLEEP_MIN = 20 * 60  # 20 minutes
SLEEP_MAX = 30 * 60  # 30 minutes
MAX_RETRIES = 3      # limit retries for failing URLs
FAILED_LOG_FILE = "failed_urls.log"

def log_failed_url(url, reason):
    """Append permanently failed URL into a log file."""
    with open(FAILED_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} - {url} - {reason}\n")

def main():
    url_csv = "data/urls.csv"
    urls_to_scrape = read_urls_from_csv(url_csv)
    scraped_urls = load_scraped_urls()
    connection_string = DB_CONNECTION_STRING

    # --- Initialization ---
    db_manager = DBManager(connection_string)
    extractor = Extractor(db_manager)

    try:
        db_manager.create_tables()
    except Exception as e:
        logging.error(f"❌ Exiting: Could not initialize database tables. Error: {e}")
        return

    # Filter out already scraped URLs
    urls_left = [u for u in urls_to_scrape if u not in scraped_urls]

    # Dictionary to track retries {url: retry_count}
    retry_counts = {}

    batch_num = 0
    while urls_left:
        batch_num += 1
        batch = urls_left[:BATCH_SIZE]
        urls_left = urls_left[BATCH_SIZE:]
        failed_urls = []

        logging.info(f"\n📦 Processing batch {batch_num} with {len(batch)} URLs...")

        for url in batch:
            try:
                logging.info(f"\n🔍 Scraping: {url}")
                html_content = get_page_html(url)

                if html_content:
                    extractor.process_and_save(html_content, url)

                    # ✅ Only mark as scraped if insertion is successful
                    scraped_urls.add(url)
                    save_scraped_urls(scraped_urls)
                else:
                    logging.warning(f"⚠️ Could not retrieve HTML for {url}. Will retry later.")
                    failed_urls.append((url, "Empty HTML"))

            except Exception as e:
                logging.error(f"❌ Error while processing {url}: {e}", exc_info=True)
                failed_urls.append((url, str(e)))

        # Handle failed URLs
        retry_next = []
        for url, reason in failed_urls:
            retry_counts[url] = retry_counts.get(url, 0) + 1
            if retry_counts[url] <= MAX_RETRIES:
                logging.info(f"🔄 Will retry {url} (attempt {retry_counts[url]}/{MAX_RETRIES})")
                retry_next.append(url)
            else:
                logging.error(f"🚫 Max retries reached for {url}, skipping permanently.")
                log_failed_url(url, reason)

        # Add failed URLs back for next round
        urls_left.extend(retry_next)

        # Wait between batches if there’s still work left
        if urls_left:
            wait_time = random.randint(SLEEP_MIN, SLEEP_MAX)
            logging.info(f"⏳ Batch {batch_num} complete. Sleeping for {wait_time//60} minutes...")
            time.sleep(wait_time)

    logging.info("🚀 All batches processed. Scraping complete!")

if __name__ == "__main__":
    main()
