import logging
import time
import random

from utilities.file_utils import read_urls_from_csv, load_scraped_urls, save_scraped_urls
from scraper.CloudflareBypasser import get_page_html
from scraper.extractor import Extractor
from utilities.dbmanager import DBManager
from config import DB_CONNECTION_STRING
from utilities.file_utils import failed_url, clean_failed_urls

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BATCH_SIZE = 25  # Number of URLs to process in each batch
SLEEP_MIN = 8 * 30  # 8 minutes
SLEEP_MAX = 10 * 30  # 10 minutes
FAILED_LOG_FILE = "failed_urls.log"


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

    batch_num = 0

    while urls_left:
        batch_num += 1
        batch = urls_left[:BATCH_SIZE]
        urls_left = urls_left[BATCH_SIZE:]
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
                    logging.warning(f"⚠️ Could not retrieve HTML for {url}. Skipping.")
                    # failed_url(url) if the internet is off the url's are added to the failed_urls.json

            except Exception as e:
                logging.error(f"❌ Error while processing {url}: {e}", exc_info=True)
                print("Adding URL to failed_urls JSON File!")
                failed_url(url)

        # Wait between batches if there’s still work left
        if urls_left:
            wait_time = random.randint(SLEEP_MIN, SLEEP_MAX)
            logging.info(f"⏳ Batch {batch_num} complete. Sleeping for {wait_time // 60} minutes...")
            time.sleep(wait_time)

    logging.info("🚀 All batches processed. Scraping complete!")

if __name__ == "__main__":
    clean_failed_urls()
    main()
