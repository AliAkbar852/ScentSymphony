import os
import csv
import json
import re
from config import OUTPUT_FOLDER
from datetime import datetime

SCRAPED_URLS_FILE = "data/scraped_urls.json"
FAILED_FILE = "data/failed_urls.json"

def clean_failed_urls():
    # Load failed urls
    with open(FAILED_FILE, "r", encoding="utf-8") as f:
        failed_data = json.load(f)

    # Load scraped urls
    with open(SCRAPED_URLS_FILE, "r", encoding="utf-8") as f:
        scraped_data = json.load(f)

    # Extract failed urls
    failed_urls = [item["url"] for item in failed_data]

    # Remove failed URLs if they exist in scraped_data
    scraped_data = [url for url in scraped_data if url not in failed_urls]

    # Save updated scraped_urls.json
    with open(SCRAPED_URLS_FILE, "w", encoding="utf-8") as f:
        json.dump(scraped_data, f, indent=4, ensure_ascii=False)

    # Clear failed_urls.json completely
    with open(FAILED_FILE, "w", encoding="utf-8") as f:
        json.dump([], f, indent=4, ensure_ascii=False)

    print("Cleanup completed successfully ✅")

def normalize_key(text):
    return re.sub(r'\W+', '_', text.strip().lower())

# def get_scraped_titles():
#     if not os.path.exists(OUTPUT_FOLDER):
#         return set()
#     files = os.listdir(OUTPUT_FOLDER)
#     return {os.path.splitext(f)[0].lower() for f in files if f.endswith(".json")}

def save_json(data):
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    title = data.get("title", "untitled").strip().replace(" ", "_")
    safe_title = re.sub(r'[\\/*?:"<>|]', '_', title)
    path = os.path.join(OUTPUT_FOLDER, f"{safe_title}.json")
    with open(path, 'w', encoding='utf-8') as jf:
        json.dump(data, jf, ensure_ascii=False, indent=4)
    print(f"📄 Saved JSON: {path}")

def read_urls_from_csv(csv_path):
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skips the header row
        return [row[0].strip() for row in reader if row]
    
def load_scraped_urls():
    if os.path.exists(SCRAPED_URLS_FILE):
        with open(SCRAPED_URLS_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

def save_scraped_urls(urls):
    with open(SCRAPED_URLS_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(urls), f, indent=2)

FAILED_LOG_FILE = "data/failed_urls.json"


def failed_url(url):
    """Save failed URLs in a JSON array with timestamps."""
    log_entry = {"time": datetime.now().isoformat(), "url": url}

    # Load existing data if file exists
    if os.path.exists(FAILED_LOG_FILE):
        with open(FAILED_LOG_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = []
    else:
        data = []

    # Append new entry and save
    data.append(log_entry)
    with open(FAILED_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)