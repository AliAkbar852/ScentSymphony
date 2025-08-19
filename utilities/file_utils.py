import os
import csv
import json
import re
from config import OUTPUT_FOLDER


SCRAPED_URLS_FILE = "data/scraped_urls.json"

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
