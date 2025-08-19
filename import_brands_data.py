import json
import csv
import logging
from utilities.dbmanager import DBManager

# --- CONFIGURATION ---
# Please ensure these file paths are correct for your project structure
COUNTRIES_JSON_PATH = 'data/countries.json'
BRANDS_JSON_PATH = 'data/brands.json'
DETAILS_CSV_PATH = 'data/perfume_url.csv'  # Path to your CSV file
BASE_URL = "https://www.fragrantica.com"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_brand_details_from_csv(csv_path):
    """
    Reads a headerless CSV file using column positions and creates a lookup dictionary.
    It also cleans the brand name by removing ' perfumes and colognes'.
    """
    logging.info(f"Loading brand details from {csv_path}...")
    details_lookup = {}
    try:
        with open(csv_path, mode='r', encoding='utf-8') as infile:
            # Using csv.reader for a headerless file
            reader = csv.reader(infile)

            for row in reader:
                # **FIXED LOGIC**: Reads every row without skipping a header.
                # Assuming the column order from your screenshot:
                # column 1 (index 0): brand_name
                # column 2 (index 1): brand_image_url
                # column 3 (index 2): country
                # column 4 (index 3): brand_website
                if not row or len(row) < 4: continue  # Skip empty or malformed rows

                original_name = row[0].strip()
                cleaned_name = original_name.replace(' perfumes and colognes', '').strip()

                # Store the details if the name is not already present
                if cleaned_name and cleaned_name not in details_lookup:
                    details_lookup[cleaned_name] = {
                        'WebsiteUrl': row[3],
                        'imageUrl': row[1]
                    }

        if not details_lookup:
            logging.warning(
                "CSV loaded, but it resulted in 0 unique brands. Check the file content and cleaning logic.")
        else:
            logging.info(f"Successfully loaded details for {len(details_lookup)} unique brands from CSV.")
        return details_lookup

    except FileNotFoundError:
        logging.error(f"FATAL: CSV file not found at '{csv_path}'. Please check the path in the script.")
        return {}
    except IndexError:
        logging.error(f"FATAL: A row in the CSV file has fewer columns than expected. Please check the file format.")
        return {}


def populate_countries_and_brands(db_manager, brand_details):
    """
    Reads the JSON files to populate the Countries and Brands tables.
    """
    # --- Populate Countries Table ---
    logging.info(f"Populating Countries table from {COUNTRIES_JSON_PATH}...")
    try:
        with open(COUNTRIES_JSON_PATH, 'r', encoding='utf-8') as f:
            countries_data = json.load(f)

        country_id_map = {}
        for name, count in countries_data.items():
            country_id = db_manager.get_or_create_country(name, count)
            if country_id:
                country_id_map[name] = country_id
        logging.info("Finished populating Countries table.")
    except FileNotFoundError:
        logging.error(f"FATAL: Countries JSON file not found at {COUNTRIES_JSON_PATH}.")
        return

    # --- Populate Brands Table ---
    logging.info(f"Populating Brands table from {BRANDS_JSON_PATH}...")
    try:
        with open(BRANDS_JSON_PATH, 'r', encoding='utf-8') as f:
            brands_by_country = json.load(f)

        for country_name, brands_list in brands_by_country.items():
            country_id = country_id_map.get(country_name)
            if not country_id:
                logging.warning(f"Could not find ID for country '{country_name}'. Skipping its brands.")
                continue

            for brand_data in brands_list:
                brand_name = brand_data.get('brand_name').strip()

                details = brand_details.get(brand_name, {})

                db_manager.get_or_create_brand(
                    name=brand_name,
                    countryID=country_id,
                    BrandUrl=f"{BASE_URL}{brand_data.get('brand_url')}",
                    PerfumeCount=brand_data.get('perfume_count'),
                    WebsiteUrl=details.get('WebsiteUrl'),
                    imageUrl=details.get('imageUrl')
                )
        logging.info("Finished populating Brands table.")
    except FileNotFoundError:
        logging.error(f"FATAL: Brands JSON file not found at {BRANDS_JSON_PATH}.")


def main():
    """
    Main function to orchestrate the data import process.
    """
    logging.info("--- Starting Brand and Country Data Import ---")

    db = DBManager()
    db.create_tables()

    brand_details_lookup = load_brand_details_from_csv(DETAILS_CSV_PATH)

    if brand_details_lookup:
        populate_countries_and_brands(db, brand_details_lookup)
    else:
        logging.warning("Brand details lookup failed or returned empty. Halting the import process.")

    logging.info("--- Data Import Process Complete ---")


if __name__ == "__main__":
    main()