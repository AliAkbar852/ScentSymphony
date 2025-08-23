import logging
import re
from bs4 import BeautifulSoup
from utilities.dbmanager import DBManager
from utilities.file_utils import normalize_key, failed_url
from .selenium_scraper import scrape_all_reviews_with_selenium

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Extractor:
    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager

    def process_and_save(self, html_content: str, url: str):
        perfume_data = self._extract_all_data(html_content, url)
        if perfume_data:
            self._save_to_relational_db(perfume_data)
        else:
            logging.warning(f"Could not extract any data for URL: {url}. Skipping database insertion.")

    def _save_to_relational_db(self, data: dict):
        logging.info(f"Processing data for '{data.get('perfume_name')}' for the database...")

        # CHANGED
        brand_id = self.db_manager.get_or_create_brand(
            brand_name=data.get('brand_name'),
            country_id=None,
            brand_url=None,
            perfume_count=None,
            brand_website_url=None,
            brand_image_url=None
        )

        # CHANGED
        perfume_id = self.db_manager.get_or_create_perfume(
            perfume_name=data.get('perfume_name'),
            perfume_for=data.get('perfume_for'),
            image_url=data.get('image_url'),
            launch_year=data.get('launch_year'),
            perfumer_name=data.get('perfumer_name'),
            perfumer_url=data.get('perfumer_url'),
            perfume_url=data.get('perfume_url'),
            brand_id=brand_id
        )

        if not perfume_id:
            logging.error(f"❌ Failed to get or create perfume ID for '{data.get('perfume_name')}'.")
            return

        self.db_manager.clear_perfume_details(perfume_id)
        logging.info(f"Updating all details for PerfumeID: {perfume_id}")

        self.db_manager.insert_perfume_vote(perfume_id, data)

        for category in ["possession", "emotional_attachment", "wearing_season"]:
            if category in data:
                self.db_manager.insert_perfume_percentages(perfume_id, category, data[category])

        for category in ["longevity", "sillage", "gender", "price_value"]:
            if category in data:
                self.db_manager.insert_perfume_stats(perfume_id, category, data[category])

        if data.get('reviews'):
            self.db_manager.insert_reviews(perfume_id, data['reviews'])

        for accord_info in data.get('main_accords', []):
            accord_name = accord_info.get('name')
            accord_strength = accord_info.get('strength')
            if accord_name:
                accord_id = self.db_manager.get_or_create_id("Accords", "accord", accord_name.strip())
                if accord_id:
                    self.db_manager.link_perfume_accord(perfume_id, accord_id, accord_strength)

        for level_key, notes_list in data.get('perfume_pyramid', {}).items():
            level = level_key.replace('_notes', '')
            for note_name in notes_list:
                note_id = self.db_manager.get_or_create_id("Notes", "note", note_name.strip())
                if note_id:
                    # CHANGED
                    self.db_manager.link_perfume_note(perfume_id, note_id, note_level=level)

        for note_name in data.get('linear_notes', []):
            note_id = self.db_manager.get_or_create_id("Notes", "note", note_name.strip())
            if note_id:
                # CHANGED
                self.db_manager.link_perfume_note(perfume_id, note_id, note_level='linear')

        logging.info(f"✅ Finished processing all data for PerfumeID {perfume_id}.")

    def _extract_all_data(self, html_content: str, url: str) -> dict:
        soup = BeautifulSoup(html_content, "html.parser")
        # CHANGED
        data = {"perfume_url": url}
        extractor_methods = [
            self._extract_title, self._extract_brand, self._extract_image_url,
            self._extract_reviews_and_ratings, self._extract_main_accords,
            self._extract_vote_sections, self._extract_notes_pyramid,
            self._extract_linear_notes_if_no_pyramid,
            lambda s: self._extract_section_votes(s, 'LONGEVITY', 'longevity'),
            lambda s: self._extract_section_votes(s, 'SILLAGE', 'sillage'),
            lambda s: self._extract_section_votes(s, 'GENDER', 'gender'),
            lambda s: self._extract_section_votes(s, 'PRICE VALUE', 'price_value'),
            self._extract_perfumer_info, self._parse_launch_year, self._extract_description
        ]
        for method in extractor_methods:
            try:
                data.update(method(soup))
            except Exception as e:
                logging.warning(f"No Value found in {getattr(method, '__name__', 'lambda')}: {e}")

        try:
            # IMPORTANT: This function must now return reviews with keys:
            # 'review_content', 'reviewer_name', 'review_date'
            data.update(scrape_all_reviews_with_selenium(url))
        except Exception as e:
            logging.error(f"❌ Selenium review scraping failed for {url}: {e}")
            print("Adding URL to JSON file!")
            failed_url(url)
        return data

    # ---------- Individual Extract Methods Below ----------

    # CHANGED
    def _extract_title(self, soup):
        title_tag = soup.select_one('#toptop > h1')
        return {
            "perfume_name": title_tag.contents[0].strip() if title_tag and title_tag.contents else "",
            "perfume_for": title_tag.find('small').get_text(strip=True) if title_tag and title_tag.find('small') else ""
        }

    def _extract_brand(self, soup):
        tag = soup.select_one('span.vote-button-name')
        return {"brand_name": tag.get_text(strip=True) if tag else "N/A"}

    def _extract_image_url(self, soup):
        img = soup.find('img', itemprop='image')
        return {"image_url": img.get("src") if img else "N/A"}

    def _extract_description(self, soup):
        desc = soup.find('div', itemprop='description')
        return {"description": desc.p.get_text(strip=True) if desc and desc.p else "N/A"}

    def _extract_reviews_and_ratings(self, soup):
        return {
            "review_count": soup.find('meta', itemprop='reviewCount').get("content") if soup.find('meta',                                                                                          itemprop='reviewCount') else "0",
            "rating_count": soup.find('span', itemprop='ratingCount').get_text(strip=True) or "0",
            "rating_value": soup.find('span', itemprop='ratingValue').get_text(strip=True) or "0"
        }

    def _extract_main_accords(self, soup):
        accords = []
        strengths = []
        for a in soup.select('div.cell.accord-box > div.accord-bar'):
            name = a.get_text(strip=True)
            width = re.search(r'width\s*:\s*([\d\.]+)', a.get("style", ""))
            if name and width:
                strength = float(width.group(1))
                accords.append({"name": name, "strength": strength})
                strengths.append(strength)
        return {
            "main_accords": accords,
            "strengths": strengths
        }

    def _extract_vote_sections(self, soup):
        brands = soup.select('span.vote-button-name')
        legends = soup.select('span.vote-button-legend')
        bars = soup.select('.voting-small-chart-size > div > div')

        sections = {
            "possession": (brands[1:4], bars[:3]),
            "emotional_attachment": (legends[:5], bars[3:8]),
            "wearing_season": (legends[5:11], bars[8:])
        }

        results = {}
        for section_name, (labels, bars_slice) in sections.items():
            section_data = {}
            for label, bar in zip(labels, bars_slice):
                key = normalize_key(label.get_text())
                match = re.search(r'width:\s*([\d.]+)%', bar.get("style", ""))
                value = f"{float(match.group(1)):.2f}%"
                if match:
                    value = round(float(match.group(1)), 2)
                else:
                    value = None
                section_data[key] = value
            results[section_name] = section_data
        return results

    def _extract_section_votes(self, soup, section_title, key_name):
        anchor = soup.find('span', string=section_title)
        container = anchor.find_parent() if anchor else None
        while container and not container.select('span.vote-button-name'):
            container = container.find_parent()
        section_data = {}
        if container:
            for label, progress in zip(container.select('span.vote-button-name'), container.find_all('progress')):
                section_data[normalize_key(label.get_text())] = progress.get('value', '0')
        return {key_name: section_data}

    def _extract_notes_pyramid(self, soup):
        pyramid = {}
        for h4 in soup.select('#pyramid h4'):
            level = normalize_key(h4.get_text())
            container = h4.find_next_sibling('div')
            if container:
                notes = [n.get_text(strip=True) for n in container.select('div[style*="margin: 0.2rem"]') if
                         n.get_text(strip=True)]
                pyramid[level] = notes
        return {"perfume_pyramid": pyramid}

    def _extract_linear_notes_if_no_pyramid(self, soup):
        if soup.select('#pyramid h4'):
            return {}
        notes = [div.get_text(strip=True) for div in soup.select('div[style*="margin: 0.2rem"] > div:nth-child(2)') if
                 div.get_text(strip=True)]
        return {"linear_notes": notes} if notes else {}

    def _extract_perfumer_info(self, soup):
        avatar = soup.find('img', class_='perfumer-avatar')
        link = avatar.find_next_sibling('a') if avatar else None
        return {
            "perfumer_name": link.get_text(strip=True) if link else "N/A",
            "perfumer_url": link.get("href") if link else "N/A"
        }

    def _parse_launch_year(self, soup):
        text = soup.get_text(separator=" ")
        match = re.search(r'was launched (?:in|during the) (\d{4})(?:\'?s)?', text)
        if match:
            return {"launch_year": match.group(1)}
        else:
            return {"launch_year": "N/A"}