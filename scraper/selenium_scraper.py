import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from utilities.file_utils import failed_url


# Due to space limitations, placeholder only
def scrape_all_reviews_with_selenium(url):
    # --- 1. Setup undetected Chrome driver ---
    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-site-isolation-trials")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-default-apps")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-client-side-phishing-detection")
    options.add_argument("--disable-service-worker")  # Disable service workers

    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = uc.Chrome(options=options)
    wait = WebDriverWait(driver, 6)
    scraped_data = []
    try:
        # --- 2. Navigate to the URL ---
        print(f"Navigating to: {url}")
        driver.get(url)

        time.sleep(3)

        # --- 3. Wait for loader to disappear ---
        try:
            wait.until(EC.invisibility_of_element_located((By.ID, "fragranticaloader")))
            print("Loader disappeared.")
        except TimeoutException:
            print("Loader did not disappear in time. Proceeding anyway.")

        # --- 4. Handle cookie consent (if exists) ---
        try:
            cookie_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'AGREE')]"))
            )
            cookie_button.click()
            print("Cookie consent clicked.")
            time.sleep(1)
        except TimeoutException:
            print("No cookie consent found.")

        # --- 5. Scroll using #popBrands logic ---
        print("Scrolling using #popBrands logic...")

        last_height = driver.execute_script("return document.body.scrollHeight")

        try:
            target_div = driver.find_element(By.XPATH,
                                             "//div[@id='popBrands' and .//span[text()='Most Popular Perfumes']]")
        except NoSuchElementException:
            print("❌ #popBrands not found. Falling back to normal scroll.")
            target_div = None

        while True:
            if target_div:
                driver.execute_script("arguments[0].scrollIntoView();", target_div)
                time.sleep(0.5)
                driver.execute_script("window.scrollBy(0, -540);")
                time.sleep(4)  # Give time to load more reviews
            else:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

            new_height = driver.execute_script("return document.body.scrollHeight")

            if new_height == last_height:
                print("No more height change. Stopping.")
                break
            else:
                print("Page grew, continue scrolling...")
                last_height = new_height
            try:
                no_more_data_xpath = "//div[contains(@class, 'infinite-status-prompt') and contains(text(), 'No more data')]"
                end_element = driver.find_element(By.XPATH, no_more_data_xpath)
                if end_element.is_displayed():
                    print("Detected 'No more data' message. Stopping.")
                    break
            except NoSuchElementException:
                pass
        try:
            review_conatiner = driver.find_element(By.XPATH, "//span[text()='All Reviews By Date']")
            if review_conatiner.is_displayed():
                print("Scrolling was Successfull ")
        except NoSuchElementException:
            print("Scrolling was Failed Due to Some Pop-Up or Something!")
            print("Adding URL to JSON file!")
            failed_url(url)

        print("Finished scrolling. Waiting to let final reviews fully render...")

        # --- 6. Extract data from all review containers ---
        review_containers = driver.find_elements(By.CLASS_NAME, 'fragrance-review-box')
        print(f"Extraction started. Total review containers found: {len(review_containers)}")

        skipped_reviews = 0
        for i, review in enumerate(review_containers):
            review_text = ""  # Reset text for each loop
            review_date = None  # Reset date for each loop
            try:
                # STRATEGY 1: Look for the structure used in lazy-loaded reviews
                text_element = review.find_element(By.CSS_SELECTOR, 'div.flex-child-auto p')
                review_text = text_element.text.strip()

            except NoSuchElementException:
                # If that fails, it might be the initial page load structure
                try:
                    # STRATEGY 2: Look for the original structure with itemprop
                    text_element = review.find_element(By.CSS_SELECTOR, 'div[itemprop="reviewBody"]')
                    review_text = text_element.text.strip()
                except NoSuchElementException:
                    skipped_reviews += 1
                    continue

            try:
               username_element = review.find_element(By.CSS_SELECTOR, "b.idLinkify a")
               username = username_element.text.strip()
            #    print("Username:", username)
            except NoSuchElementException:
                print("Username not found for a review. Skipping extraction.")

            # Now, attempt to find the date for the review
            try:
                date_element = review.find_element(By.CSS_SELECTOR, 'span[itemprop="datePublished"]')
                review_date = date_element.get_attribute("content")
            except NoSuchElementException:
                # This is not critical, so we'll just log and continue without a date.
                print("Date not found for a review. Skipping date extraction.")

            # Add the found text and date to our list if text is not empty
            if review_text:
                # CHANGED: Updated dictionary keys to match dbmanager expectations
                scraped_data.append({'review_content': review_text, 'review_date': review_date, 'reviewer_name': username})
            else:
                skipped_reviews += 1

        print(f"Extraction complete. Successfully parsed {len(scraped_data)} reviews.")
        if skipped_reviews > 0:
            print(f"Skipped {skipped_reviews} containers that were ads or empty placeholders.")

    except Exception as e:
        print("Error:", e)
        print("Adding URL to failed_url JSON file!")
        failed_url(url)
    finally:
        driver.quit()

    return {"reviews": scraped_data}