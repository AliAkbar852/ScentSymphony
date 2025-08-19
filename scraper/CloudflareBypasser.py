from DrissionPage import ChromiumPage, ChromiumOptions
from scraper.bypass_core import CloudflareBypasser   # ✅ FIXED
from selenium import webdriver
import undetected_chromedriver as uc

def get_page_html(url):
    page = ChromiumPage()
    page.get(url)
    driver=page
    bypasser = CloudflareBypasser(driver, max_retries=5, log=True)
    bypasser.bypass()
      # 2. Dismiss Adblock popup

    html = page.html
    page.quit()
    return html
