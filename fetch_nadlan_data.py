import time
import os
import json
from datetime import datetime
import logging
from queue import Queue
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import StaleElementReferenceException
import pandas as pd
from selenium.webdriver.common.keys import Keys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
DRIVER_PATH = 'drivers\\chromedriver-win64\\chromedriver.exe'
NEIGHBORHOODS = [
    "תל עמל", "שער העלייה"
]
# NEIGHBORHOODS = [
#     "תל עמל", "שער העלייה", "זיו", "רמת שאול", "רמת ספיר", "רמת חן", "רמת ויזניץ", "רמת התשבי",
#     "רמת הדר", "רמת גולדה", "רמת בן גוריון", "רמת בגין", "רמת אשכול", "אוניברסיטת חיפה",
#     "רמת אלמוגי", "רמת אלון", "רמות רמז", "רוממה", "קרית הטכניון", "קריית אליעזר", "קריית אליהו",
#     "קרית חיים מערבית", "קרית חיים מזרחית", "עין הים", "סביוני הכרמל", "נמל חיפה", "נווה שאנן",
#     "נווה פז", "נווה דוד", "נוה יוסף", "מרכז הכרמל", "אזור תעשיה מפרץ", "כרמליה", "כרמל צרפתי",
#     "כרמל מערבי", "כבביר", "יזרעאליה", "חליסה", "חיפה אל עתיקה", "ורדיה", "ואדי סאליב",
#     "ואדי ניסנאס", "העיר התחתית", "המושבה הגרמנית", "הוד הכרמל-דניה", "גבעת זמר", "בת גלים",
#     "אחוזה", "אזור תעשיה חוף שמן", "אזור הקישון", "הדר", "העיר התחתית", "העיר התחתית",
#     "גבעת דאונס", "קריית שפרינצק", "מרכז תעשיות מדע", "בבנייה", "רמת הנשיא",
#     "קריית שמואל"
# ]
CHECKPOINT_INTERVAL = 100  # Save every 100 records
CHECKPOINT_DIR = 'checkpoints'
DATA_DIR = 'data/gov'
MAX_WORKERS = 4  # Number of parallel threads

# Create necessary directories
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# Global lock for thread-safe operations
checkpoint_lock = Lock()


def create_browser():
    service = Service(DRIVER_PATH)
    browser = webdriver.Chrome(service=service)
    browser.set_window_size(1500, 1000)
    return browser


def safe_get(features, idx):
    return features[idx].text if len(features) > idx else ""


def save_checkpoint(data, checkpoint_num, neighborhood):
    with checkpoint_lock:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkpoint_file = os.path.join(CHECKPOINT_DIR, f'checkpoint_{neighborhood}_{timestamp}_{checkpoint_num}.json')
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved checkpoint {checkpoint_num} with {len(data)} records for {neighborhood}")


def load_latest_checkpoint(neighborhood):
    with checkpoint_lock:
        checkpoint_files = [f for f in os.listdir(CHECKPOINT_DIR) if f.startswith(f'checkpoint_{neighborhood}_')]
        if not checkpoint_files:
            return None, 0
        latest_checkpoint = max(checkpoint_files)
        checkpoint_num = int(latest_checkpoint.split('_')[-1].split('.')[0])
        with open(os.path.join(CHECKPOINT_DIR, latest_checkpoint), 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Loaded checkpoint {checkpoint_num} with {len(data)} records for {neighborhood}")
        return data, checkpoint_num


def perform_search(browser, search_query):
    """Perform the search using the input field"""
    try:
        # Wait for the search input to be present
        search_input = wait(browser, 10).until(
            EC.presence_of_element_located((By.ID, "myInput2"))
        )

        # Clear any existing text and enter the search query
        search_input.clear()
        search_input.send_keys(search_query)
        time.sleep(1)  # Wait for suggestions to appear

        # Press Enter to submit the search
        search_input.send_keys(Keys.RETURN)
        time.sleep(2)  # Wait for the search results to load
        return True
    except Exception as e:
        logger.error(f"Error performing search: {e}")
        return False


def process_neighborhood(neighborhood):
    """Process a single neighborhood"""
    search_query = f"חיפה {neighborhood}"
    logger.info(f"Processing neighborhood: {neighborhood}")

    try:
        all_data, checkpoint_num = load_latest_checkpoint(neighborhood)
        if all_data is None:
            all_data = []
            checkpoint_num = 0

        browser = create_browser()
        url = 'https://www.nadlan.gov.il/'
        logger.info(f"Accessing URL: {url}")
        browser.get(url)
        browser.implicitly_wait(5)

        # Perform the search
        if not perform_search(browser, search_query):
            logger.error(f"Failed to perform search for {neighborhood}. Skipping.")
            return 0

        records_since_last_checkpoint = 0
        has_next = True
        page_num = 1
        MAX_PAGES = 2  # Maximum number of pages to process

        while has_next and page_num <= MAX_PAGES:
            # Always re-find the table after navigation
            for retry in range(3):
                try:
                    table = browser.find_elements(By.CLASS_NAME, "mainTable")[0]
                    break
                except (IndexError, StaleElementReferenceException):
                    logger.warning(f"Retrying to find mainTable (attempt {retry + 1}/3)...")
                    time.sleep(1)
            else:
                logger.error("Could not find mainTable after retries. Stopping.")
                break

            sell_row_data = table.find_elements(By.CLASS_NAME, "mainTable__row")
            for i in range(1, len(sell_row_data)):
                try:
                    ActionChains(browser).move_to_element(sell_row_data[i]).perform()
                    features = sell_row_data[i].find_elements(By.CLASS_NAME, "mainTable__cell")
                    row_data = {
                        'כתובת': safe_get(features, 1),
                        'מ"ר': safe_get(features, 2),
                        'תאריך עסקה': safe_get(features, 3),
                        'מחיר': safe_get(features, 4),
                        'גוש/חלקה/תת-חלקה': safe_get(features, 5),
                        'סוג נכס': safe_get(features, 6),
                        'חדרים': safe_get(features, 7),
                        'קומה': safe_get(features, 8)
                    }
                    arrow = sell_row_data[i].find_elements(By.CLASS_NAME, "collapseArrow")[0]
                    browser.execute_script("arguments[0].click();", arrow)
                    time.sleep(0.2)
                    sell_row_data_collapse = table.find_elements(By.CLASS_NAME, "innerTablesContainer")[0]
                    features = sell_row_data_collapse.find_elements(By.CLASS_NAME, "innerTable__cell")
                    row_data.update({
                        'שנת בנייה': safe_get(features, 3),
                        'מחיר למ"ר': safe_get(features, 4),
                        'קומות במבנה': safe_get(features, 5)
                    })
                    browser.execute_script("arguments[0].click();", arrow)
                    time.sleep(0.2)
                    all_data.append(row_data)
                    records_since_last_checkpoint += 1
                    if records_since_last_checkpoint >= CHECKPOINT_INTERVAL:
                        checkpoint_num += 1
                        save_checkpoint(all_data, checkpoint_num, neighborhood)
                        records_since_last_checkpoint = 0
                except Exception as e:
                    logger.error(f"Error processing row {i}: {e}")
                    continue

            # Always re-find the next button after navigation
            next_button = None
            for retry in range(3):
                try:
                    next_buttons = browser.find_elements(By.ID, "next")
                    if next_buttons and next_buttons[0].is_displayed() and next_buttons[0].is_enabled():
                        next_button = next_buttons[0]
                        break
                except StaleElementReferenceException:
                    logger.warning(f"Retrying to find next button (attempt {retry + 1}/3)...")
                    time.sleep(1)
            if not next_button:
                has_next = False
                break
            try:
                wait(browser, 10).until(EC.element_to_be_clickable((By.ID, "next")))
                browser.execute_script("arguments[0].scrollIntoView(true);", next_button)
                ActionChains(browser).move_to_element(next_button).perform()
                browser.execute_script("arguments[0].click();", next_button)
                logger.info(f"Clicked next button on page {page_num} for {neighborhood}")
                time.sleep(2)  # Give time for the page to update
                page_num += 1

                if page_num > MAX_PAGES:
                    logger.info(f"Reached maximum page limit ({MAX_PAGES}) for {neighborhood}")
                    has_next = False
                    break

            except Exception as e:
                logger.error(f"Error clicking next button: {e}")
                has_next = False

        # Save final checkpoint
        if records_since_last_checkpoint > 0:
            checkpoint_num += 1
            save_checkpoint(all_data, checkpoint_num, neighborhood)

        if all_data:
            nadlan_df = pd.DataFrame(all_data)
            csv_path = f'{DATA_DIR}/{neighborhood}.csv'
            nadlan_df.to_csv(csv_path, index=False)
            logger.info(f"Saved {len(nadlan_df)} records to {csv_path}")
            return len(nadlan_df)
        else:
            logger.warning(f"No data was collected for {neighborhood}")
            return 0

    except Exception as e:
        logger.error(f"Error processing neighborhood {neighborhood}: {e}")
        return 0
    finally:
        try:
            browser.quit()
        except:
            pass


def main():
    total_records = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all neighborhoods to the thread pool
        future_to_neighborhood = {
            executor.submit(process_neighborhood, neighborhood): neighborhood
            for neighborhood in NEIGHBORHOODS
        }

        # Process completed neighborhoods as they finish
        for future in as_completed(future_to_neighborhood):
            neighborhood = future_to_neighborhood[future]
            try:
                records = future.result()
                total_records += records
                logger.info(f"Completed processing {neighborhood} with {records} records")
            except Exception as e:
                logger.error(f"Error processing {neighborhood}: {e}")

    logger.info(f"Total records collected across all neighborhoods: {total_records}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nProcess interrupted by user. Progress has been saved in checkpoints.")
    except Exception as e:
        logger.error(f"\nAn error occurred: {e}")
        logger.info("Progress has been saved in checkpoints.") 