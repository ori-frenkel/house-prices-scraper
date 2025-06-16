import time
import os
import json
from datetime import datetime
import logging
from queue import Queue
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

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
CHECKPOINT_INTERVAL = 100  # Save every 100 records
CHECKPOINT_DIR = 'checkpoints'
DATA_DIR = 'data/gov'
MAX_WORKERS = 1  # Reduced to 1 to avoid conflicts on the same site
MAX_PAGES = 2  # Maximum number of pages to process

# Create necessary directories
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# Global lock for thread-safe operations
checkpoint_lock = Lock()


def create_record_hash(record):
    """Create a unique hash for a record to detect duplicates"""
    # Create a string from key fields that should be unique
    key_fields = f"{record.get('כתובת', '')}-{record.get('תאריך עסקה', '')}-{record.get('מחיר', '')}-{record.get('גוש/חלקה/תת-חלקה', '')}"
    return hashlib.md5(key_fields.encode('utf-8')).hexdigest()


def create_browser():
    service = Service(DRIVER_PATH)
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    browser = webdriver.Chrome(service=service, options=options)
    browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    browser.set_window_size(1500, 1000)
    return browser


def safe_get(features, idx):
    return features[idx].text.strip() if len(features) > idx else ""


def extract_multiple_transactions(features, base_row_data):
    """Extract multiple transactions from a single row"""
    transactions = []

    # First, always add the original transaction (this is the base row data)
    transactions.append(base_row_data.copy())

    # Start checking from index 8 for additional transactions
    transaction_index = 0
    while True:
        date_idx = 8 + (transaction_index * 2)
        price_idx = 9 + (transaction_index * 2)

        transaction_date = safe_get(features, date_idx)
        transaction_price = safe_get(features, price_idx)

        # If we get empty strings, we've reached the end
        if not transaction_date and not transaction_price:
            break

        # Create a new row with the base data but updated date and price
        transaction_row = base_row_data.copy()

        # Update with the specific transaction data
        if transaction_date:
            transaction_row['תאריך עסקה'] = transaction_date
        if transaction_price:
            transaction_row['מחיר'] = transaction_price

        transactions.append(transaction_row)
        transaction_index += 1

    return transactions


def save_checkpoint(data, seen_hashes, checkpoint_num, neighborhood):
    """Save checkpoint with both data and seen hashes"""
    with checkpoint_lock:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkpoint_file = os.path.join(CHECKPOINT_DIR, f'checkpoint_{neighborhood}_{timestamp}_{checkpoint_num}.json')
        checkpoint_data = {
            'data': data,
            'seen_hashes': list(seen_hashes),
            'timestamp': timestamp,
            'record_count': len(data)
        }
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved checkpoint {checkpoint_num} with {len(data)} unique records for {neighborhood}")


def load_latest_checkpoint(neighborhood):
    """Load the latest checkpoint with seen hashes"""
    with checkpoint_lock:
        checkpoint_files = [f for f in os.listdir(CHECKPOINT_DIR) if f.startswith(f'checkpoint_{neighborhood}_')]
        if not checkpoint_files:
            return None, set(), 0

        latest_checkpoint = max(checkpoint_files)
        checkpoint_num = int(latest_checkpoint.split('_')[-1].split('.')[0])

        try:
            with open(os.path.join(CHECKPOINT_DIR, latest_checkpoint), 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)

            # Handle both old and new checkpoint formats
            if isinstance(checkpoint_data, list):
                # Old format - just data
                data = checkpoint_data
                seen_hashes = {create_record_hash(record) for record in data}
            else:
                # New format - data + seen_hashes
                data = checkpoint_data.get('data', [])
                seen_hashes = set(checkpoint_data.get('seen_hashes', []))

            logger.info(
                f"Loaded checkpoint {checkpoint_num} with {len(data)} records and {len(seen_hashes)} seen hashes for {neighborhood}")
            return data, seen_hashes, checkpoint_num
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
            return None, set(), 0


def perform_search(browser, search_query):
    """Perform the search using the input field"""
    try:
        # Wait for the search input to be present
        search_input = wait(browser, 10).until(
            EC.presence_of_element_located((By.ID, "myInput2"))
        )

        # Clear any existing text and enter the search query
        search_input.clear()
        time.sleep(0.5)
        search_input.send_keys(search_query)
        time.sleep(1)  # Wait for suggestions to appear

        # Press Enter to submit the search
        search_input.send_keys(Keys.RETURN)
        time.sleep(3)  # Wait for the search results to load

        # Verify search results loaded
        try:
            wait(browser, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "mainTable"))
            )
            return True
        except:
            logger.error("Search results did not load properly")
            return False

    except Exception as e:
        logger.error(f"Error performing search: {e}")
        return False


def wait_for_page_load(browser, timeout=10):
    """Wait for page to fully load after navigation"""
    try:
        wait(browser, timeout).until(
            lambda driver: driver.execute_script("return document.readyState") == "complete"
        )
        time.sleep(1)  # Additional buffer
        return True
    except:
        return False


def process_neighborhood(neighborhood):
    """Process a single neighborhood with duplicate detection and multiple transactions"""
    search_query = f"חיפה {neighborhood}"
    logger.info(f"Processing neighborhood: {neighborhood}")

    try:
        # Load existing data and seen hashes
        all_data, seen_hashes, checkpoint_num = load_latest_checkpoint(neighborhood)
        if all_data is None:
            all_data = []
            seen_hashes = set()
            checkpoint_num = 0

        initial_count = len(all_data)
        logger.info(f"Starting with {initial_count} existing records for {neighborhood}")

        browser = create_browser()
        url = 'https://www.nadlan.gov.il/'
        logger.info(f"Accessing URL: {url}")
        browser.get(url)

        # Wait for page to load completely
        if not wait_for_page_load(browser):
            logger.error("Initial page load failed")
            return len(all_data)

        # Perform the search
        if not perform_search(browser, search_query):
            logger.error(f"Failed to perform search for {neighborhood}. Skipping.")
            return len(all_data)

        records_since_last_checkpoint = 0
        has_next = True
        page_num = 1
        duplicates_found = 0
        new_records_this_session = 0

        while has_next and page_num <= MAX_PAGES:
            logger.info(f"Processing page {page_num} for {neighborhood}")

            # Wait for page to load after navigation
            if not wait_for_page_load(browser):
                logger.error(f"Page {page_num} failed to load properly")
                break

            # Find the table with retries
            table = None
            for retry in range(3):
                try:
                    tables = browser.find_elements(By.CLASS_NAME, "mainTable")
                    if tables:
                        table = tables[0]
                        break
                except StaleElementReferenceException:
                    logger.warning(f"Retrying to find mainTable (attempt {retry + 1}/3)...")
                    time.sleep(2)

            if not table:
                logger.error("Could not find mainTable after retries. Stopping.")
                break

            # Get all rows
            try:
                sell_row_data = table.find_elements(By.CLASS_NAME, "mainTable__row")
                logger.info(f"Found {len(sell_row_data)} rows on page {page_num}")
            except Exception as e:
                logger.error(f"Error finding rows: {e}")
                break

            # Process each row (skip header row)
            for i in range(1, len(sell_row_data)):
                try:
                    # Scroll to element to ensure it's visible
                    browser.execute_script("arguments[0].scrollIntoView(true);", sell_row_data[i])
                    time.sleep(0.2)

                    # Get basic row data
                    features = sell_row_data[i].find_elements(By.CLASS_NAME, "mainTable__cell")
                    base_row_data = {
                        'כתובת': safe_get(features, 1),
                        'מ"ר': safe_get(features, 2),
                        'תאריך עסקה': safe_get(features, 3),
                        'מחיר': safe_get(features, 4),
                        'גוש/חלקה/תת-חלקה': safe_get(features, 5),
                        'סוג נכס': safe_get(features, 6),
                        'חדרים': safe_get(features, 7),
                        'קומה': safe_get(features, 8)
                    }

                    # Expand row for additional details
                    arrows = sell_row_data[i].find_elements(By.CLASS_NAME, "collapseArrow")
                    if not arrows:
                        logger.warning(f"No collapse arrow found for row {i}")
                        continue

                    arrow = arrows[0]
                    browser.execute_script("arguments[0].click();", arrow)
                    time.sleep(0.3)

                    # Get expanded details
                    try:
                        inner_containers = table.find_elements(By.CLASS_NAME, "innerTablesContainer")
                        if inner_containers:
                            features = inner_containers[0].find_elements(By.CLASS_NAME, "innerTable__cell")
                            base_row_data.update({
                                'שנת בנייה': safe_get(features, 3),
                                'מחיר למ"ר': safe_get(features, 4),
                                'קומות במבנה': safe_get(features, 5)
                            })

                            # Extract multiple transactions from the expanded row
                            transactions = extract_multiple_transactions(features, base_row_data)

                            # If no additional transactions found, use the original row
                            if not transactions:
                                transactions = [base_row_data]

                    except Exception as e:
                        logger.warning(f"Could not get expanded details for row {i}: {e}")
                        transactions = [base_row_data]

                    # Collapse the row back
                    browser.execute_script("arguments[0].click();", arrow)
                    time.sleep(0.2)

                    # Process each transaction
                    for transaction in transactions:
                        # Check for duplicates
                        record_hash = create_record_hash(transaction)
                        if record_hash in seen_hashes:
                            duplicates_found += 1
                            logger.debug(f"Duplicate found: {transaction.get('כתובת', 'Unknown')}")
                            continue

                        # Add to our data
                        all_data.append(transaction)
                        seen_hashes.add(record_hash)
                        records_since_last_checkpoint += 1
                        new_records_this_session += 1

                    # Save checkpoint periodically
                    if records_since_last_checkpoint >= CHECKPOINT_INTERVAL:
                        checkpoint_num += 1
                        save_checkpoint(all_data, seen_hashes, checkpoint_num, neighborhood)
                        records_since_last_checkpoint = 0

                except Exception as e:
                    logger.error(f"Error processing row {i} on page {page_num}: {e}")
                    continue

            # Check for next page
            has_next = False
            try:
                next_buttons = browser.find_elements(By.ID, "next")
                if next_buttons:
                    next_button = next_buttons[0]
                    if next_button.is_displayed() and next_button.is_enabled():
                        # Scroll to next button
                        browser.execute_script("arguments[0].scrollIntoView(true);", next_button)
                        time.sleep(0.5)

                        # Click next button
                        browser.execute_script("arguments[0].click();", next_button)
                        logger.info(f"Navigated to page {page_num + 1} for {neighborhood}")
                        time.sleep(3)  # Wait for navigation

                        page_num += 1
                        has_next = (page_num <= MAX_PAGES)
                    else:
                        logger.info(f"Next button not available - reached end for {neighborhood}")
                else:
                    logger.info(f"No next button found - reached end for {neighborhood}")
            except Exception as e:
                logger.error(f"Error navigating to next page: {e}")
                has_next = False

        # Save final checkpoint
        if records_since_last_checkpoint > 0:
            checkpoint_num += 1
            save_checkpoint(all_data, seen_hashes, checkpoint_num, neighborhood)

        # Save final CSV
        if all_data:
            # Remove any remaining duplicates (just in case)
            df = pd.DataFrame(all_data)
            df_unique = df.drop_duplicates(subset=['כתובת', 'תאריך עסקה', 'מחיר', 'גוש/חלקה/תת-חלקה'])

            csv_path = f'{DATA_DIR}/{neighborhood}.csv'
            df_unique.to_csv(csv_path, index=False, encoding='utf-8-sig')

            logger.info(f"Completed {neighborhood}:")
            logger.info(f"  - Total unique records: {len(df_unique)}")
            logger.info(f"  - New records this session: {new_records_this_session}")
            logger.info(f"  - Duplicates skipped: {duplicates_found}")
            logger.info(f"  - Records removed in final dedup: {len(df) - len(df_unique)}")
            logger.info(f"  - Saved to: {csv_path}")

            return len(df_unique)
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
    """Main function to process all neighborhoods"""
    logger.info(f"Starting scraper for {len(NEIGHBORHOODS)} neighborhoods")
    logger.info(f"Neighborhoods: {', '.join(NEIGHBORHOODS)}")

    total_records = 0

    # Process neighborhoods sequentially to avoid conflicts
    for neighborhood in NEIGHBORHOODS:
        try:
            records = process_neighborhood(neighborhood)
            total_records += records
            logger.info(f"Completed {neighborhood}: {records} records")

            # Brief pause between neighborhoods
            time.sleep(2)

        except Exception as e:
            logger.error(f"Failed to process {neighborhood}: {e}")
            continue

    logger.info(f"Scraping completed!")
    logger.info(f"Total unique records collected: {total_records}")
    logger.info(f"Data saved in: {DATA_DIR}")
    logger.info(f"Checkpoints saved in: {CHECKPOINT_DIR}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nProcess interrupted by user. Progress has been saved in checkpoints.")
    except Exception as e:
        logger.error(f"\nAn error occurred: {e}")
        logger.info("Progress has been saved in checkpoints.")
