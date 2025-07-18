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
CITY_NAME = "חיפה"
# NEIGHBORHOODS = [
#     "רמת אלון", "רמות רמז", "רוממה", "קרית הטכניון", "קריית אליעזר", "קריית אליהו",
#     "קרית חיים מערבית", "קרית חיים מזרחית", "עין הים", "סביוני הכרמל", "נמל חיפה", "נווה שאנן",
#     "נווה פז", "נווה דוד", "נוה יוסף"
# ]
NEIGHBORHOODS = [
    "נוה פז"
]
CHECKPOINT_INTERVAL = 100  # Save every 100 records
CHECKPOINT_DIR = 'checkpoints'
DATA_DIR = 'data/gov'
MAX_WORKERS = 1  # Reduced number of concurrent threads to avoid overwhelming the system
MAX_PAGES = 100  # Maximum number of pages to process

# Create necessary directories
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# Global lock for thread-safe operations
checkpoint_lock = Lock()
logging_lock = Lock()


def thread_safe_log(message, level='info'):
    """Thread-safe logging function"""
    with logging_lock:
        if level == 'info':
            logger.info(message)
        elif level == 'error':
            logger.error(message)
        elif level == 'warning':
            logger.warning(message)


def create_record_hash(record):
    """Create a unique hash for a record to detect duplicates"""
    # Create a string from key fields that should be unique
    key_fields = f"{record.get('כתובת', '')}-{record.get('תאריך עסקה', '')}-{record.get('מחיר', '')}-{record.get('גוש/חלקה/תת-חלקה', '')}"
    return hashlib.md5(key_fields.encode('utf-8')).hexdigest()


def create_browser():
    """Create a new Chrome browser instance with thread-safe options"""
    service = Service(DRIVER_PATH)
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    # Add thread-safe options for parallel execution
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--remote-debugging-port=0')  # Use random port for each instance

    browser = webdriver.Chrome(service=service, options=options)
    browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    browser.set_window_size(1500, 1000)
    return browser


def safe_get(features, idx):
    return features[idx].text.strip() if len(features) > idx else ""


def extract_multiple_transactions(features, base_row_data):
    """Extract multiple transactions from a single row, including the original"""
    transactions = []

    # First, always add the original transaction (this is the base row data)
    transactions.append(base_row_data.copy())

    # Then check for additional transactions starting from index 8
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
        additional_transaction = base_row_data.copy()

        # Update with the specific transaction data
        if transaction_date:
            additional_transaction['תאריך עסקה'] = transaction_date
        if transaction_price:
            additional_transaction['מחיר'] = transaction_price

        transactions.append(additional_transaction)
        transaction_index += 1

    return transactions


def save_checkpoint(data, seen_hashes, checkpoint_num, neighborhood):
    """Save checkpoint with both data and seen hashes - thread-safe"""
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
        thread_safe_log(f"Saved checkpoint {checkpoint_num} with {len(data)} unique records for {neighborhood}")


def load_latest_checkpoint(neighborhood):
    """Load the latest checkpoint with seen hashes - thread-safe"""
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

            thread_safe_log(
                f"Loaded checkpoint {checkpoint_num} with {len(data)} records and {len(seen_hashes)} seen hashes for {neighborhood}")
            return data, seen_hashes, checkpoint_num
        except Exception as e:
            thread_safe_log(f"Error loading checkpoint: {e}", 'error')
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
            thread_safe_log("Search results did not load properly", 'error')
            return False

    except Exception as e:
        thread_safe_log(f"Error performing search: {e}", 'error')
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
    search_query = f"{CITY_NAME} {neighborhood}"
    thread_safe_log(f"Processing neighborhood: {neighborhood}")

    browser = None
    try:
        # Load existing data and seen hashes
        all_data, seen_hashes, checkpoint_num = load_latest_checkpoint(neighborhood)
        if all_data is None:
            all_data = []
            seen_hashes = set()
            checkpoint_num = 0

        initial_count = len(all_data)
        thread_safe_log(f"Starting with {initial_count} existing records for {neighborhood}")

        # Create browser instance for this thread
        browser = create_browser()
        url = 'https://www.nadlan.gov.il/'
        thread_safe_log(f"Accessing URL: {url} for {neighborhood}")
        browser.get(url)

        # Wait for page to load completely
        if not wait_for_page_load(browser):
            thread_safe_log(f"Initial page load failed for {neighborhood}", 'error')
            return len(all_data)

        # Perform the search
        if not perform_search(browser, search_query):
            thread_safe_log(f"Failed to perform search for {neighborhood}. Skipping.", 'error')
            return len(all_data)

        records_since_last_checkpoint = 0
        has_next = True
        page_num = 1
        duplicates_found = 0
        new_records_this_session = 0

        while has_next and page_num <= MAX_PAGES:
            thread_safe_log(f"Processing page {page_num} for {neighborhood}")

            # Wait for page to load after navigation
            if not wait_for_page_load(browser):
                thread_safe_log(f"Page {page_num} failed to load properly for {neighborhood}", 'error')
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
                    thread_safe_log(f"Retrying to find mainTable (attempt {retry + 1}/3) for {neighborhood}...",
                                    'warning')
                    time.sleep(2)

            if not table:
                thread_safe_log(f"Could not find mainTable after retries for {neighborhood}. Stopping.", 'error')
                break

            # Get all rows
            try:
                sell_row_data = table.find_elements(By.CLASS_NAME, "mainTable__row")
                thread_safe_log(f"Found {len(sell_row_data)} rows on page {page_num} for {neighborhood}")
            except Exception as e:
                thread_safe_log(f"Error finding rows for {neighborhood}: {e}", 'error')
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
                        thread_safe_log(f"No collapse arrow found for row {i} in {neighborhood}", 'warning')
                        continue

                    arrow = arrows[0]
                    browser.execute_script("arguments[0].click();", arrow)
                    time.sleep(0.3)

                    # Get expanded details
                    try:
                        inner_containers = table.find_elements(By.CLASS_NAME, "innerTablesContainer")
                        if inner_containers:
                            expanded_features = inner_containers[0].find_elements(By.CLASS_NAME, "innerTable__cell")

                            # Add the additional property details to base_row_data
                            base_row_data.update({
                                'שנת בנייה': safe_get(expanded_features, 3),
                                'מחיר למ"ר': safe_get(expanded_features, 4),
                                'קומות במבנה': safe_get(expanded_features, 5)
                            })

                            # Extract all transactions (original + additional ones)
                            transactions = extract_multiple_transactions(expanded_features, base_row_data)

                    except Exception as e:
                        thread_safe_log(f"Could not get expanded details for row {i} in {neighborhood}: {e}", 'warning')
                        # If expansion fails, just use the original row
                        transactions = [base_row_data]

                    # Collapse the row back
                    browser.execute_script("arguments[0].click();", arrow)
                    time.sleep(0.2)

                    # Process each transaction (including the original)
                    for transaction in transactions:
                        # Check for duplicates
                        record_hash = create_record_hash(transaction)
                        if record_hash in seen_hashes:
                            duplicates_found += 1
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
                    thread_safe_log(f"Error processing row {i} on page {page_num} for {neighborhood}: {e}", 'error')
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
                        thread_safe_log(f"Navigated to page {page_num + 1} for {neighborhood}")
                        time.sleep(3)  # Wait for navigation

                        page_num += 1
                        has_next = (page_num <= MAX_PAGES)
                    else:
                        thread_safe_log(f"Next button not available - reached end for {neighborhood}")
                else:
                    thread_safe_log(f"No next button found - reached end for {neighborhood}")
            except Exception as e:
                thread_safe_log(f"Error navigating to next page for {neighborhood}: {e}", 'error')
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

            thread_safe_log(f"Completed {neighborhood}:")
            thread_safe_log(f"  - Total unique records: {len(df_unique)}")
            thread_safe_log(f"  - New records this session: {new_records_this_session}")
            thread_safe_log(f"  - Duplicates skipped: {duplicates_found}")
            thread_safe_log(f"  - Records removed in final dedup: {len(df) - len(df_unique)}")
            thread_safe_log(f"  - Saved to: {csv_path}")

            return len(df_unique)
        else:
            thread_safe_log(f"No data was collected for {neighborhood}", 'warning')
            return 0

    except Exception as e:
        thread_safe_log(f"Error processing neighborhood {neighborhood}: {e}", 'error')
        return 0
    finally:
        if browser:
            try:
                browser.quit()
            except:
                pass


def main():
    """Main function to process all neighborhoods in parallel using ThreadPoolExecutor"""
    thread_safe_log(f"Starting multi-threaded scraper for {len(NEIGHBORHOODS)} neighborhoods")
    thread_safe_log(f"Neighborhoods: {', '.join(NEIGHBORHOODS)}")
    thread_safe_log(f"Max concurrent threads: {MAX_WORKERS}")

    total_records = 0
    results = {}

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all neighborhood processing tasks
        future_to_neighborhood = {
            executor.submit(process_neighborhood, neighborhood): neighborhood
            for neighborhood in NEIGHBORHOODS
        }

        # Collect results as they complete
        for future in as_completed(future_to_neighborhood):
            neighborhood = future_to_neighborhood[future]
            try:
                records = future.result()
                results[neighborhood] = records
                total_records += records
                thread_safe_log(f"Completed {neighborhood}: {records} records")
            except Exception as e:
                thread_safe_log(f"Failed to process {neighborhood}: {e}", 'error')
                results[neighborhood] = 0

    thread_safe_log(f"Multi-threaded scraping completed!")
    thread_safe_log(f"Results summary:")
    for neighborhood, count in results.items():
        thread_safe_log(f"  - {neighborhood}: {count} records")
    thread_safe_log(f"Total unique records collected: {total_records}")
    thread_safe_log(f"Data saved in: {DATA_DIR}")
    thread_safe_log(f"Checkpoints saved in: {CHECKPOINT_DIR}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        thread_safe_log("\nProcess interrupted by user. Progress has been saved in checkpoints.")
    except Exception as e:
        thread_safe_log(f"\nAn error occurred: {e}", 'error')
        thread_safe_log("Progress has been saved in checkpoints.")
