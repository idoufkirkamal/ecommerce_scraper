import os
import time
import random
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Constants
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
]
DEFAULT_HEADERS = {
    'User-Agent': random.choice(USER_AGENTS),
    'Accept-Language': 'en-US, en;q=0.5'
}
CSV_COLUMNS = [
    "brand", "model_name", "screen_size", "operating_system", "memory_storage_capacity",
    "special_feature", "battery_capacity", "connectivity_technology",
    "wireless_communication_standard", "battery_cell_composition", "gps", "shape"
]
DEFAULT_CSV_PATH = r"C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\amazon/amazon_smart_watch2.csv"
COLLECTION_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


# Helper Functions
def get_text_or_default(element, default="Data not available"):
    """Extract text from an element or return a default value."""
    return element.text.strip() if element else default


def wait_random(min_time=5, max_time=15):
    """Wait for a random amount of time to avoid detection."""
    delay = random.uniform(min_time, max_time)
    print(f"Waiting for {delay:.2f} seconds...")
    time.sleep(delay)


def save_results_to_csv(data, output_path=DEFAULT_CSV_PATH):
    """Serialize results into a CSV file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df = pd.DataFrame(data)
    df.fillna("Data not available", inplace=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Data saved to {output_path}")


# Scrape an Amazon page
def scrape_amazon_page(url, session):
    try:
        # Rotate headers for each request
        DEFAULT_HEADERS['User-Agent'] = random.choice(USER_AGENTS)
        response = session.get(url, headers=DEFAULT_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        results = soup.find_all('div', {'data-component-type': 's-search-result'})
        if not results:
            print("No results found on this page.")
            return []
        collection_date = datetime.now().strftime(COLLECTION_DATE_FORMAT)
        return [
            {
                "title": get_text_or_default(item.select_one('.a-size-medium.a-color-base.a-text-normal')),
                "price": get_text_or_default(item.select_one('.a-price .a-offscreen')),
                "promo": get_text_or_default(
                    item.select_one(
                        '.a-row.a-size-base.a-color-secondary.s-align-children-center span.a-color-base.a-text-bold'),
                    "No promotion"
                ),
                "coupon": get_text_or_default(
                    item.select_one('.s-coupon-clipped .a-color-base'), "No coupon"
                ),
                "product_url": "https://www.amazon.com" + item.select_one('.a-link-normal.s-no-outline')['href']
                if item.select_one('.a-link-normal.s-no-outline') else "URL not available",
                "collection_date": collection_date,
            }
            for item in results
        ]
    except requests.exceptions.RequestException as e:
        print(f"Error scraping {url}: {e}")
        return []


# Scrape product details
def scrape_product_details(url, session):
    try:
        # Rotate headers for each request
        DEFAULT_HEADERS['User-Agent'] = random.choice(USER_AGENTS)
        response = session.get(url, headers=DEFAULT_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return {
            "brand": get_text_or_default(soup.select_one('.po-brand .a-span9')),
            "model_name": get_text_or_default(soup.select_one('.po-model_name .a-span9')),
            "screen_size": get_text_or_default(soup.select_one('.po-display\\.size .a-span9')),
            "operating_system": get_text_or_default(soup.select_one('.po-operating_system .a-span9')),
            "memory_storage_capacity": get_text_or_default(soup.select_one('.po-memory_storage_capacity .a-span9')),
            "battery_capacity": get_text_or_default(soup.select_one('.po-battery\\.capacity .a-span9')),
            "battery_cell_composition": get_text_or_default(soup.select_one('.po-battery\\.cell_composition .a-span9')),
            "shape": get_text_or_default(soup.select_one('.po-item_shape .a-span9')),
        }
    except requests.exceptions.RequestException as e:
        print(f"Error scraping details of {url}: {e}")
        return {}


# Scrape multiple Amazon pages
def scrape_amazon(search_query, num_pages, output_path=DEFAULT_CSV_PATH):
    base_url = f"https://www.amazon.com/s?k={search_query.replace(' ', '+')}&page="
    aggregated_results = []
    session = requests.Session()  # Use session for better performance and cookie persistence

    for page in range(1, num_pages + 1):
        print(f"Scraping page {page}...")
        page_url = f"{base_url}{page}"
        page_results = scrape_amazon_page(page_url, session)
        if not page_results:
            print("No more pages available. Stopping.")
            break
        aggregated_results.extend(page_results)
        wait_random()

    detailed_results = []
    for product in aggregated_results:
        if product["product_url"] != "URL not available":
            print(f"Scraping details for {product['title']}...")
            details = scrape_product_details(product["product_url"], session)
            product.update(details)
            wait_random()
        detailed_results.append(product)

    save_results_to_csv(detailed_results, output_path)
    return detailed_results


# Main Script
if __name__ == "__main__":
    search_query = "smart watch"
    num_pages = 15
    scraped_data = scrape_amazon(search_query, num_pages)
    for product in scraped_data[:5]:
        print(product)