import os
import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

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


# Helper Functions
def get_text_or_default(element, default="Data not available"):
    """Extracts text from BeautifulSoup element or returns a default value."""
    return element.text.strip() if element else default


def wait_random(min_time=5, max_time=15):
    """Wait for a random number of seconds to avoid detection."""
    delay = random.randint(min_time, max_time)
    print(f"Waiting {delay} seconds...")
    time.sleep(delay)


# Scrape a single Amazon page
def scrape_amazon_page(url):
    headers = DEFAULT_HEADERS
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        results = soup.find_all('div', {'data-component-type': 's-search-result'})
        if not results:
            print("No results found on this page.")
            return []

        collection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        scraped_items = []

        for item in results:
            scraped_items.append({
                "title": get_text_or_default(item.select_one('.a-size-medium.a-color-base.a-text-normal')),
                "price": get_text_or_default(item.select_one('.a-price .a-offscreen')),
                "promo": get_text_or_default(item.select_one(
                    '.a-row.a-size-base.a-color-secondary.s-align-children-center span.a-color-base.a-text-bold'),
                    "No promotion"),
                "coupon": get_text_or_default(item.select_one('.s-coupon-clipped .a-color-base'), "No coupon"),
                "product_url": "https://www.amazon.com" + item.select_one('.a-link-normal.s-no-outline')[
                    'href'] if item.select_one('.a-link-normal.s-no-outline') else "URL not available",
                "collection_date": collection_date
            })

        return scraped_items

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while scraping {url}: {e}")
        return []


# Scrape product details page
def scrape_product_details(url):
    headers = DEFAULT_HEADERS
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        product_details = {
            "brand": get_text_or_default(soup.select_one('.po-brand .a-span9')),
            "screen_size": get_text_or_default(soup.select_one('.po-display\\.size .a-span9')),
            "color": get_text_or_default(soup.select_one('.po-resolution .a-span9')),
            "hard_disk_size": get_text_or_default(soup.select_one('.po-aspect_ratio .a-span9')),
            "cpu_model": get_text_or_default(soup.select_one('.po-screen_surface_description .a-span9')),

        }


        return product_details

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while scraping {url}: {e}")
        return {}


# Scrape multiple Amazon pages
def scrape_amazon(search_query, num_pages, output_dir=r"C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\amazon"):
    base_url = f"https://www.amazon.com/s?k={search_query.replace(' ', '+')}&page="
    aggregated_results = []

    for page in range(1, num_pages + 1):
        print(f"Scraping page {page}...")
        page_url = f"{base_url}{page}"
        page_results = scrape_amazon_page(page_url)

        if not page_results:
            print("No more pages or an error occurred. Stopping.")
            break

        aggregated_results.extend(page_results)
        wait_random()

    detailed_results = []
    for product in aggregated_results:
        if product["product_url"] != "URL not available":
            print(f"Scraping details for {product['title']}...")
            details = scrape_product_details(product["product_url"])
            product.update(details)
            wait_random()
        detailed_results.append(product)

    # Save results to CSV
    df = pd.DataFrame(detailed_results)
    df.fillna("Data not available", inplace=True)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "monitor_amazon2.csv")
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Data saved to {output_path}")

    return detailed_results
# Main script
if __name__ == "__main__":
    search_query = "monitor"
    num_pages = 15

    scraped_data = scrape_amazon(search_query, num_pages)

    for product in scraped_data[:5]:
        print(product)