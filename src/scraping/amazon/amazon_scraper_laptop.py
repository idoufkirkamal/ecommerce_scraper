import os
import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Constants
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/113.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
]


def get_random_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US, en;q=0.5',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.amazon.com/',
        'DNT': '1'
    }


# Helper Functions
def get_text_or_default(element, default="Data not available"):
    return element.text.strip() if element else default


def wait_random(min_time=2, max_time=8):
    delay = random.uniform(min_time, max_time)
    print(f"Waiting {delay:.2f} seconds...")
    time.sleep(delay)


# Scrape a single Amazon page with retries
def scrape_amazon_page(url):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=get_random_headers(), timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Check for CAPTCHA
            if soup.find('form', {'action': '/errors/validateCaptcha'}):
                raise requests.exceptions.RequestException("CAPTCHA encountered")

            results = soup.find_all('div', {'data-component-type': 's-search-result'})
            if not results:
                return []

            collection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return [{
                "title": get_text_or_default(item.select_one('.a-size-medium.a-color-base.a-text-normal')),
                "price": get_text_or_default(item.select_one('.a-price .a-offscreen')),
                "promo": get_text_or_default(item.select_one(
                    '.a-row.a-size-base.a-color-secondary.s-align-children-center span.a-color-base.a-text-bold'),
                    "No promotion"),
                "coupon": get_text_or_default(item.select_one('.s-coupon-clipped .a-color-base'), "No coupon"),
                "product_url": "https://www.amazon.com" + link['href'] if (
                    link := item.select_one('.a-link-normal.s-no-outline')) else "URL not available",
                "collection_date": collection_date
            } for item in results]

        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
            if attempt < max_retries - 1:
                backoff = (attempt + 1) * 5
                print(f"Retrying in {backoff} seconds...")
                time.sleep(backoff)
            else:
                print(f"Max retries exceeded for {url}")
                return []
    return []


# Scrape product details with retries
def scrape_product_details(url):
    max_retries = 2
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=get_random_headers(), timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            return {
                "brand": get_text_or_default(soup.select_one('.po-brand .a-span9')),
                "model_name": get_text_or_default(soup.select_one('.po-model_name .a-span9')),
                "screen_size": get_text_or_default(soup.select_one('.po-display\\.size .a-span9')),
                "color": get_text_or_default(soup.select_one('.po-color .a-span9')),
                "hard_disk_size": get_text_or_default(soup.select_one('.po-hard_disk\\.size .a-span9')),
                "cpu_model": get_text_or_default(soup.select_one('.po-cpu_model\\.family .a-span9')),
                "ram_memory": get_text_or_default(soup.select_one('.po-ram_memory\\.installed_size .a-span9')),
                "operating_system": get_text_or_default(soup.select_one('.po-operating_system .a-span9')),
                "special_feature": get_text_or_default(soup.select_one('.po-special_feature .a-span9')),
            }

        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 3)
    return {}


# Parallel product detail scraping
def process_product_details(product):
    if product["product_url"].startswith("https://"):
        details = scrape_product_details(product["product_url"])
        product.update(details)
        wait_random(2, 5)  # Shorter delay between detail requests
    return product


# Improved main scraping function
def scrape_amazon(search_query, num_pages, output_dir=r"C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\amazon"):
    base_url = f"https://www.amazon.com/s?k={search_query.replace(' ', '+')}&page="
    aggregated_results = []

    for page in range(1, num_pages + 1):
        page_url = f"{base_url}{page}"
        print(f"Scraping {page_url}")

        page_results = scrape_amazon_page(page_url)
        if not page_results:
            print(f"Stopped at page {page}")
            break

        aggregated_results.extend(page_results)
        if page < num_pages:
            wait_random(3, 6)  # Longer delay between page requests

    # Parallel processing of product details
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_product_details, p) for p in aggregated_results]
        detailed_results = [future.result() for future in as_completed(futures)]

    # Save results
    df = pd.DataFrame(detailed_results)
    df.fillna("Data not available", inplace=True)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "laptop_day2_2024.csv")
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Data saved to {output_path}")
    return detailed_results


# Main execution
if __name__ == "__main__":
    scraped_data = scrape_amazon("laptop Mac", num_pages=15)
    for product in scraped_data[:5]:
        print(product)