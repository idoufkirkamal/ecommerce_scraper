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


# Scrape a single Walmart page
def scrape_walmart_page(url):
    headers = DEFAULT_HEADERS
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the product container
        product_block = soup.find('div', {'data-testid': 'item-stack'})
        if not product_block:
            print("No product block found on this page.")
            return []

        # Extract all products
        products = product_block.find_all('div', {'data-item-id': True})
        if not products:
            print("No products found in the block.")
            return []

        scraped_items = []
        collection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for product in products:
            # Extract title
            title = product.find('span', {'data-automation-id': 'product-title'})
            title_text = get_text_or_default(title)

            # Extract price
            price_container = product.find('div', {'data-automation-id': 'product-price'})
            price_text = None
            if price_container:
                price_text = price_container.find('span', {'class': 'f2'})
                price_text = get_text_or_default(price_text)

            # Extract rating
            rating = product.find('span', {'data-testid': 'product-ratings'})
            rating_value = rating['data-value'] if rating else None

            # Extract number of reviews
            reviews = product.find('span', {'data-testid': 'product-reviews'})
            reviews_value = reviews['data-value'] if reviews else None

            # Extract image URL
            image = product.find('img', {'data-testid': 'productTileImage'})
            image_url = image['src'] if image else "Image not available"

            # Extract product URL
            product_link = product.find('a', {'data-automation-id': 'product-title'})
            product_url = "https://www.walmart.com" + product_link['href'] if product_link else "URL not available"

            scraped_items.append({
                "title": title_text,
                "price": price_text,
                "rating": rating_value,
                "reviews": reviews_value,
                "image_url": image_url,
                "product_url": product_url,
                "collection_date": collection_date
            })

        return scraped_items

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while scraping {url}: {e}")
        return []

# Update pagination handling
def scrape_walmart(category_url, num_pages, output_dir="data/raw/walmart"):
    aggregated_results = []
    for page in range(1, num_pages + 1):
        print(f"Scraping page {page}...")
        page_url = f"{category_url}&page={page}"
        page_results = scrape_walmart_page(page_url)

        if not page_results:
            print("No more products found. Stopping.")
            break

        aggregated_results.extend(page_results)
        wait_random()

    # Save results to CSV
    if aggregated_results:
        df = pd.DataFrame(aggregated_results)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "walmart_laptops.csv")
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Data saved to {output_path}")
    else:
        print("No data scraped.")

    return aggregated_results


# Main script
if __name__ == "__main__":
    category_url = "https://www.walmart.com/browse/electronics/all-laptop-computers/3944_1089430_3951_132960?povid=ETS_browse_Laptops_WEB"
    num_pages = 5  # Number of pages to scrape

    scraped_data = scrape_walmart(category_url, num_pages)

    for product in scraped_data[:5]:
        print(product)