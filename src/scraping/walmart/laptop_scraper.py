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

        # Find the specific block containing the products
        product_block = soup.find('div', {'class': 'flex flex-wrap w-100 flex-grow-0 flex-shrink-0 ph2 pr0-xl pl4-xl mt0-xl'})
        if not product_block:
            print("No product block found on this page.")
            return []

        # Find all product items within the block
        products = product_block.find_all('div', {'data-item-id': True})
        if not products:
            print("No products found in the block.")
            return []

        collection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        scraped_items = []

        for product in products:
            # Extract title
            title = product.find('span', {'class': 'w_iUH7'})  # Updated selector
            if not title:
                title = product.find('span', {'data-automation-id': 'product-title'})  # Fallback selector

            # Extract price
            price = product.find('div', {'data-automation-id': 'product-price'})
            if price:
                price_text = price.find('span', {'class': 'f2'})  # Current price
                if not price_text:
                    price_text = price.find('span', {'class': 'w_iUH7'})  # Fallback selector

            # Extract rating
            rating = product.find('span', {'data-testid': 'product-ratings'})
            if rating:
                rating_value = rating.get('data-value', None)
            else:
                rating_value = None

            # Extract number of reviews
            review = product.find('span', {'data-testid': 'product-reviews'})
            if review:
                review_value = review.get('data-value', None)
            else:
                review_value = None

            # Extract image URL
            image = product.find('img', {'data-testid': 'productTileImage'})
            if image:
                image_url = image.get('src', None)
            else:
                image_url = None

            # Extract product URL
            product_link = product.find('a', {'data-automation-id': 'product-title'})
            if product_link:
                product_url = "https://www.walmart.com" + product_link['href']
            else:
                product_url = None

            scraped_items.append({
                "title": get_text_or_default(title),
                "price": get_text_or_default(price_text),
                "rating": rating_value,
                "reviews": review_value,
                "image_url": image_url if image_url else "Image not available",
                "product_url": product_url if product_url else "URL not available",
                "collection_date": collection_date
            })

        return scraped_items

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while scraping {url}: {e}")
        return []


# Scrape multiple Walmart pages
def scrape_walmart(category_url, num_pages, output_dir="data/raw/walmart"):
    base_url = category_url
    aggregated_results = []

    for page in range(1, num_pages + 1):
        print(f"Scraping page {page}...")
        page_url = f"{base_url}&page={page}"
        page_results = scrape_walmart_page(page_url)

        if not page_results:
            print("No more pages or an error occurred. Stopping.")
            break

        aggregated_results.extend(page_results)
        wait_random()

    # Save results to CSV
    df = pd.DataFrame(aggregated_results)
    df.fillna("Data not available", inplace=True)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "walmart_laptops.csv")
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Data saved to {output_path}")

    return aggregated_results


# Main script
if __name__ == "__main__":
    category_url = "https://www.walmart.com/browse/electronics/all-laptop-computers/3944_1089430_3951_132960?povid=ETS_browse_Laptops_WEB"
    num_pages = 5  # Number of pages to scrape

    scraped_data = scrape_walmart(category_url, num_pages)

    for product in scraped_data[:5]:
        print(product)