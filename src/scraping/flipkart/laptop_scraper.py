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

def wait_random(min_time=3, max_time=7):
    """Wait for a random number of seconds to avoid detection."""
    delay = random.randint(min_time, max_time)
    print(f"Waiting {delay} seconds...")
    time.sleep(delay)

# Scrape a single Flipkart page
def scrape_flipkart_page(url):
    headers = DEFAULT_HEADERS
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the product container
        product_blocks = soup.find_all('div', class_='cPHDOP col-12-12')
        if not product_blocks:
            print("No product blocks found on this page.")
            return []

        scraped_items = []
        collection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for product in product_blocks:
            # Extract title
            title_element = product.find('div', class_='KzDlHZ')
            title = get_text_or_default(title_element)

            # Extract price
            price_element = product.find('div', class_='Nx9bqj _4b5DiR')
            price = get_text_or_default(price_element)

            # Extract rating
            rating_element = product.find('div', class_='XQDdHH')
            rating = get_text_or_default(rating_element)

            # Extract number of reviews
            reviews_element = product.find('span', class_='Wphh3N')
            reviews = get_text_or_default(reviews_element)

            # Extract image URL
            image_element = product.find('img', class_='DByuf4')
            image_url = image_element['src'] if image_element else "Image not available"

            # Extract product URL
            link_element = product.find('a', class_='CGtC98')
            product_url = f"https://www.flipkart.com{link_element['href']}" if link_element else "URL not available"

            scraped_items.append({
                "title": title,
                "price": price,
                "rating": rating,
                "reviews": reviews,
                "image_url": image_url,
                "product_url": product_url,
                "collection_date": collection_date
            })

        return scraped_items

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while scraping {url}: {e}")
        return []

# Update pagination handling
def scrape_flipkart(category_url, num_pages, output_dir="data/raw/flipkart"):
    aggregated_results = []
    for page in range(1, num_pages + 1):
        print(f"Scraping page {page}...")
        page_url = f"{category_url}&page={page}"
        page_results = scrape_flipkart_page(page_url)

        if not page_results:
            print("No more products found. Stopping.")
            break

        aggregated_results.extend(page_results)
        wait_random()

    # Save results to CSV
    if aggregated_results:
        df = pd.DataFrame(aggregated_results)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "flipkart_laptops.csv")
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Data saved to {output_path}")
    else:
        print("No data scraped.")

    return aggregated_results

# Main script
if __name__ == "__main__":
    category_url = "https://www.flipkart.com/laptops/pr?sid=6bo,b5g&q=laptop&otracker=categorytree"
    num_pages = 2  # Number of pages to scrape

    scraped_data = scrape_flipkart(category_url, num_pages)

    for product in scraped_data[:5]:
        print(product)
