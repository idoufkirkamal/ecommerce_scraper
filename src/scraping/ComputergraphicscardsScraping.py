import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime

# Constants
BASE_OUTPUT_DIR = r"C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw"
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0',
    'Mozilla/5.0 (iPad; CPU OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
]


def generate_headers():
    """Generate HTTP headers with randomized User-Agent."""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US, en;q=0.5'
    }


def wait_random_delay():
    """Wait for a random delay to avoid detection."""
    delay = random.randint(1, 5)
    print(f"Waiting {delay} seconds to avoid detection...")
    time.sleep(delay)


def scrape_amazon_page(url):
    headers = generate_headers()
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        results = soup.find_all('div', {'data-component-type': 's-search-result'})
        if not results:
            print("No results found on this page.")
            return []

        collection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        scraped_results = []

        for result in results:
            scraped_results.append({
                "title": result.select_one('.a-size-medium.a-color-base.a-text-normal').text.strip()
                if result.select_one('.a-size-medium.a-color-base.a-text-normal') else "Title not available",
                "price": result.select_one('.a-price .a-offscreen').text.strip()
                if result.select_one('.a-price .a-offscreen') else "Price not available",
                "promo": result.select_one(
                    '.a-row.a-size-base.a-color-secondary.s-align-children-center span.a-color-base.a-text-bold').text.strip()
                if result.select_one(
                    '.a-row.a-size-base.a-color-secondary.s-align-children-center span.a-color-base.a-text-bold') else "No promotion",
                "coupon": result.select_one('.s-coupon-clipped .a-color-base').text.strip()
                if result.select_one('.s-coupon-clipped .a-color-base') else "No coupon",
                "product_url": "https://www.amazon.com" + result.select_one('.a-link-normal.s-no-outline')['href']
                if result.select_one('.a-link-normal.s-no-outline') else "URL not available",
                "collection_date": collection_date
            })

        return scraped_results
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while scraping {url}: {e}")
        return []


def scrape_product_details(url):
    headers = generate_headers()
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return {
            "Graphics_Coprocessor": soup.select_one('.po-graphics_coprocessor .a-span9').text.strip()
            if soup.select_one('.po-graphics_coprocessor .a-span9') else "Graphics Coprocessor not available",
            "Brand": soup.select_one('.po-brand .a-span9').text.strip()
            if soup.select_one('.po-brand .a-span9') else "Brand not available",
            "Graphics_Ram_Size": soup.select_one('.po-graphics_ram\\.size .a-span9').text.strip()
            if soup.select_one('.po-graphics_ram\\.size .a-span9') else "Graphics Ram Size not available",
            "GPU_Clock_Speed": soup.select_one('.po-gpu_clock_speed .a-span9').text.strip()
            if soup.select_one('.po-gpu_clock_speed .a-span9') else "GPU Clock Speed not available",
            "Video_Output_Interface": soup.select_one('.po-video_output_interface .a-span9').text.strip()
            if soup.select_one('.po-video_output_interface .a-span9') else "Video Output Interface not available",
        }
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while scraping {url}: {e}")
        return {}


def scrape_amazon(search_query, num_pages):
    base_url = f"https://www.amazon.com/s?k={search_query.replace(' ', '+')}&page="
    all_results = []

    for page in range(1, num_pages + 1):
        print(f"Scraping page {page}...")
        page_results = scrape_amazon_page(base_url + str(page))
        if not page_results:
            print("No more pages or an error occurred. Stopping.")
            break

        all_results.extend(page_results)
        wait_random_delay()

    detailed_results = []
    for result in all_results:
        if result["product_url"] != "URL not available":
            print(f"Scraping details for {result['title']}...")
            details = scrape_product_details(result["product_url"])
            result.update(details)
            wait_random_delay()
        detailed_results.append(result)

    df = pd.DataFrame(detailed_results).fillna("Data not available")
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
    output_file_path = os.path.join(BASE_OUTPUT_DIR, "amazon_computer_graphics_cards.csv")
    df.to_csv(output_file_path, index=False, encoding='utf-8')
    print(f"Data saved to {output_file_path}")
    return detailed_results


if __name__ == "__main__":
    search_query = "Computer graphics cards"
    num_pages = 10
    scraped_data = scrape_amazon(search_query, num_pages)
    for product in scraped_data[:5]:
        print(product)