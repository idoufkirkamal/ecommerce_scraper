import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

# List of User-Agent Strings for Rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0',
    'Mozilla/5.0 (iPad; CPU OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
]


# Function to scrape a single page
def scrape_amazon_page(url):
    # Select a random User-Agent for making the request
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US, en;q=0.5'
    }

    # HTTP request
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Parse the product results
    results = soup.find_all('div', {'data-component-type': 's-search-result'})
    result_list = []

    for r in results:
        result_dict = {}

        # Extract the product title
        title_element = r.select_one('.a-size-medium.a-color-base.a-text-normal')
        if title_element:
            result_dict["title"] = title_element.text.strip()
        else:
            result_dict["title"] = "Title not available"

        # Extract the price
        price_element = r.select_one('.a-price .a-offscreen')
        if price_element:
            result_dict["price"] = price_element.text.strip()
        else:
            result_dict["price"] = "Price not available"

        # Extract the image URL
        image_element = r.select_one('.s-image')
        if image_element:
            result_dict["image"] = image_element.attrs['src']
        else:
            result_dict["image"] = "Image not available"

        # Append the product dictionary to the result list
        result_list.append(result_dict)

    return result_list


# Function to scrape multiple pages and save to CSV
def scrape_amazon(search_query, num_pages):
    base_url = f"https://www.amazon.com/s?k={search_query.replace(' ', '+')}&s=price-asc-rank&page="
    all_results = []

    for page in range(1, num_pages + 1):
        print(f"Scraping page {page}...")
        url = base_url + str(page)
        page_results = scrape_amazon_page(url)
        all_results.extend(page_results)

        # Add a random delay to avoid being detected as a bot
        random_delay = random.randint(2, 5)
        print(f"Waiting {random_delay} seconds to avoid being blocked...")
        time.sleep(random_delay)

    # Convert results to a Pandas DataFrame
    df = pd.DataFrame(all_results)

    # Create 'data' directory if it doesn't exist
    os.makedirs("data", exist_ok=True)

    # Save the DataFrame to a CSV file
    csv_file_path = "data/raw/amazon_results.csv"
    df.to_csv(csv_file_path, index=False, encoding='utf-8')
    print(f"Data saved to {csv_file_path}")

    return all_results


# Example script
if __name__ == "_main_":
    search_query = "computers"  # Search query
    num_pages = 5  # Number of pages to scrape

    # Scrape Amazon and save the results
    scraped_data = scrape_amazon(search_query, num_pages)

    # Print a preview of the results
    for product in scraped_data[:5]:  # Show only the first 5 products
        print(product)