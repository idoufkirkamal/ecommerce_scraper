import os
import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re

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

# Updated Helper Function for extracting specifications
def extract_specifications(soup):
    """Extracts specifications from the product details section."""
    specifications = {}
    spec_sections = soup.find_all('div', class_='GNDEQ-')
    
    for section in spec_sections:
        # Get the section title (e.g., "General", "Processor And Memory Features")
        section_title = section.find('div', class_='_4BJ2V+')
        if not section_title:
            continue
        section_title = section_title.text.strip()

        # Extract the specifications in the table under this section
        spec_rows = section.find_all('tr', class_='WJdYP6 row')
        for row in spec_rows:
            key_element = row.find('td', class_='+fFi1w col col-3-12')
            value_element = row.find('td', class_='Izz52n col col-9-12')

            if key_element and value_element:
                key = key_element.text.strip()
                value = value_element.text.strip()
                specifications[key] = value

    return specifications

# Updated function to scrape details from a single product
def scrape_flipkart_product(product_url):
    """Scrapes detailed specifications for a single product."""
    headers = DEFAULT_HEADERS
    try:
        response = requests.get(product_url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract specifications
        specifications = extract_specifications(soup)
        return specifications

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while scraping product {product_url}: {e}")
        return {}

# Update scrape_flipkart_page to include detailed specifications
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

            # Extract product specifications
            specifications = scrape_flipkart_product(product_url) if product_url != "URL not available" else {}

            scraped_items.append({
                "title": title,
                "price": price,
                "rating": rating,
                "reviews": reviews,
                "image_url": image_url,
                "product_url": product_url,
                "collection_date": collection_date,
                **specifications,  # Merge specifications into the product data
            })

        return scraped_items

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while scraping {url}: {e}")
        return []

def get_next_scrape_number(output_dir, category_name):
    """Determine the next scrape number globally, regardless of the date."""
    scrape_number = 1
    for filename in os.listdir(output_dir):
        if filename.startswith(f"{category_name}_") and filename.endswith(".csv"):
            try:
                # Extract the scrape number from the filename
                current_number = int(filename.split('_scrape')[-1].split('.')[0])
                if current_number >= scrape_number:
                    scrape_number = current_number + 1
            except ValueError:
                continue
    return scrape_number

def scrape_flipkart(category_url, num_pages, category_name="laptops", output_dir="data/raw/flipkart"):
    """
    Scrapes Flipkart products for a given category and saves the output with a dynamic filename.
    Args:
        category_url (str): URL of the category to scrape.
        num_pages (int): Number of pages to scrape.
        category_name (str): Name of the category being scraped.
        output_dir (str): Directory to save the scraped data.
    """
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
        # Get today's date in the desired format
        today = datetime.today()
        formatted_date = today.strftime("%Y_%m_%d")

        # Determine the next scrape number globally
        scrape_number = get_next_scrape_number(output_dir, category_name)

        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Generate the filename
        filename = f"{category_name}_{formatted_date}_scrape{scrape_number}.csv"
        output_path = os.path.join(output_dir, filename)

        # Save the data to a CSV file
        df = pd.DataFrame(aggregated_results)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"Data saved to {output_path}")
    else:
        print("No data scraped.")

    return aggregated_results


# Main script
if __name__ == "__main__":
    # Define the category URL and scrape parameters
    category_url = "https://www.flipkart.com/laptops/pr?sid=6bo,b5g&q=laptop&otracker=categorytree"
    num_pages = 18  # Number of pages to scrape
    category_name = "laptops"  # Define the category name

    # Perform the scraping
    scraped_data = scrape_flipkart(category_url, num_pages, category_name=category_name)