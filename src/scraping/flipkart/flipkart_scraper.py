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

def extract_specifications(soup):
    """Extracts specifications from the product details section."""
    specifications = {}
    spec_sections = soup.find_all('div', class_='GNDEQ-')
    
    for section in spec_sections:
        section_title = section.find('div', class_='_4BJ2V+')
        if not section_title:
            continue
        section_title = section_title.text.strip()

        spec_rows = section.find_all('tr', class_='WJdYP6 row')
        for row in spec_rows:
            key_element = row.find('td', class_='+fFi1w col col-3-12')
            value_element = row.find('td', class_='Izz52n col col-9-12')

            if key_element and value_element:
                key = key_element.text.strip()
                value = value_element.text.strip()
                specifications[key] = value

    return specifications

def scrape_flipkart_product(product_url):
    """Scrapes detailed information (including ratings and reviews) for a single product."""
    headers = DEFAULT_HEADERS
    try:
        response = requests.get(product_url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        specifications = extract_specifications(soup)

        rating_element = soup.find('div', class_='_3LWZlK')
        rating = get_text_or_default(rating_element)

        reviews_element = soup.find('span', class_='_2_R_DZ')
        reviews_text = get_text_or_default(reviews_element)

        reviews_match = re.search(r'\d+', reviews_text.replace(',', ''))
        reviews = reviews_match.group() if reviews_match else "Data not available"

        return {
            "rating": rating,
            "reviews": reviews,
            **specifications,
        }

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while scraping product {product_url}: {e}")
        return {"rating": "Data not available", "reviews": "Data not available"}

def scrape_flipkart_page(url, category_name):
    headers = DEFAULT_HEADERS
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        product_blocks = soup.find_all('div', class_='cPHDOP col-12-12')
        if not product_blocks:
            print("No product blocks found on this page.")
            return []

        scraped_items = []
        collection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for product in product_blocks:
            if category_name == "graphics_cards":
                title_element = product.find('a', class_='wjcEIp')
                price_element = product.find('div', class_='Nx9bqj')
                rating_element = product.find('div', class_='XQDdHH')
                reviews_element = product.find('span', class_='Wphh3N')
                image_element = product.find('img', class_='DByuf4')
                link_element = product.find('a', class_='VJA3rP')
            elif category_name == "laptops":
                title_element = product.find('div', class_='KzDlHZ')
                price_element = product.find('div', class_='Nx9bqj _4b5DiR')
                rating_element = product.find('div', class_='XQDdHH')
                reviews_element = product.find('span', class_='Wphh3N')
                image_element = product.find('img', class_='DByuf4')
                link_element = product.find('a', class_='CGtC98')
            elif category_name == "monitors":
                title_element = product.find('div', class_='KzDlHZ')
                price_element = product.find('div', class_='Nx9bqj _4b5DiR')
                rating_element = product.find('div', class_='XQDdHH')
                reviews_element = product.find('span', class_='Wphh3N')
                image_element = product.find('img', class_='DByuf4')
                link_element = product.find('a', class_='CGtC98')
            elif category_name == "smart_watches":
                title_element = product.find('a', class_='WKTcLC')
                price_element = product.find('div', class_='Nx9bqj')
                rating_element = product.find('div', class_='XQDdHH')
                reviews_element = product.find('span', class_='Wphh3N')
                image_element = product.find('img', class_='_53J4C-')
                link_element = product.find('a', class_='rPDeLR')

            title = get_text_or_default(title_element)
            price = get_text_or_default(price_element)
            rating = get_text_or_default(rating_element)
            reviews = get_text_or_default(reviews_element)
            image_url = image_element['src'] if image_element else "Image not available"
            product_url = f"https://www.flipkart.com{link_element['href']}" if link_element else "URL not available"

            specifications = scrape_flipkart_product(product_url) if product_url != "URL not available" else {}

            scraped_items.append({
                "title": title,
                "price": price,
                "rating": rating,
                "reviews": reviews,
                "image_url": image_url,
                "product_url": product_url,
                "collection_date": collection_date,
                **specifications,
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

def scrape_flipkart(category_url, num_pages, category_name, output_dir="data/raw/flipkart"):
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
        page_results = scrape_flipkart_page(page_url, category_name)

        if not page_results:
            print("No more products found. Stopping.")
            break

        aggregated_results.extend(page_results)
        wait_random()

    if aggregated_results:
        today = datetime.today()
        formatted_date = today.strftime("%Y_%m_%d")

        # Determine the next scrape number globally
        scrape_number = get_next_scrape_number(output_dir, category_name)

        filename = f"{category_name}_{formatted_date}_scrape{scrape_number}.csv"

        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(output_dir, filename)

        df = pd.DataFrame(aggregated_results)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"Data saved to {output_path}")
    else:
        print("No data scraped.")

    return aggregated_results

# Main script
if __name__ == "__main__":
    categories = {
        "graphics_cards": {
            "url": "https://www.flipkart.com/gaming-components/graphic-cards/pr?sid=4rr,tin,6zn&q=graphics+card&otracker=categorytree",
            "num_pages": 18
        },
        "laptops": {
            "url": "https://www.flipkart.com/laptops/pr?sid=6bo,b5g&q=laptop&otracker=categorytree",
            "num_pages": 18
        },
        "monitors": {
            "url": "https://www.flipkart.com/search?q=monitor&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off",
            "num_pages": 18
        },
        "smart_watches": {
            "url": "https://www.flipkart.com/wearable-smart-devices/smart-watches/pr?sid=ajy,buh&q=smart+watches&otracker=categorytree",
            "num_pages": 18
        }
    }

    for category_name, config in categories.items():
        print(f"Scraping {category_name}...")
        scrape_flipkart(config["url"], config["num_pages"], category_name)