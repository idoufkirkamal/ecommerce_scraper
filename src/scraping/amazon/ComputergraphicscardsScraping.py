import time
import random
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Constants
BASE_OUTPUT_DIR = r"C:\Users\AdMin\Desktop\ecommerce_scraper\src\scraping\amazon\data"
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
    """Generate randomized HTTP headers."""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US, en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }


def wait_random_delay():
    """Random delay with fractional seconds."""
    delay = random.uniform(0.5, 3.5)
    time.sleep(delay)


def create_session_with_retries():
    """Create requests session with retry strategy."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def extract_text(soup, selectors, is_multiple=False):
    """Extract text using multiple selector fallbacks."""
    for selector in selectors:
        try:
            elements = soup.select(selector) if is_multiple else soup.select_one(selector)
            if elements:
                if is_multiple:
                    return [elem.get_text(strip=True) for elem in elements]
                return elements.get_text(strip=True)
        except Exception as e:
            continue
    return None


def search_features(features, keywords):
    """Search feature bullets for relevant information."""
    if not features:
        return None

    for feature in features:
        lower_feature = feature.lower()
        for keyword in keywords:
            if keyword.lower() in lower_feature:
                numbers = re.findall(r'\d+\.?\d*\s*[GM]?B|\d+\.?\d*\s*GHz', feature)
                if numbers:
                    return f"{' '.join(numbers)}".strip()
                return feature
    return None


def scrape_amazon_page(url, session):
    """Scrape product listings from Amazon search page."""
    try:
        response = session.get(url, headers=generate_headers(), timeout=15)
        response.raise_for_status()

        if "api-services-support@amazon.com" in response.text:
            print("CAPTCHA detected. Skipping page.")
            return []

        soup = BeautifulSoup(response.text, 'lxml')
        results = soup.find_all('div', {'data-component-type': 's-search-result'})

        page_data = []
        collection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for result in results:
            # Title with multiple selector fallbacks
            title = extract_text(result, [
                'h2.a-size-mini a.a-link-normal',
                'h2.a-size-mini a.a-text-normal',
                '.a-size-medium.a-color-base.a-text-normal',
                '.a-size-base-plus.a-color-base.a-text-normal'
            ]) or "N/A"

            # Price extraction with multiple fallbacks
            price = extract_text(result, [
                '.a-price .a-offscreen',
                '.a-price-fraction',
                '.a-color-price',
                '.a-text-price'
            ]) or "N/A"

            # Product URL
            link_tag = result.select_one('a.a-link-normal.s-no-outline') or result.select_one('h2 a.a-link-normal')
            product_url = (
                "https://www.amazon.com" + link_tag['href']
                if link_tag and link_tag.has_attr('href')
                else "N/A"
            )

            page_data.append({
                'title': title,
                'price': re.sub(r'\$|\s+', '', str(price)) if price != "N/A" else price,
                'product_url': product_url,
                'collection_date': collection_date
            })

        return page_data

    except Exception as e:
        print(f"Page scrape error: {str(e)[:100]}")
        return []


def scrape_product_details(url, session):
    """Scrape detailed product information with multiple fallback methods."""
    try:
        response = session.get(url, headers=generate_headers(), timeout=20)
        response.raise_for_status()

        if "api-services-support@amazon.com" in response.text:
            print("CAPTCHA in product page. Skipping.")
            return {}

        soup = BeautifulSoup(response.text, 'lxml')
        details = {}

        # Method 1: Technical Specifications Table
        tech_specs = {}
        for table in soup.find_all('table', {'id': 'productDetails_techSpec_section_1'}):
            for row in table.find_all('tr'):
                cells = row.find_all(['th', 'td'])
                if len(cells) == 2:
                    key = cells[0].get_text(strip=True).replace('\u200e', '').lower()
                    value = cells[1].get_text(strip=True).replace('\u200e', '')
                    tech_specs[key] = value

        # Method 2: Feature Bullets
        feature_bullets = []
        bullet_section = soup.find('div', {'id': 'feature-bullets'})
        if bullet_section:
            feature_bullets = [
                li.get_text(strip=True)
                for li in bullet_section.select('ul.a-unordered-list.a-vertical.a-spacing-mini li span.a-list-item')
            ]

        # Method 3: Additional Details Section
        additional_details = {}
        detail_sections = soup.find_all('div', {'class': 'a-section a-spacing-medium a-spacing-top-small'})
        for section in detail_sections:
            rows = section.find_all('div', {'class': 'a-row'})
            for row in rows:
                parts = row.find_all(['span', 'div'])
                if len(parts) >= 2:
                    key = parts[0].get_text(strip=True).lower()
                    value = parts[1].get_text(strip=True)
                    additional_details[key] = value

        # Combined data extraction
        brand = (
                        tech_specs.get('brand', '')
                        or additional_details.get('brand', '')
                        or extract_text(soup, ['#bylineInfo', '#brand', '.a-link-normal#bylineUrl'])
                ) or "N/A"

        details.update({
            'brand': brand[:100] if brand else "N/A",
            'graphics_coprocessor': (
                    tech_specs.get('graphics coprocessor')
                    or search_features(feature_bullets, ['coprocessor', 'gpu', 'graphics processor'])
                    or "N/A"
            ),
            'graphics_ram_size': (
                    tech_specs.get('graphics ram size')
                    or search_features(feature_bullets, ['vram', 'memory size', 'ram size'])
                    or "N/A"
            ),
            'gpu_clock_speed': (
                    tech_specs.get('gpu clock speed')
                    or search_features(feature_bullets, ['clock speed', 'boost clock', 'gpu speed'])
                    or "N/A"
            ),
            'video_output_interface': (
                    tech_specs.get('video output interface')
                    or search_features(feature_bullets, ['hdmi', 'displayport', 'output ports'])
                    or additional_details.get('video interface', 'N/A')
            )
        })

        return details

    except Exception as e:
        print(f"Product detail error: {str(e)[:100]}")
        return {}


def scrape_amazon(search_query, num_pages):
    """Main scraping function with parallel processing."""
    base_url = f"https://www.amazon.com/s?k={search_query.replace(' ', '+')}&page="
    all_results = []

    with create_session_with_retries() as session:
        # Scrape search pages
        for page in range(1, num_pages + 1):
            print(f"Scraping page {page}/{num_pages}")
            page_url = f"{base_url}{page}"
            page_data = scrape_amazon_page(page_url, session)

            if not page_data:
                print("No more results or CAPTCHA block detected. Stopping early.")
                break

            all_results.extend(page_data)
            wait_random_delay()

        # Parallel product detail scraping
        print(f"Scraping details for {len(all_results)} products...")
        url_map = {prod['product_url']: idx for idx, prod in enumerate(all_results) if prod['product_url'] != "N/A"}

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {}
            for url in url_map.keys():
                futures[executor.submit(scrape_product_details, url, session)] = url

            for future in as_completed(futures):
                url = futures[future]
                try:
                    details = future.result()
                    if details:
                        all_results[url_map[url]].update(details)
                except Exception as e:
                    print(f"Detail error for {url[:50]}: {str(e)[:50]}")

    # Create DataFrame and save
    df = pd.DataFrame(all_results).replace({'N/A': None, '': None})
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(BASE_OUTPUT_DIR, "amazon_gpu_products.csv")
    df.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"Successfully saved {len(df)} products to {output_path}")
    return df


if __name__ == "__main__":
    scraped_data = scrape_amazon("Computer graphics cards", 15)
    print("\nSample results:")
    print(scraped_data[['title', 'price', 'brand']].head().to_string(index=False))