import os
import csv
import time
import random
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from webdriver_manager.chrome import ChromeDriverManager

# Function to initialize Selenium driver
def init_driver():
    options = uc.ChromeOptions()
    options.headless = False  # Run in visible mode for debugging
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = uc.Chrome(driver_executable_path=ChromeDriverManager().install(), options=options)
    return driver

# Function to scrape product details
def scrape_product_details(driver, product_url):
    """Scrapes detailed product specifications from a product page."""
    try:
        driver.get(product_url)
        time.sleep(random.uniform(2, 5))  # Random delay to avoid detection

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div#additional-info table, div#technical-info table"))
        )

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        specs = {}

        # Find specification tables
        spec_tables = soup.select("div#additional-info table, div#technical-info table")
        for table in spec_tables:
            rows = table.find_all("tr")
            for row in rows:
                cols = row.find_all("td")
                if len(cols) == 2:
                    key = cols[0].text.strip()
                    value = cols[1].text.strip()
                    specs[key] = value

        return specs
    except Exception as e:
        print(f"Error scraping {product_url}: {e}")
        return {}

# Function to scrape Ubuy product listings
def scrape_ubuy_selenium(base_url, max_pages=3):
    """Scrapes multiple pages of Ubuy using Selenium and BeautifulSoup."""
    driver = init_driver()
    
    try:
        scraped_items = []
        all_spec_keys = set()
        current_page = 1
        current_url = base_url

        while current_page <= max_pages:
            print(f"Scraping page {current_page}: {current_url}")
            driver.get(current_url)
            time.sleep(random.uniform(2, 5))

            # Wait for product listings
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.product-card"))
                )
            except Exception as e:
                print(f"Error waiting for product cards: {e}")
                break

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            product_blocks = soup.find_all('div', class_='product-card')
            if not product_blocks:
                print("No products found. The page structure may have changed.")
                break

            # Collect product URLs
            product_urls = []
            for product in product_blocks:
                link_element = product.find('a', class_='product-img')
                if link_element and "href" in link_element.attrs:
                    product_url = link_element['href']
                    full_product_url = f"https://www.ubuy.ma{product_url}" if product_url.startswith('/') else product_url
                    product_urls.append(full_product_url)

            # Scrape product details concurrently
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_url = {executor.submit(scrape_product_details, driver, url): url for url in product_urls}
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        specifications = future.result()
                        all_spec_keys.update(specifications.keys())

                        for product in product_blocks:
                            link_element = product.find('a', class_='product-img')
                            if link_element and "href" in link_element.attrs:
                                product_url = link_element['href']
                                full_product_url = f"https://www.ubuy.ma{product_url}" if product_url.startswith('/') else product_url
                                if full_product_url == url:
                                    title_element = product.find('h3', class_='product-title')
                                    title = title_element.text.strip() if title_element else "No title"

                                    price_element = product.find('p', class_='product-price')
                                    price = price_element.text.strip() if price_element else "No price"

                                    image_element = product.find('img')
                                    image_url = image_element['src'] if image_element else "No image"

                                    product_data = {
                                        "title": title,
                                        "price": price,
                                        "image_url": image_url,
                                        "product_url": full_product_url,
                                        "specifications": specifications
                                    }
                                    scraped_items.append(product_data)
                                    break
                    except Exception as e:
                        print(f"Error processing {url}: {e}")

            # Find and update the next page URL
            next_page_element = soup.find('a', class_='next-page')
            if next_page_element and "href" in next_page_element.attrs:
                current_url = f"https://www.ubuy.ma{next_page_element['href']}"
                current_page += 1
            else:
                print("No more pages found.")
                break

        return scraped_items, all_spec_keys
    finally:
        driver.quit()

# Function to determine next scrape number
def get_next_scrape_number(output_dir, category):
    """Determine the next scrape number globally, regardless of the date."""
    scrape_number = 1
    for filename in os.listdir(output_dir):
        if filename.startswith(f"{category}_") and filename.endswith(".csv"):
            try:
                current_number = int(filename.split('_scrape')[-1].split('.')[0])
                if current_number >= scrape_number:
                    scrape_number = current_number + 1
            except ValueError:
                continue
    return scrape_number

# Function to save scraped data to CSV
def save_to_csv(data, category, all_spec_keys):
    """Saves scraped data to CSV file."""
    output_dir = "data/raw/ubuy"
    os.makedirs(output_dir, exist_ok=True)

    today_date = datetime.today().strftime("%Y_%m_%d")
    scrape_number = get_next_scrape_number(output_dir, category)
    filename = f"{category}_{today_date}_scrape{scrape_number}.csv"
    filepath = os.path.join(output_dir, filename)

    fieldnames = ["title", "price", "image_url", "product_url"] + list(all_spec_keys)

    with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for item in data:
            row = {
                "title": item["title"],
                "price": item["price"],
                "image_url": item["image_url"],
                "product_url": item["product_url"]
            }
            row.update(item["specifications"])
            writer.writerow(row)

    print(f"Data saved to {filepath}")

# Run scraper
category = "smart_watches"
max_pages_to_scrape = 2
scraped_data, all_spec_keys = scrape_ubuy_selenium("https://www.ubuy.ma/en/search/?ref_p=ser_tp&q=smart+watche", max_pages=max_pages_to_scrape)

if scraped_data:
    save_to_csv(scraped_data, category, all_spec_keys)
else:
    print("No data scraped.")