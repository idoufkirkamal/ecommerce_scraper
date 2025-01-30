import os
import csv
import time
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
import undetected_chromedriver as uc

def get_driver():
    """Sets up an undetected Chrome WebDriver instance."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = uc.Chrome(service=service, options=options)
    return driver

def scrape_product_details(driver, product_url):
    """Scrapes product specifications from a product page."""
    try:
        print(f"Opening product page: {product_url}")
        driver.get(product_url)
        time.sleep(random.uniform(2, 5))  # Random delay
        
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div#additional-info table, div#technical-info table"))
        )
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        specs = {}

        for table in soup.select("div#additional-info table, div#technical-info table"):
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) == 2:
                    specs[cols[0].text.strip()] = cols[1].text.strip()
        
        return specs
    except Exception as e:
        print(f"Error scraping {product_url}: {e}")
        return {}

def scrape_ubuy(base_url, max_pages=3):
    """Scrapes multiple pages of product data."""
    driver = get_driver()
    
    try:
        scraped_items = []
        all_spec_keys = set()
        current_page = 1
        current_url = base_url

        while current_page <= max_pages:
            print(f"Scraping page {current_page}: {current_url}")
            driver.get(current_url)
            time.sleep(random.uniform(3, 6))
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            product_blocks = soup.find_all('div', class_='product-card')
            if not product_blocks:
                print("No products found. The page structure may have changed.")
                break

            product_urls = []
            for product in product_blocks:
                link = product.find('a', class_='product-img')
                if link and "href" in link.attrs:
                    full_url = f"https://www.ubuy.ma{link['href']}" if link['href'].startswith('/') else link['href']
                    product_urls.append(full_url)

            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_url = {executor.submit(scrape_product_details, driver, url): url for url in product_urls}
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        specifications = future.result()
                        all_spec_keys.update(specifications.keys())
                        
                        for product in product_blocks:
                            link = product.find('a', class_='product-img')
                            if link and "href" in link.attrs:
                                full_url = f"https://www.ubuy.ma{link['href']}" if link['href'].startswith('/') else link['href']
                                if full_url == url:
                                    title = product.find('h3', class_='product-title').text.strip() if product.find('h3', class_='product-title') else "No title"
                                    price = product.find('p', class_='product-price').text.strip() if product.find('p', class_='product-price') else "No price"
                                    image_url = product.find('img')['src'] if product.find('img') else "No image"
                                    scraped_items.append({
                                        "title": title,
                                        "price": price,
                                        "image_url": image_url,
                                        "product_url": full_url,
                                        "specifications": specifications
                                    })
                                    break
                    except Exception as e:
                        print(f"Error processing {url}: {e}")

            next_page = soup.find('a', class_='next-page')
            if next_page and "href" in next_page.attrs:
                current_url = f"https://www.ubuy.ma{next_page['href']}"
                current_page += 1
            else:
                print("No more pages found.")
                break

        return scraped_items, all_spec_keys
    finally:
        driver.quit()

def get_next_scrape_number(output_dir, category):
    """Determines the next available scrape number."""
    scrape_number = 1
    for filename in os.listdir(output_dir):
        if filename.startswith(f"{category}_") and filename.endswith(".csv"):
            try:
                num = int(filename.split('_scrape')[-1].split('.')[0])
                scrape_number = max(scrape_number, num + 1)
            except ValueError:
                continue
    return scrape_number

def save_to_csv(data, category, all_spec_keys):
    """Saves scraped data to a CSV file."""
    output_dir = "data/raw/ubuy"
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{category}_{datetime.today().strftime('%Y_%m_%d')}_scrape{get_next_scrape_number(output_dir, category)}.csv"
    filepath = os.path.join(output_dir, filename)
    
    fieldnames = ["title", "price", "image_url", "product_url"] + list(all_spec_keys)
    
    with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in data:
            row = {"title": item["title"], "price": item["price"], "image_url": item["image_url"], "product_url": item["product_url"]}
            row.update(item["specifications"])
            writer.writerow(row)
    
    print(f"Data saved to {filepath}")

# Run scraper
category = "graphics_cards"
max_pages_to_scrape = 3
scraped_data, all_spec_keys = scrape_ubuy("https://www.ubuy.ma/en/search/?ref_p=ser_tp&q=graphics+cards", max_pages=max_pages_to_scrape)

if scraped_data:
    save_to_csv(scraped_data, category, all_spec_keys)
else:
    print("No data scraped.")