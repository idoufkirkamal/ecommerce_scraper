import os
import csv
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

def scrape_product_details(driver, product_url):
    """Scrapes detailed product specifications from a product page using the existing driver instance."""
    try:
        print(f"Opening product page: {product_url}")
        driver.get(product_url)
        
        # Wait for the specification table to load
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
                if len(cols) == 2:  # Ensure valid row
                    key = cols[0].text.strip()
                    value = cols[1].text.strip()
                    specs[key] = value

        return specs
    except Exception as e:
        print(f"Error scraping {product_url}: {e}")
        return {}

def scrape_ubuy_selenium(base_url, max_pages=3):
    """Scrapes multiple pages of product data from Ubuy using Selenium and BeautifulSoup."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        scraped_items = []
        all_spec_keys = set()
        current_page = 1
        current_url = base_url

        while current_page <= max_pages:
            print(f"Scraping page {current_page}: {current_url}")
            driver.get(current_url)
            
            # Wait for product cards to load
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

            # Collect product URLs for concurrent scraping
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

                        # Find the corresponding product block
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

def save_to_csv(data, category, all_spec_keys):
    """Saves scraped data to a CSV file with each specification in its own column."""
    output_dir = "data/raw/ubuy"
    os.makedirs(output_dir, exist_ok=True)

    today_date = datetime.today().strftime("%Y_%m_%d")
    scrape_count = 1
    filename = f"{category}_{today_date}_scrape{scrape_count}.csv"
    filepath = os.path.join(output_dir, filename)

    while os.path.exists(filepath):
        scrape_count += 1
        filename = f"{category}_{today_date}_scrape{scrape_count}.csv"
        filepath = os.path.join(output_dir, filename)

    # Define column headers
    fieldnames = ["title", "price", "image_url", "product_url"] + list(all_spec_keys)

    # Save data to CSV
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
            # Add specifications dynamically
            row.update(item["specifications"])
            writer.writerow(row)

    print(f"Data saved to {filepath}")

# Run scraper with pagination support
category = "monitors"
max_pages_to_scrape = 18  # Adjust this value to control the number of pages scraped
scraped_data, all_spec_keys = scrape_ubuy_selenium("https://www.ubuy.ma/en/search/?q=computer%20monitor&u_sr_id=08b5411f848a2581a41672a759c87380", max_pages=max_pages_to_scrape)

if scraped_data:
    save_to_csv(scraped_data, category, all_spec_keys)
else:
    print("No data scraped.")