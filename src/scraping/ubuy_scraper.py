import os
import csv
import time
import random
import logging
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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Shared Functions
def get_driver():
    """Initialize an undetected ChromeDriver instance."""
    try:
        logging.info("Initializing ChromeDriver...")
        options = uc.ChromeOptions()
        options.headless = False  # Set to False for debugging
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument(f"--user-agent={get_random_user_agent()}")

        service = Service(ChromeDriverManager().install())
        driver = uc.Chrome(service=service, options=options)
        logging.info("ChromeDriver initialized successfully!")
        return driver
    except Exception as e:
        logging.error(f"Failed to initialize ChromeDriver: {e}")
        raise

def get_random_user_agent():
    """Returns a randomly selected user-agent to avoid detection."""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    ]
    return random.choice(user_agents)

def handle_captcha(driver):
    """Pause execution and allow the user to solve CAPTCHA manually."""
    logging.info("CAPTCHA detected. Please solve it manually.")
    input("Press Enter to continue after solving the CAPTCHA...")
    logging.info("Resuming script execution...")

def scrape_product_details(driver, product_url):
    """Scrapes detailed product specifications from a product page."""
    try:
        logging.info(f"Scraping product: {product_url}")
        driver.get(product_url)
        time.sleep(random.uniform(2, 5))  # Random delay

        # Check for CAPTCHA
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='captcha']"))
            )
            handle_captcha(driver)  # Pause for manual CAPTCHA solving
        except:
            logging.info("No CAPTCHA detected. Proceeding with scraping...")

        # Wait for the specifications table to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div#additional-info table, div#technical-info table"))
        )

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        specs = {}

        # Extract specifications
        spec_tables = soup.select("div#additional-info table, div#technical-info table")
        for table in spec_tables:
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) == 2:
                    key = cols[0].text.strip()
                    value = cols[1].text.strip()
                    specs[key] = value

        return specs
    except Exception as e:
        logging.error(f"Error scraping {product_url}: {e}")
        return {}

def get_next_scrape_number(output_dir, category):
    """Determines the next scrape number for versioning output files."""
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

def save_to_csv(data, category, all_spec_keys):
    """Saves scraped data to a CSV file with specifications in separate columns."""
    # Create category-specific directory
    output_dir = os.path.join("data/raw/ubuy", category)
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

    logging.info(f"Data saved to {filepath}")

# Category-Specific Scraping Functions
def scrape_ubuy(driver, base_url, max_pages):
    """Scrapes product data from multiple pages on Ubuy."""
    scraped_items = []
    all_spec_keys = set()
    
    try:
        current_page = 1
        current_url = base_url

        while current_page <= max_pages:
            logging.info(f"Scraping page {current_page}: {current_url}")
            driver.get(current_url)
            time.sleep(random.uniform(3, 6))  # Random delay

            # Check for CAPTCHA
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='captcha']"))
                )
                handle_captcha(driver)  # Pause for manual CAPTCHA solving
            except:
                logging.info("No CAPTCHA detected. Proceeding with scraping...")

            # Wait for product listings
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.product-card"))
                )
            except Exception as e:
                logging.error("No products found. Page may have changed.")
                break

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            product_blocks = soup.find_all('div', class_='product-card')

            if not product_blocks:
                logging.info("No products found. Exiting scraping.")
                break

            # Collect product URLs
            product_urls = []
            for product in product_blocks:
                link_element = product.find('a', class_='product-img')
                if link_element and "href" in link_element.attrs:
                    product_url = link_element['href']
                    full_product_url = f"https://www.ubuy.ma{product_url}" if product_url.startswith('/') else product_url
                    product_urls.append(full_product_url)

            # Scrape details concurrently
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_url = {executor.submit(scrape_product_details, driver, url): url for url in product_urls}
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        specifications = future.result()
                        all_spec_keys.update(specifications.keys())

                        # Find corresponding product details
                        for product in product_blocks:
                            link_element = product.find('a', class_='product-img')
                            if link_element and "href" in link_element.attrs:
                                product_url = link_element['href']
                                full_product_url = f"https://www.ubuy.ma{product_url}" if product_url.startswith('/') else product_url
                                if full_product_url == url:
                                    title = product.find('h3', class_='product-title').text.strip() if product.find('h3', class_='product-title') else "No title"
                                    price = product.find('p', class_='product-price').text.strip() if product.find('p', class_='product-price') else "No price"
                                    image_url = product.find('img')['src'] if product.find('img') else "No image"

                                    scraped_items.append({
                                        "title": title,
                                        "price": price,
                                        "image_url": image_url,
                                        "product_url": full_product_url,
                                        "specifications": specifications
                                    })
                                    break
                    except Exception as e:
                        logging.error(f"Error processing {url}: {e}")

            # Find and update next page URL
            next_page_element = soup.find('li', class_='page-item', title=str(current_page + 1))
            if next_page_element:
                next_button = next_page_element.find('button', class_='page-link')
                if next_button and "data-pageno" in next_button.attrs:
                    next_page_number = next_button['data-pageno']
                    current_url = f"{base_url}&page={next_page_number}"
                    current_page += 1
                    time.sleep(random.uniform(3, 7))  # Randomized delay to prevent bot detection
                else:
                    logging.info("No more pages found.")
                    break
            else:
                logging.info("No more pages found.")
                break

    except Exception as e:
        logging.error(f"Error during scraping: {e}")
    finally:
        driver.quit()

    return scraped_items, all_spec_keys

# Main Execution
if __name__ == "__main__":
    try:
        logging.info("Starting script...")
        categories = {
            "graphics_cards": ("https://www.ubuy.ma/en/search/?ref_p=ser_tp&q=graphics+cards", 18),
            "laptops": ("https://www.ubuy.ma/en/category/laptops-21457", 15),
            "monitors": ("https://www.ubuy.ma/en/search/?q=computer%20monitor", 15),
            "smart_watches": ("https://www.ubuy.ma/en/search/?ref_p=ser_tp&q=smart+watch", 15)
        }

        for category, (base_url, max_pages) in categories.items():
            logging.info(f"Scraping {category}...")
            driver = get_driver()
            scraped_data, all_spec_keys = scrape_ubuy(driver, base_url, max_pages)

            if scraped_data:
                save_to_csv(scraped_data, category, all_spec_keys)
            else:
                logging.info(f"No data scraped for {category}.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")