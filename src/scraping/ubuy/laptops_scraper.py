import os
import csv
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

def scrape_ubuy_selenium(url):
    """Scrapes product data from Ubuy using Selenium and BeautifulSoup."""
    
    # Set up Chrome options
    options = Options()
    options.add_argument("--headless")  # Run in headless mode (no browser window)
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    # Automatically install and use the correct ChromeDriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        print(f"Opening {url} in Selenium...")
        driver.get(url)
        time.sleep(5)  # Wait for JavaScript to load content

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Find product elements (Update class names if needed)
        product_blocks = soup.find_all('div', class_='product-card')

        if not product_blocks:
            print("No products found. The page structure may have changed.")
            return []

        scraped_items = []
        for product in product_blocks:
            title_element = product.find('h3', class_='product-title')
            title = title_element.text.strip() if title_element else "No title"

            price_element = product.find('p', class_='product-price')
            price = price_element.text.strip() if price_element else "No price"

            image_element = product.find('img')
            image_url = image_element['src'] if image_element else "No image"

            link_element = product.find('a', class_='product-img')
            product_url = link_element['href'] if link_element else "No link"

            # Format product URL correctly
            full_product_url = f"https://www.ubuy.ma{product_url}" if product_url.startswith('/') else product_url

            scraped_items.append({
                "title": title,
                "price": price,
                "image_url": image_url,
                "product_url": full_product_url,
            })

        return scraped_items
    finally:
        driver.quit()

def save_to_csv(data, category):
    """Saves scraped data to a CSV file in raw/ubuy/."""
    
    # Create directory if it doesn't exist
    output_dir = "data/raw/ubuy"
    os.makedirs(output_dir, exist_ok=True)

    # Generate filename with date and scrape count
    today_date = datetime.today().strftime("%Y_%m_%d")
    scrape_count = 1
    filename = f"{category}_{today_date}_scrape{scrape_count}.csv"
    filepath = os.path.join(output_dir, filename)

    # Increment filename if it already exists
    while os.path.exists(filepath):
        scrape_count += 1
        filename = f"{category}_{today_date}_scrape{scrape_count}.csv"
        filepath = os.path.join(output_dir, filename)

    # Save data to CSV
    with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["title", "price", "image_url", "product_url"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Data saved to {filepath}")

# Run scraper and save data
category = "laptops"
scraped_data = scrape_ubuy_selenium("https://www.ubuy.ma/en/category/laptops-21457")

if scraped_data:
    save_to_csv(scraped_data, category)
else:
    print("No data scraped.")
