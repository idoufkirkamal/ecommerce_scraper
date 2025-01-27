import aiohttp
import asyncio
from bs4 import BeautifulSoup
import csv
import random
from fake_useragent import UserAgent
from datetime import datetime
import os

# Initialize UserAgent for rotating headers
ua = UserAgent()


# Define headers with rotating User-Agent
def get_headers():
    return {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.ebay.com/',

        'DNT': '1'
    }


async def scrape_product_details(session, product_url, category):
    try:
        await asyncio.sleep(random.uniform(2, 5))
        headers = get_headers()

        async with session.get(product_url, headers=headers) as response:
            response.raise_for_status()
            soup = BeautifulSoup(await response.text(), 'html.parser')

            title = soup.find('h1', class_='x-item-title__mainTitle').text.strip() if soup.find('h1',
                                                                                                class_='x-item-title__mainTitle') else 'N/A'
            price = soup.find('div', class_='x-price-primary').text.strip() if soup.find('div',
                                                                                         class_='x-price-primary') else 'N/A'

            specs = {}
            for spec in soup.find_all('div', class_='ux-labels-values__labels'):
                key = spec.text.strip()
                value = spec.find_next('div', class_='ux-labels-values__values').text.strip()
                specs[key] = value

            product_details = {
                'Category': category,
                'Title': title,
                'Price': price,
                'Collection Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            if category == "Laptops":
                product_details.update({
                    'RAM': specs.get('RAM Size', 'N/A'),
                    'CPU': specs.get('Processor', 'N/A'),
                    'Model': specs.get('Model', 'N/A'),
                    'Brand': specs.get('Brand', 'N/A'),
                    'GPU': specs.get('Graphics Coprocessor', 'N/A'),
                    'Screen Size': specs.get('Screen Size', 'N/A'),
                    'Storage': specs.get('SSD Capacity', 'N/A'),
                })
            elif category == "Gaming Monitors":
                product_details.update({
                    'Screen Size': specs.get('Screen Size', 'N/A'),
                    'Maximum Resolution': specs.get('Resolution', 'N/A'),
                    'Aspect Ratio': specs.get('Aspect Ratio', 'N/A'),
                    'Refresh Rate': specs.get('Refresh Rate', 'N/A'),
                    'Response Time': specs.get('Response Time', 'N/A'),
                    'Brand': specs.get('Brand', 'N/A'),
                    'Model': specs.get('Model', 'N/A'),
                })
            elif category == "Smart Watches":
                product_details.update({
                    'Case Size': specs.get('Case Size', 'N/A'),
                    'Battery Capacity': specs.get('Battery Capacity', 'N/A'),
                    'Brand': specs.get('Brand', 'N/A'),
                    'Model': specs.get('Model', 'N/A'),
                    'Operating System': specs.get('Operating System', 'N/A'),
                    'Storage Capacity': specs.get('Storage Capacity', 'N/A')
                })
            elif category == "Graphics Cards":
                product_details.update({
                    'Brand': specs.get('Brand', 'N/A'),
                    'Memory Size': specs.get('Memory Size', 'N/A'),
                    'Memory Type': specs.get('Memory Type', 'N/A'),
                    'Chipset/GPU Model': specs.get('Chipset/GPU Model', 'N/A'),
                    'Connectors': specs.get('Connectors', 'N/A')
                })

            print(f"Successfully scraped {category}: {title[:50]}...")
            return product_details

    except Exception as e:
        print(f"Error scraping {product_url}: {str(e)}")
        return None


async def scrape_search_page(session, query, page, semaphore, category):
    async with semaphore:
        try:
            base_url = "https://www.ebay.com/sch/i.html"
            params = {
                '_nkw': query,
                '_sacat': 0,
                '_from': 'R40',
                '_pgn': page
            }

            headers = get_headers()
            async with session.get(base_url, params=params, headers=headers) as response:
                response.raise_for_status()
                soup = BeautifulSoup(await response.text(), 'html.parser')

                items = soup.find_all('div', class_='s-item__wrapper')
                product_urls = [
                    item.find('a', class_='s-item__link')['href']
                    for item in items
                    if item.find('a', class_='s-item__link')
                ]

                print(f"Scraped page {page} for {category} ({len(product_urls)} products)")
                return product_urls

        except Exception as e:
            print(f"Error scraping page {page} for {category}: {str(e)}")
            return []


async def scrape_ebay_search(categories, max_pages=1):
    all_products = {}
    semaphore = asyncio.Semaphore(2)

    async with aiohttp.ClientSession() as session:
        for category, query in categories.items():
            print(f"\n{'=' * 30}\nStarting {category} scraping\n{'=' * 30}")
            tasks = [scrape_search_page(session, query, page, semaphore, category)
                     for page in range(1, max_pages + 1)]

            search_results = await asyncio.gather(*tasks)
            product_urls = [url for sublist in search_results for url in sublist]

            product_tasks = [scrape_product_details(session, url, category) for url in product_urls]
            products = await asyncio.gather(*product_tasks)

            all_products[category] = [p for p in products if p]
            print(f"\n{'=' * 30}\nCompleted {category} ({len(all_products[category])} items)\n{'=' * 30}")

    return all_products


def save_to_csv(data, filename, fieldnames):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


async def main():
    categories = {
        "Laptops": "laptop mac",
        "Gaming Monitors": "gaming monitor",
        "Smart Watches": "smart watch",
        "Graphics Cards": "graphics card"
    }

    max_pages = 19
    save_directory = r"C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw"

    print("\nStarting eBay scraping...")
    all_products = await scrape_ebay_search(categories, max_pages)

    category_fields = {
        "Laptops": ['Category', 'Title', 'Price', 'RAM', 'CPU', 'Model',
                    'Brand', 'GPU', 'Screen Size', 'Storage', 'Collection Date'],
        "Gaming Monitors": ['Category', 'Title', 'Price', 'Screen Size', 'Maximum Resolution',
                            'Aspect Ratio', 'Refresh Rate', 'Response Time', 'Brand', 'Model', 'Collection Date'],
        "Smart Watches": ['Category', 'Title', 'Price', 'Case Size', 'Battery Capacity',
                          'Brand', 'Model', 'Operating System', 'Storage Capacity', 'Collection Date'],
        "Graphics Cards": ['Category', 'Title', 'Price', 'Brand', 'Memory Size',
                           'Memory Type', 'Chipset/GPU Model', 'Connectors', 'Collection Date']
    }

    for category, products in all_products.items():
        if not products:
            continue

        save_filename = os.path.join(save_directory, f"{category.lower().replace(' ', '_')}_results.csv")
        save_to_csv(products, save_filename, category_fields[category])
        print(f"Saved {len(products)} {category} items to {save_filename}")


if __name__ == "__main__":
    asyncio.run(main())