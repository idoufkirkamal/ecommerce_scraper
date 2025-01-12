import requests
from bs4 import BeautifulSoup
import time
from fake_useragent import UserAgent


# Function to fetch a URL with retries
def fetch_url(url, headers, retries=3):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                return response
            else:
                print(f"Received status code {response.status_code}, retrying...")
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            time.sleep(2)  # Wait before retrying
    return None  # Return None if all retries fail


# Function to scrape a single Amazon search results page
def scrape_page(url, headers):
    html = fetch_url(url, headers)
    if not html:
        print("Failed to fetch the URL.")
        return []

    soup = BeautifulSoup(html.text, "html.parser")
    results = soup.find_all('div', attrs={'data-component-type': 's-search-result'})

    product_list = []
    for r in results:
        # Extract title and price, handle missing elements properly
        title = r.select_one('.a-text-normal')
        price = r.select_one('.a-price .a-offscreen')

        product = {
            "title": title.text.strip() if title else "N/A",
            "price": price.text.strip() if price else "N/A"
        }
        product_list.append(product)

    return product_list


# Pagination scraping
def scrape_amazon(search_url, max_pages=5):
    user_agent = UserAgent()  # Initialize random User-Agent generator
    all_products = []

    for page in range(1, max_pages + 1):  # Iterate over multiple pages
        print(f"Scraping page {page}...")
        paginated_url = f"{search_url}&page={page}"
        headers = {
            'User-Agent': user_agent.random,  # Rotate User-Agent
            'Accept-Language': 'en-US,en;q=0.5'
        }

        # Scrape the current page
        products = scrape_page(paginated_url, headers)
        if products:
            all_products.extend(products)
        else:
            print(f"No products found on page {page}. Stopping.")
            break  # Stop if no products are found (end of results)

        # Respect Amazon's anti-scraping measures by delaying requests
        time.sleep(2)

    return all_products


# Main execution
if __name__ == "__main__":
    # URL of the Amazon search results page (customize it for your needs)
    BASE_URL = "https://www.amazon.com/s?i=computers-intl-ship&srs=16225007011&rh=n%3A16225007011&s=popularity-rank&fs=true&ref=lp_16225007011_sar"

    # Scrape up to 5 pages of results
    products = scrape_amazon(BASE_URL, max_pages=5)

    # Output the results
    print(f"Total products scraped: {len(products)}")
    for product in products:
        print(product)
