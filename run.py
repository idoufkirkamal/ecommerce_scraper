from src.scraping.aliexpress_scraper import scrape_aliexpress
from src.scraping.amazon_scraper_laptop import scrape_amazon
from src.scraping.jumia_scraper import scrape_jumia
from src.cleaning.clean_utils import clean_all_data
from src.analysis.analyze_prices import analyze_prices

if __name__ == "__main__":
    print("Démarrage du scraping...")
    scrape_aliexpress()
    scrape_amazon()
    scrape_jumia()

    print("Nettoyage des données...")
    clean_all_data()

    print("Analyse des prix...")
    analyze_prices()

    print("Projet terminé.")
