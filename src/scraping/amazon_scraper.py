import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime

# Liste des User-Agents pour rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0',
    'Mozilla/5.0 (iPad; CPU OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
]


# Fonction pour scraper une seule page
def scrape_amazon_page(url):
    # Selectionner un User-Agent aléatoire
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US, en;q=0.5'
    }

    # Envoyer une requête HTTP
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Trouver les produits sur la page
    results = soup.find_all('div', {'data-component-type': 's-search-result'})
    result_list = []

    # Date actuelle (pour suivi de la collecte)
    collection_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for r in results:
        result_dict = {}

        # Extraire le titre
        title_element = r.select_one('.a-size-medium.a-color-base.a-text-normal')
        result_dict["title"] = title_element.text.strip() if title_element else "Title not available"

        # Extraire le prix
        price_element = r.select_one('.a-price .a-offscreen')
        result_dict["price"] = price_element.text.strip() if price_element else "Price not available"

        # Extraire l'URL de l'image

        # Extraire les promotions (réductions, offres spéciales)
        promo_element = r.select_one(
            '.a-row.a-size-base.a-color-secondary.s-align-children-center span.a-color-base.a-text-bold')
        result_dict["promo"] = promo_element.text.strip() if promo_element else "No promotion"

        # Coupons (exemple de coupon avec réduction)
        coupon_element = r.select_one('.s-coupon-clipped .a-color-base')
        result_dict["coupon"] = coupon_element.text.strip() if coupon_element else "No coupon"

        # Indiquer la date de collecte
        result_dict["collection_date"] = collection_date

        # Ajouter le dictionnaire des résultats à la liste
        result_list.append(result_dict)

    return result_list


# Fonction pour scraper plusieurs pages et enregistrer au format CSV
def scrape_amazon(search_query, num_pages):
    base_url = f"https://www.amazon.com/s?k={search_query.replace(' ', '+')}&page="
    all_results = []

    for page in range(1, num_pages + 1):
        print(f"Scraping page {page}...")
        url = base_url + str(page)
        page_results = scrape_amazon_page(url)
        all_results.extend(page_results)

        # Pause aléatoire pour éviter le blocage
        random_delay = random.randint(2, 5)
        print(f"Waiting {random_delay} seconds to avoid detection...")
        time.sleep(random_delay)

    # Convertir les résultats au format DataFrame
    df = pd.DataFrame(all_results)

    # Créer le dossier 'data' s'il n'existe pas
    output_dir = r"C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw"
    os.makedirs(output_dir, exist_ok=True)

    # Chemin complet pour le fichier CSV
    csv_file_path = os.path.join(output_dir, "amazon_results.csv")

    # Sauvegarder sous forme de fichier CSV
    df.to_csv(csv_file_path, index=False, encoding='utf-8')
    print(f"Data saved to {csv_file_path}")

    return all_results


# Script principal
if __name__ == "__main__":
    search_query = "computers"  # Requête de recherche
    num_pages = 200  # Nombre de pages à scraper

    # Scraper Amazon et sauvegarder les résultats
    scraped_data = scrape_amazon(search_query, num_pages)

    # Afficher un aperçu des données
    for product in scraped_data[:5]:  # Affiche les 5 premiers produits
        print(product)