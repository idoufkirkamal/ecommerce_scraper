import numpy as np
import pandas as pd
import re
from fuzzywuzzy import process

file_path = r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\ebay\graphics_cards_2025_01_29_scrape1.csv'
df = pd.read_csv(file_path)



# Fonction pour nettoyer les titres
def clean_title(row):
    # Utiliser comme base le modèle de GPU et la marque
    gpu_model = str(row["Chipset/GPU Model"]).strip()
    brand = str(row["Brand"]).strip()

    # Liste de termes inutiles à supprimer
    unwanted_terms = [
        r'\bnew\b', r'\bused\b', r'\bgraphics\b', r'\bcard\b', r'\bgpu\b', r'\bvideo\b', r'\bhdmi\b', r'\bvga\b',
        r'\bdvi\b', r'\bdisplayport\b', r'\bminidisplayport\b', r'\busb-c\b', r'\boc\b', r'\bgddr5\b', r'\bgddr6\b',
        r'\bgddr6x\b', r'\b256-bit\b', r'\b128-bit\b', r'\b512mb\b', r'\b1gb\b', r'\b2gb\b', r'\b4gb\b', r'\b8gb\b',
        r'\b12gb\b', r'\b16gb\b', r'\b24gb\b', r'\b32gb\b', r'\b64gb\b', r'\b128mb\b', r'\b256mb\b', r'\b512mb\b',
        r'\b1yr\b', r'\bwarranty\b', r'\bfast\b', r'\bship\b', r'\btested\b', r'\bworking\b', r'\bnot working\b',
        r'\bexcellent\b', r'\bcondition\b', r'\brefurbished\b', r'\bgrade\b', r'\bwith box\b', r'\bwithout box\b',
        r'\bbulk\b', r'\boem\b', r'\bretail\b', r'\bpackage\b', r'\bpackaging\b', r'\bmodel\b', r'\bseries\b',
        r'\bversion\b', r'\bgen\b', r'\bgen\b', r'\bpcie\b', r'\bexpress\b', r'\bslot\b', r'\bconnector\b',
        r'\binterface\b', r'\bdual\b', r'\bsingle\b', r'\btriple\b', r'\bquad\b', r'\bhex\b', r'\boct\b', r'\bcore\b',
    ]

    # Nettoyer le titre original
    clean_name = row['Title'].lower()
    for term in unwanted_terms:
        clean_name = re.sub(term, "", clean_name)

    # Supprimer multiples espaces et caractères spéciaux
    clean_name = re.sub(r"[^a-zA-Z0-9\s]", "", clean_name).strip()
    clean_name = re.sub(r"\s+", " ", clean_name)

    # Inclure le modèle de GPU et la marque
    # Prioriser les données fiables des colonnes
    if gpu_model.lower() in clean_name and brand.lower() in clean_name:
        return f"{brand} {gpu_model}"
    elif gpu_model.lower() in clean_name:
        return gpu_model
    elif brand.lower() in clean_name:
        return f"{brand} {clean_name}"
    else:
        # Dernier recours - combiner marque et modèle
        return f"{brand} {gpu_model}" if brand != "nan" else gpu_model


# Appliquer la fonction à chaque ligne
df['Cleaned Title'] = df.apply(clean_title, axis=1)

# 3. Corriger les marques avec fuzzy matching
brands = df['Brand'].dropna().unique().tolist()
brand_mapping = {}
for brand in df['Brand']:
    if pd.isna(brand): continue
    match, score = process.extractOne(brand, brands)
    brand_mapping[brand] = match if score > 85 else brand
df['Brand'] = df['Brand'].replace(brand_mapping)


# 4. Normaliser les prix

def clean_price(price):
    if isinstance(price, str):
        # Supprimer tout sauf les chiffres, les virgules et les points
        price = re.sub(r'[^\d.,]', '', price)
        # Si une virgule est suivie d'un point ou l'inverse, corriger une éventuelle confusion
        if ',' in price and '.' in price:
            if price.index(',') < price.index('.'):
                price = price.replace(',', '')  # Suppression des virgules dans les formats comme "1,234.56"
            else:
                price = price.replace('.', '').replace(',', '.')  # Conversion des formats européens "1.234,56"
        else:
            price = price.replace(',', '.')  # Remplacement simple des virgules par des points pour les décimales
    return float(price) if price else None


# Appliquer la transformation sur la colonne 'Price'
df['Price'] = df['Price'].apply(clean_price)


# 5. Normaliser la mémoire
def convert_to_gb(value):
    # Vérifie si la valeur est manquante ou nulle
    if pd.isna(value) or value.strip() == '':
        return None  # Retourne une valeur nulle si la donnée est absente
    try:
        # Conversion spécifique pour les valeurs exprimées en MB
        if 'MB' in value.upper():
            return float(value.upper().replace('MB', '').strip()) / 1024
        # Conversion standard en traitant les caractères non numériques
        return float(re.sub(r'[^\d.]', '', value))
    except ValueError:
        return None  # Retourne une valeur nulle si la conversion échoue


df['Memory Size'] = df['Memory Size'].astype(str).apply(convert_to_gb)


# 6. Imputation des valeurs manquantes
# Fonction pour remplir les valeurs manquantes ou groupe entièrement vide
def fill_with_median_or_default(series):
    if series.notna().any():  # Vérifie qu'il y a au moins une valeur valide
        return series.fillna(series.median())
    else:
        return series.fillna(0)  # Remplace les groupes vides par 0 (ou autre valeur)


df['Memory Size'] = df.groupby('Chipset/GPU Model')['Memory Size'].transform(fill_with_median_or_default)
df['Price'] = df.groupby('Chipset/GPU Model')['Price'].transform(fill_with_median_or_default)
# Function to fill missing values with the mode
def fill_with_mode(series):
    if series.notna().any():
        return series.fillna(series.mode()[0])
    else:
        return series

# Apply the function to 'Memory Type' and 'Connectors' columns
df['Memory Type'] = df.groupby('Chipset/GPU Model')['Memory Type'].transform(fill_with_mode)
df['Connectors'] = df.groupby('Chipset/GPU Model')['Connectors'].transform(fill_with_mode)

# Supprimer les doublons en conservant la ligne avec le prix minimum
def remove_duplicates_with_min_price(df):
    columns_for_duplicates = [ 'Brand', 'Memory Size', 'Memory Type', 'Chipset/GPU Model']
    idx_min_price = df.groupby(columns_for_duplicates)['Price'].idxmin()
    return df.loc[idx_min_price].reset_index(drop=True)


# Appliquer cette méthode à votre DataFrame
df = remove_duplicates_with_min_price(df)



# 8. Validation des données
df = df[(df['Price'] > 0) & (df['Memory Size'] > 0.1)]

# 9. Sauvegarder
output_path = r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\cleaned\ebay\cleaned_gpu1.csv'
df.to_csv(output_path, index=False)