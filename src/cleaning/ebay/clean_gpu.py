import numpy as np
import pandas as pd
import re
from fuzzywuzzy import process
from pathlib import Path

# Define paths using relative paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # Root folder of the project
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw' / 'ebay' / 'graphics_cards'
CLEANED_DATA_DIR = BASE_DIR / 'data' / 'cleaned' / 'ebay' / 'graphics_cards'

# Ensure the cleaned data directory exists
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Debugging: Print paths
print(f"Base directory: {BASE_DIR}")
print(f"Raw data directory: {RAW_DATA_DIR}")
print(f"Cleaned data directory: {CLEANED_DATA_DIR}")

# Check if raw data directory exists
if not RAW_DATA_DIR.exists():
    raise FileNotFoundError(f"Raw data directory not found: {RAW_DATA_DIR}")

# Check if there are CSV files to process
files = list(RAW_DATA_DIR.glob('*.csv'))
if not files:
    print(f"No CSV files found in {RAW_DATA_DIR}")
else:
    print(f"Found {len(files)} CSV files to process")

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
    if gpu_model.lower() in clean_name and brand.lower() in clean_name:
        return f"{brand} {gpu_model}"
    elif gpu_model.lower() in clean_name:
        return gpu_model
    elif brand.lower() in clean_name:
        return f"{brand} {clean_name}"
    else:
        return f"{brand} {gpu_model}" if brand != "nan" else gpu_model

# 3. Corriger les marques avec fuzzy matching
def correct_brands(df):
    brands = df['Brand'].dropna().unique().tolist()
    brand_mapping = {}
    for brand in df['Brand']:
        if pd.isna(brand): continue
        match, score = process.extractOne(brand, brands)
        brand_mapping[brand] = match if score > 85 else brand
    df['Brand'] = df['Brand'].replace(brand_mapping)
    return df

# 4. Normaliser les prix
def clean_price(price):
    if isinstance(price, str):
        price = re.sub(r'[^\d.,]', '', price)
        if ',' in price and '.' in price:
            if price.index(',') < price.index('.'):
                price = price.replace(',', '')
            else:
                price = price.replace('.', '').replace(',', '.')
        else:
            price = price.replace(',', '.')
    return float(price) if price else None

# 5. Normaliser la mémoire
def convert_to_gb(value):
    if pd.isna(value) or value.strip() == '':
        return None
    try:
        if 'MB' in value.upper():
            return float(value.upper().replace('MB', '').strip()) / 1024
        return float(re.sub(r'[^\d.]', '', value))
    except ValueError:
        return None

# 6. Imputation des valeurs manquantes
def fill_with_median_or_default(series):
    if series.notna().any():
        return series.fillna(series.median())
    else:
        return series.fillna(0)

def fill_with_mode(series):
    if series.notna().any():
        return series.fillna(series.mode()[0])
    else:
        return series

# 7. Supprimer les doublons en conservant la ligne avec le prix minimum
def remove_duplicates_with_min_price(df):
    columns_for_duplicates = ['Brand', 'Memory Size', 'Memory Type', 'Chipset/GPU Model']
    idx_min_price = df.groupby(columns_for_duplicates)['Price'].idxmin()
    return df.loc[idx_min_price].reset_index(drop=True)

# Process all CSV files in the raw data directory
try:
    for file in RAW_DATA_DIR.glob('*.csv'):
        print(f"Processing file: {file}")
        df = pd.read_csv(file)
        print(f"Loaded {len(df)} rows from {file.name}")
        print(f"Columns in the file: {df.columns.tolist()}")

        # Appliquer les fonctions pour nettoyer et normaliser les données
        df['Cleaned Title'] = df.apply(clean_title, axis=1)
        df = correct_brands(df)
        df['Price'] = df['Price'].apply(clean_price)
        df['Memory Size'] = df['Memory Size'].astype(str).apply(convert_to_gb)
        df['Memory Size'] = df.groupby('Chipset/GPU Model')['Memory Size'].transform(fill_with_median_or_default)
        df['Price'] = df.groupby('Chipset/GPU Model')['Price'].transform(fill_with_median_or_default)
        df['Memory Type'] = df.groupby('Chipset/GPU Model')['Memory Type'].transform(fill_with_mode)
        df['Connectors'] = df.groupby('Chipset/GPU Model')['Connectors'].transform(fill_with_mode)
        df = remove_duplicates_with_min_price(df)

        # Validation des données
        df = df[(df['Price'] > 0) & (df['Memory Size'] > 0.1)]

        # Sauvegarder le DataFrame nettoyé dans un nouveau fichier CSV
        output_filename = CLEANED_DATA_DIR / f"{file.stem}_cleaned.csv"
        df.to_csv(output_filename, index=False)
        print(f"Cleaned data saved to {output_filename}")
except Exception as e:
    print(f"An error occurred: {e}")