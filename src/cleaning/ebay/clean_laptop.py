import os
import pandas as pd
import numpy as np
import re
from pathlib import Path

# Define paths using relative paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # Root folder of the project
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw' / 'ebay' / 'laptops'
CLEANED_DATA_DIR = BASE_DIR / 'data' / 'cleaned' / 'ebay' / 'laptops'

# Ensure the cleaned data directory exists
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Debugging: Print paths
print(f"Base directory: {BASE_DIR}")
print(f"Raw data directory: {RAW_DATA_DIR}")
print(f"Cleaned data directory: {CLEANED_DATA_DIR}")

# Check if raw data directory exists
if not RAW_DATA_DIR.exists():
    raise FileNotFoundError(f"Raw data directory not found: {RAW_DATA_DIR}")
def clean_cpu(cpu):
    if pd.isna(cpu):
        return np.nan
    match = re.search(r'(Core\s+i\d|Ryzen\s+\d|Snapdragon\s+\w+)', str(cpu), re.IGNORECASE)
    return match.group(0) if match else 'Unknown CPU'
# Check if there are CSV files to process
files = list(RAW_DATA_DIR.glob('*.csv'))
if not files:
    print(f"No CSV files found in {RAW_DATA_DIR}")
else:
    print(f"Found {len(files)} CSV files to process")

# 2. Nettoyer la colonne Price
def clean_price(price):
    if isinstance(price, str):
        cleaned = ''.join(c for c in price if c.isdigit() or c in {'.', ','})
        cleaned = cleaned.replace(',', '.').replace(' ', '')
        if '.' in cleaned:
            parts = cleaned.split('.')
            if len(parts) > 2:
                cleaned = parts[0] + '.' + ''.join(parts[1:])
        return float(cleaned) if cleaned else np.nan
    return price

# 3. Uniformisation des colonnes
def convert_to_gb(value):
    if pd.isna(value) or value in ['N/A', '', ' ']:
        return np.nan
    match = re.match(r'(\d+\.?\d*)\s*([GTM]B?|GB)', str(value), re.IGNORECASE)
    if match:
        num, unit = match.groups()
        num = float(num)
        unit = unit.upper()
        if 'T' in unit:
            return num * 1000
        elif 'M' in unit:
            return num / 1000
        return num
    return np.nan

# 4. Imputation des valeurs manquantes
def impute_missing_values(df):
    imputation_rules = {
        'RAM': 'median',
        'Storage': 'median',
        'Screen Size': 'median',
        'CPU': 'ffill',
        'Price': 'median'
    }

    for col, method in imputation_rules.items():
        if method == 'median':
            df[col] = df[col].fillna(df[col].median())
        elif method == 'ffill':
            df[col] = df[col].ffill()
    return df

# 5. Nettoyer les titres
def clean_title(title):
    return re.sub(
        r'(?i)\b(8GB|. |PC|Notebook|Ryzen|UHD|Graphics|DDR4|AMD|W11|Win11|Win|11|Cond|!!|LOADED|TouchBar|Mac OS|Black| i3StorageWin|Gaming|Laptop|Touchscreen|Pro|15.6|Windows|RTX|FHD|LaptopWin11|HDD| ,|French|13inch|'
        r' - | /|macOS|VENTURA|FREE|SHIPPIN|i9|13.3|inches|TURBO|"|- | , |13INCH|EXCELLENT|'
        r'REFURBISHED|NEW|MWTK2LL|Qwerty|Spanish|Keyboard|British|\d+GB|\d+TB|[\d\.]+ ?GHz| GB |'
        r'rouge|Gray|BIG SUR|WEBCAM|WIFI|BLUETOOTHGB|TB|space gray|silver|gold|touch bar|GHz|'
        r'Intel|Core|i7|th|Gen|GB|Very|RAM|i5| GB| TB|GB GB|.GHZ| CPU | GPU|-|SSD|256|512|Good|'
        r'Condition|magic keyboard|✅|🔋|grade [A-B]|warranty\.\.\.)',
        '',
        str(title)
    ).strip().replace('  ', '')

# 6. Supprimer les doublons en conservant le prix minimum
def remove_duplicates_keep_min_price(df):
    # Convertir la colonne Price en numérique si ce n'est pas déjà fait
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    # Identifier les colonnes qui définissent les caractéristiques uniques d'un produit
    key_columns = ['Model', 'RAM', 'CPU', 'Brand', 'Storage']
    df = df.sort_values(by='Price', ascending=True)
    df_cleaned = df.drop_duplicates(subset=key_columns, keep='first')
    return df_cleaned

# Process all CSV files in the raw data directory
try:
    for file in RAW_DATA_DIR.glob('*.csv'):
        print(f"Processing file: {file}")
        df = pd.read_csv(file)
        print(f"Loaded {len(df)} rows from {file.name}")
        print(f"Columns in the file: {df.columns.tolist()}")  # Debugging: Print column names

        # Appliquer les fonctions pour nettoyer et normaliser les données
        df['Price'] = df['Price'].apply(clean_price)
        for col in ['RAM', 'Storage']:
            df[col] = df[col].apply(convert_to_gb).astype(float)
        df['Screen Size'] = df['Screen Size'].str.extract(r'(\d+\.?\d*)').astype(float)
        df = impute_missing_values(df)
        df['GPU'] = df['GPU'].fillna('Unknown Graphics')
        df['CPU'] = df['CPU'].apply(clean_cpu)
        df['Title'] = df['Title'].apply(clean_title)
        df['Model'] = df['Model'].replace('', np.nan)  # Remplacer '' par NaN pour uniformité
        df['Model'] = df['Model'].fillna(df['Title'])  # Remplacer les NaN dans 'Model' par les valeurs de 'Title'
        df['Storage'] = df['Storage'].round(2)
        df = df[df['Price'] >= 90]
        df = remove_duplicates_keep_min_price(df)

        # Sauvegarder le DataFrame nettoyé dans un nouveau fichier CSV
        output_filename = CLEANED_DATA_DIR / f"{file.stem}_cleaned.csv"
        df.to_csv(output_filename, index=False)
        print(f"Cleaned data saved to {output_filename}")
        print(f"Shape final : {df.shape}")
except Exception as e:
    print(f"An error occurred: {e}")