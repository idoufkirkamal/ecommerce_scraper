import pandas as pd
import re
import os
from pathlib import Path

# Define paths using relative paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # Root folder of the project
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw' / 'flipkart' / 'graphics_cards'
CLEANED_DATA_DIR = BASE_DIR / 'data' / 'cleaned' / 'flipkart' / 'graphics_cards'

# Debugging: Print paths
print(f"Base directory: {BASE_DIR}")
print(f"Raw data directory: {RAW_DATA_DIR}")
print(f"Cleaned data directory: {CLEANED_DATA_DIR}")

# Ensure the cleaned data directory exists
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Check if raw data directory exists
if not RAW_DATA_DIR.exists():
    raise FileNotFoundError(f"Raw data directory not found: {RAW_DATA_DIR}")

# Check if there are CSV files to process
files = list(RAW_DATA_DIR.glob('*.csv'))
if not files:
    print(f"No CSV files found in {RAW_DATA_DIR}")
else:
    print(f"Found {len(files)} CSV files to process")

# Fonction pour nettoyer le prix
def clean_price(price):
    if pd.isna(price) or price == 'Data not available':
        return None
    return int(re.sub(r'[^\d]', '', price))

# Fonction pour extraire la taille de la mémoire
def extract_memory_size(memory):
    if pd.isna(memory) or memory == 'Data not available':
        return None
    match = re.search(r'(\d+) GB', memory)
    return int(match.group(1)) if match else None

# Fonction pour extraire le type de mémoire
def extract_memory_type(memory):
    if pd.isna(memory) or memory == 'Data not available':
        return None
    match = re.search(r'\d+ GB (\w+)', memory)
    return match.group(1) if match else None

# Fonction pour extraire le chipset/GPU model
def extract_gpu_model(model_id):
    if pd.isna(model_id) or model_id == 'Data not available':
        return None
    return model_id

# Fonction pour extraire les connecteurs
def extract_connectors(dvi_hmdi_interface):
    if pd.isna(dvi_hmdi_interface) or dvi_hmdi_interface == 'Data not available':
        return None
    connectors = []
    if 'HDMI' in dvi_hmdi_interface:
        connectors.append('HDMI')
    if 'DVI' in dvi_hmdi_interface:
        connectors.append('DVI')
    if 'Mini HDMI' in dvi_hmdi_interface:
        connectors.append('Mini HDMI')
    if 'DisplayPort' in dvi_hmdi_interface:
        connectors.append('DisplayPort')
    if 'VGA' in dvi_hmdi_interface:
        connectors.append('VGA')
    return ', '.join(connectors) if connectors else None

# Étape 3: Extraire les informations manquantes
def extract_missing_data(df):
    df['Memory Size'] = df['Memory'].apply(extract_memory_size)
    df['Memory Type'] = df['Memory'].apply(extract_memory_type)
    df['Chipset/GPU Model'] = df['Model ID'].apply(extract_gpu_model)
    df['Connectors'] = df['DVI and HDMI Interface'].apply(extract_connectors)
    return df

# Étape 4: Supprimer les colonnes inutiles
def drop_unnecessary_columns(df):
    columns_to_keep = ['title', 'price', 'Brand', 'Memory Size', 'Memory Type', 'Chipset/GPU Model', 'Connectors']
    existing_columns = [col for col in columns_to_keep if col in df.columns]
    return df[existing_columns]

# Étape 5: Gérer les valeurs manquantes
def fill_missing_values(df):
    if 'price' in df.columns:
        df.loc[:, 'Price'] = df['price'].apply(clean_price)
    if 'Brand' in df.columns:
        df.loc[:, 'Brand'] = df['Brand'].fillna('Unknown')
    if 'Memory Size' in df.columns:
        df.loc[:, 'Memory Size'] = df['Memory Size'].fillna(0)
    if 'Memory Type' in df.columns:
        df.loc[:, 'Memory Type'] = df['Memory Type'].fillna('Unknown')
    if 'Chipset/GPU Model' in df.columns:
        df.loc[:, 'Chipset/GPU Model'] = df['Chipset/GPU Model'].fillna('Unknown')
    if 'Connectors' in df.columns:
        df.loc[:, 'Connectors'] = df['Connectors'].fillna('Unknown')
    return df

# Étape 6: Sauvegarder le DataFrame nettoyé
def save_cleaned_data(df, filename):
    df.to_csv(filename, index=False)

# Supprimer les colonnes avec exactement trois valeurs 'Unknown'
def remove_rows_with_three_unknowns(df):
    unknown_counts = df.isin(['Unknown']).sum(axis=1)
    return df[unknown_counts < 3]

def rename_and_drop_price_column(df):
    if 'price' in df.columns:
        df.rename(columns={'price': 'Price'}, inplace=True)
    return df

# Nettoyage complet des données
def clean_data(df):
    df = df.copy()
    df = extract_missing_data(df)
    df = drop_unnecessary_columns(df)
    df = fill_missing_values(df)
    df = remove_rows_with_three_unknowns(df)
    df = rename_and_drop_price_column(df)
    return df

# Process all CSV files in the raw data directory
try:
    for file in RAW_DATA_DIR.glob('*.csv'):
        print(f"Processing file: {file}")
        df = pd.read_csv(file)
        print(f"Loaded {len(df)} rows from {file.name}")
        print(f"Columns in the file: {df.columns.tolist()}")
        
        df_cleaned = clean_data(df)
        print(f"Cleaned data has {len(df_cleaned)} rows")
        
        output_filename = CLEANED_DATA_DIR / f"{file.stem}_cleaned.csv"
        save_cleaned_data(df_cleaned, output_filename)
        print(f"Cleaned data saved to {output_filename}")
except Exception as e:
    print(f"An error occurred: {e}")