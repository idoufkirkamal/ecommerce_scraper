import pandas as pd
import re

# Étape 1: Charger les données
df = pd.read_csv(r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\flipkart\graphics_cards\graphics_cards_2025_01_29_scrape1.csv')

# Étape 2: Nettoyer les données pour chaque colonne

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
    return df[['title', 'price', 'Brand', 'Memory Size', 'Memory Type', 'Chipset/GPU Model', 'Connectors']]
# Étape 5: Gérer les valeurs manquantes
def fill_missing_values(df):
    df.loc[:, 'Price'] = df['price'].apply(clean_price)
    df.loc[:, 'Brand'] = df['Brand'].fillna('Unknown')
    df.loc[:, 'Memory Size'] = df['Memory Size'].fillna(0)
    df.loc[:, 'Memory Type'] = df['Memory Type'].fillna('Unknown')
    df.loc[:, 'Chipset/GPU Model'] = df['Chipset/GPU Model'].fillna('Unknown')
    df.loc[:, 'Connectors'] = df['Connectors'].fillna('Unknown')
    return df
# Étape 6: Sauvegarder le DataFrame nettoyé
def save_cleaned_data(df, filename='cleaned_graphics_cards.csv'):
    df.to_csv(filename, index=False)
# Supprimer les colonnes avec exactement trois valeurs 'Unknown'
def remove_rows_with_three_unknowns(df):
    unknown_counts = df.isin(['Unknown']).sum(axis=1)
    return df[unknown_counts < 3]
def rename_and_drop_price_column(df):
    df.rename(columns={'price': 'Price'}, inplace=True)
    return df.drop(columns=['price'])
# Nettoyage complet des données
def clean_data(df):
    df = df.copy()  # Créer une copie explicite du DataFrame
    df = extract_missing_data(df)
    df = drop_unnecessary_columns(df)
    df = fill_missing_values(df)
    df = remove_rows_with_three_unknowns(df)
    df = rename_and_drop_price_column(df)
    return df




# Appliquer le nettoyage des données
df_cleaned = clean_data(df)

# Sauvegarder les données nettoyées
save_cleaned_data(df_cleaned)