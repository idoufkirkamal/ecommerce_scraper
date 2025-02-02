import os
import pandas as pd
import numpy as np
import re

# Définir les chemins relatifs pour eBay
base_dir = os.path.dirname(os.path.abspath(__file__))
raw_data_dir_ebay = os.path.join(base_dir, '..', '..', '..', 'data', 'raw', 'ebay', 'monitors')
cleaned_data_dir_ebay = os.path.join(base_dir, '..', '..', '..', 'data', 'cleaned', 'ebay', 'monitors')

# Créer les dossiers de sortie s'ils n'existent pas
os.makedirs(cleaned_data_dir_ebay, exist_ok=True)


# Fonction pour nettoyer les moniteurs eBay
def clean_monitors_ebay(df):
    # 1. Nettoyage de la colonne Price
    if 'Brand' not in df.columns:
        print("⚠️ La colonne 'Brand' est absente. Une valeur par défaut sera utilisée.")
        df['Brand'] = 'Unknown'

    def clean_price(price):
        if isinstance(price, str):
            cleaned = ''.join(filter(lambda x: x.isdigit() or x == '.', price))
            return float(cleaned) if cleaned else np.nan
        return price

    df['Price'] = df['Price'].apply(clean_price)

    # 2. Uniformisation de Screen Size
    def clean_screen_size(size):
        if isinstance(size, str):
            size = size.replace('"', '').replace("''", "").replace("inches", "")
            matches = [s for s in size.split() if s.replace('.', '').isdigit()]
            return float(matches[0]) if matches else np.nan
        return size

    df['Screen Size'] = df['Screen Size'].apply(clean_screen_size)
    brand_avg_size = df.groupby('Brand')['Screen Size'].transform('median')
    df['Screen Size'] = df['Screen Size'].fillna(brand_avg_size)
    median_screen_size = df['Screen Size'].dropna().median()
    df['Screen Size'] = df['Screen Size'].fillna(median_screen_size)

    # Nettoyer la colonne "Response Time"
    def clean_response_time(response_time):
        if isinstance(response_time, str):
            match = re.search(r'(\d+(\.\d*)?)', response_time)
            return float(match.group(0)) if match else np.nan
        elif isinstance(response_time, (int, float)):
            return float(response_time)
        return np.nan

    df['Response Time'] = df['Response Time'].apply(clean_response_time)
    median_response_time = df['Response Time'].dropna().median()
    df['Response Time'] = df['Response Time'].fillna(median_response_time)

    # Extraction du Refresh Rate
    def extract_refresh_rate(row):
        original_value = row['Refresh Rate']
        if pd.notna(original_value):
            matches = re.findall(r'\d+', str(original_value))
            return max(map(int, matches)) if matches else None
        title_matches = re.findall(r'(\d+)\s*Hz', row['Title'], flags=re.IGNORECASE)
        if title_matches:
            return max(map(int, title_matches))
        if "HZ" in row['Title'].upper():
            return int(re.search(r'(\d+)\s*HZ', row['Title'].upper()).group(1))
        return None

    df['Refresh Rate'] = df.apply(extract_refresh_rate, axis=1)

    # Renommer les colonnes
    new_columns = {
        'Screen Size': 'Screen_Size_in',
        'Maximum Resolution': 'Max_Resolution',
        'Aspect Ratio': 'Aspect_Ratio',
        'Refresh Rate': 'Refresh_Rate_Hz',
        'Response Time': 'Response_Time_ms'
    }
    df = df.rename(columns=new_columns)

    # Nettoyage avancé du titre
    def clean_title_advanced(title):
        terms_to_remove = [
            r'\bGaming Monitor\b',
            r'\b\d+\.?\d*\s*Hz\b',
            r'\b\d+\.?\d*\s*ms\b',
            r'\b(HDR\d*|IPS|VA|TN|OLED)\b',
            r'\b(Curved|Flat|UltraWide)\b'
        ]
        title = re.sub(r'\b\d+\.?\d*\s*(inch|inches|"|’’|’)\b', '', title, flags=re.IGNORECASE)
        title = re.sub(r'\b(\d{3,4}[x×]\d{3,4}|FHD|HD|UHD|QHD|WQHD|4K)\b', '', title, flags=re.IGNORECASE)
        for pattern in terms_to_remove:
            title = re.sub(pattern, '', title, flags=re.IGNORECASE)
        title = re.sub(r'[^\w\s.-]', '', title)
        title = re.sub(r'\s+', ' ', title).strip()
        return title

    df['Title'] = df['Title'].apply(clean_title_advanced)

    # Suppression des doublons
    df = df.drop('Max_Resolution', axis=1)
    df = df.dropna(thresh=df.shape[1] - 2)

    def remove_duplicates_with_min_price(dataframe, columns_to_check, price_column):
        dataframe = dataframe.sort_values(by=price_column, ascending=True)
        dataframe = dataframe.drop_duplicates(subset=columns_to_check, keep='first')
        return dataframe

    columns_to_check_for_duplicates = ['Brand', 'Model', 'Screen_Size_in', 'Refresh_Rate_Hz', 'Response_Time_ms']
    df = remove_duplicates_with_min_price(df, columns_to_check_for_duplicates, 'Price')
    return df


# Traiter les fichiers eBay
for filename in os.listdir(raw_data_dir_ebay):
    if filename.endswith('.csv'):
        file_path = os.path.join(raw_data_dir_ebay, filename)
        df = pd.read_csv(file_path)
        cleaned_df = clean_monitors_ebay(df)
        cleaned_filename = f"cleaned_{filename}"
        cleaned_file_path = os.path.join(cleaned_data_dir_ebay, cleaned_filename)
        cleaned_df.to_csv(cleaned_file_path, index=False)
        print(f"Fichier nettoyé (eBay) : {cleaned_file_path}")