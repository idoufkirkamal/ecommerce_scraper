import os
import pandas as pd
import numpy as np
import re
file_path = r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\ebay\laptops_2025_01_29_scrape1.csv'
df = pd.read_csv(file_path)

# Définir le chemin d'enregistrement
output_path = r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\cleaned\ebay\cleaned_laptop1.csv'
output_dir = os.path.dirname(output_path)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)  # Crée les d
df = pd.read_csv(file_path)



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


df['Price'] = df['Price'].apply(clean_price)


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


for col in ['RAM', 'Storage']:
    df[col] = df[col].apply(convert_to_gb).astype(float)

df['Screen Size'] = df['Screen Size'].str.extract(r'(\d+\.?\d*)').astype(float)

# 4. Imputation des valeurs manquantes
imputation_rules = {
    'RAM': 'median',
    'Storage': 'median',
    'Screen Size': 'median',
    'CPU': 'ffill',
    'Price': 'median'
}

for col, method in imputation_rules.items():
    if method == 'median':
        # Remplacer inplace par une affectation explicite
        df[col] = df[col].fillna(df[col].median())
    elif method == 'ffill':
        # Remplacer inplace par une méthode d'affectation explicite
        df[col] = df[col].ffill()

# Nettoyage spécifique pour la colonne GPU
# Remplacer les valeurs NaN par une valeur par défaut (e.g., "Unknown")
df['GPU'] = df['GPU'].fillna('Unknown')

df['GPU'] = df['GPU'].fillna('Unknown Graphics')




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


df['Title'] = df['Title'].apply(clean_title)
# Corrige la condition pour la colonne 'Model'
df['Model'] = df['Model'].replace('', np.nan)  # Remplacer '' par NaN pour uniformité
df['Model'] = df['Model'].fillna(df['Title'])  # Remplacer les NaN dans 'Model' par les valeurs de 'Title'

# 6. Conversion finale du stockage en GB
df['Storage'] = df['Storage'].round(2)

# Sauvegarder le résultat
df = df[df['Price'] >= 90]


def remove_duplicates_keep_min_price(df):
    # Convertir la colonne Price en numérique si ce n'est pas déjà fait
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    # Identifier les colonnes qui définissent les caractéristiques uniques d'un produit
    key_columns = ['Model', 'RAM', 'CPU', 'Brand', 'Storage']
    df = df.sort_values(by='Price', ascending=True)
    df_cleaned = df.drop_duplicates(subset=key_columns, keep='first')

    return df_cleaned


df = remove_duplicates_keep_min_price(df)

output_path = r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\cleaned\ebay\cleaned_laptop1.csv'
df.to_csv(output_path, index=False)

print("Nettoyage terminé ! Fichier sauvegardé sous : laptops_clean_2025-01-28.csv")
print(f"Shape final : {df.shape}")