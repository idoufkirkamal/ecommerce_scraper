import pandas as pd
import re


def clean_data(df):
    # Garder uniquement les colonnes nécessaires
    cols_to_keep = ['title', 'price', 'Standing screen display size',
                    'Aspect Ratio', 'Brand', 'Item model number']
    df = df[cols_to_keep]

    # Renommer les colonnes
    df.columns = ['Title', 'Price', 'Screen_Size_in', 'Aspect_Ratio',
                  'Brand', 'Model']

    # Fonction de nettoyage pour chaque colonne
    def clean_title(title):

        terms_to_remove = [
            r'\bGaming Monitor\b',
            r'\b\d+\.?\d*\s*Hz\b',  # "144Hz", "60 Hz"
            r'\b\d+\.?\d*\s*ms\b',  # "1ms", "5 ms"
            r'\b(HDR\d*|IPS|VA|TN|OLED)\b',  # "HDR400", "IPS"
            r'\b(Curved|Flat|UltraWide)\b'  # Exemple de termes optionnels à conserver
        ]

        # Supprimer les tailles d'écran (ex: 27", 24-inch)
        title = re.sub(r'\b\d+\.?\d*\s*(inch|inches|"|’’|’)\b', '', title, flags=re.IGNORECASE)

        # Supprimer les résolutions (ex: 2560x1440, FHD, WQHD)
        title = re.sub(r'\b(\d{3,4}[x×]\d{3,4}|FHD|HD|UHD|QHD|WQHD|4K)\b', '', title, flags=re.IGNORECASE)

        # Supprimer tous les termes techniques listés
        for pattern in terms_to_remove:
            title = re.sub(pattern, '', title, flags=re.IGNORECASE)

        # Nettoyage final : caractères spéciaux et espaces
        title = re.sub(r'[^\w\s.-]', '', title)  # Garder les tirets/points
        title = re.sub(r'\s+', ' ', title).strip()

        return title

    def extract_price(price):
        match = re.search(r'(\d+\.?\d*)', str(price).replace(' ', ''))
        return float(match.group(1)) if match else None

    def convert_to_usd(price_mad, exchange_rate=10):
        if price_mad is None:
            return None
        return round(price_mad / exchange_rate, 2)
    def extract_screen_size(size):
        match = re.search(r'(\d+\.?\d*)[" ]*Inch', str(size), re.IGNORECASE)
        return float(match.group(1)) if match else None

    def clean_aspect_ratio(ratio):
        if pd.isna(ratio): return None
        return ratio.split('/')[0].strip() if '/' in ratio else ratio

    def extract_refresh_rate(title):
        match = re.search(r'(\d+\.?\d*)[hH][zZ]', str(title))
        return float(match.group(1)) if match else None

    def extract_response_time(title):
        match = re.search(r'(\d+\.?\d*)\s*ms', str(title))
        return float(match.group(1)) if match else None

    def clean_brand(brand):
        brand = str(brand).strip().title()
        return re.sub(r'[^\w\s-]', '', brand) if not pd.isna(brand) else None

    def clean_model(model):
        return re.sub(r'[^\w\s-]', '', str(model)).strip() if not pd.isna(model) else 'Unknown'

    # Application des fonctions de nettoyage
    df['Title'] = df['Title'].apply(clean_title)
    df['Price'] = df['Price'].apply(extract_price)
    df['Price'] = df['Price'].apply(convert_to_usd)
    df['Screen_Size_in'] = df['Screen_Size_in'].apply(extract_screen_size)
    df['Aspect_Ratio'] = df['Aspect_Ratio'].apply(clean_aspect_ratio)
    df['Refresh_Rate_Hz'] = df['Title'].apply(extract_refresh_rate)
    df['Response_Time_ms'] = df['Title'].apply(extract_response_time)
    df['Brand'] = df['Brand'].apply(clean_brand)
    df['Model'] = df['Model'].apply(clean_model)

    # Suppression de la colonne Response_Time_ms
    df = df.drop(columns=['Response_Time_ms'])

    # Gestion des marques manquantes
    df['Brand'] = df.apply(lambda x: x['Brand'] or re.search(r'^([A-Z][a-z]+)', x['Title']).group(1), axis=1)

    # Réorganisation des colonnes
    final_columns = ['Title', 'Price', 'Screen_Size_in', 'Aspect_Ratio',
                     'Refresh_Rate_Hz', 'Brand', 'Model']

    return df[final_columns].dropna(subset=['Price', 'Screen_Size_in'])

# Chargement et nettoyage des données
df = pd.read_csv(r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\ubuy\monitors\monitors_2025_01_30_scrape1.csv')
cleaned_df = clean_data(df)

# Vérification des résultats
print(cleaned_df.head())
print(f"\nNombre total d'entrées après nettoyage : {len(cleaned_df)}")
print("\nValeurs manquantes par colonne :")
print(cleaned_df.isnull().sum())

# Sauvegarde
cleaned_df.to_csv('cleaned_monitors.csv', index=False)