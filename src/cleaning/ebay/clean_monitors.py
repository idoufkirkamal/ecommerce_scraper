import re
import pandas as pd
import numpy as np

file_path = r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\ebay\gaming_monitors_2025-01-28.csv'
df = pd.read_csv(file_path)


# 1. Nettoyage de la colonne Price
def clean_price(price):
    if isinstance(price, str):
        # Supprimer les symboles monétaires et texte supplémentaire
        cleaned = ''.join(filter(lambda x: x.isdigit() or x == '.', price))
        return float(cleaned) if cleaned else np.nan
    return price


df['Price'] = df['Price'].apply(clean_price)


# 2. Uniformisation de Screen Size
def clean_screen_size(size):
    if isinstance(size, str):
        # Extraire les nombres avec gestion des formats variés
        size = size.replace('"', '').replace("''", "").replace("inches", "")
        matches = [s for s in size.split() if s.replace('.', '').isdigit()]
        return float(matches[0]) if matches else np.nan
    return size


df['Screen Size'] = df['Screen Size'].apply(clean_screen_size)
brand_avg_size = df.groupby('Brand')['Screen Size'].transform('median')
df['Screen Size'] = df['Screen Size'].fillna(brand_avg_size)
median_screen_size = df['Screen Size'].dropna().median()  # Already floats
df['Screen Size'] = df['Screen Size'].fillna(median_screen_size)  # Fill missing with median


# Nettoyer la colonne "Response Time"
def clean_response_time(response_time):
    if isinstance(response_time, str):
        # Extraire les parties numériques au début de la chaîne
        match = re.search(r'(\d+(\.\d*)?)', response_time)
        if match:
            return float(match.group(0))  # Convertir en float
        else:
            return np.nan  # Remettre à NaN si aucune partie numérique valide
    elif isinstance(response_time, (int, float)):
        # Si la valeur est déjà un nombre, on la conserve
        return float(response_time)
    return np.nan  # Pour tout cas imprévu


# Appliquer cette fonction à la colonne 'Response Time'
df['Response Time'] = df['Response Time'].apply(clean_response_time)
median_response_time = df['Response Time'].dropna().median()
df['Response Time'] = df['Response Time'].fillna(median_response_time)


def extract_refresh_rate(row):
    """Combine original data and title extraction with priority logic"""
    # (1) Try to clean existing value first
    original_value = row['Refresh Rate']
    if pd.notna(original_value):
        matches = re.findall(r'\d+', str(original_value))
        return max(map(int, matches)) if matches else None

    # (2) Fallback to title extraction
    title_matches = re.findall(r'(\d+)\s*Hz', row['Title'], flags=re.IGNORECASE)
    if title_matches:
        return max(map(int, title_matches))

    # (3) Final fallback for special cases
    if "HZ" in row['Title'].upper():  # Handle non-numeric mentions
        return int(re.search(r'(\d+)\s*HZ', row['Title'].upper()).group(1))

    return None


# Application
df['Refresh Rate'] = df.apply(extract_refresh_rate, axis=1)

new_columns = {
    'Screen Size': 'Screen_Size_in',
    'Maximum Resolution': 'Max_Resolution',
    'Aspect Ratio': 'Aspect_Ratio',
    'Refresh Rate': 'Refresh_Rate_Hz',
    'Response Time': 'Response_Time_ms'
}
df = df.rename(columns=new_columns)

def clean_title_advanced(title):
    # Supprimer les termes spécifiques avec variations
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


df['Title'] = df['Title'].apply(clean_title_advanced)

# Résultat

# 6. Suppression des doublons
df = df.drop_duplicates(subset=['Title', 'Brand', 'Model', 'Screen_Size_in'])
df = df.drop('Max_Resolution', axis=1)
df = df.dropna(thresh=df.shape[1] - 2)
# Export final
output_path = r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\cleaned\ebay\cleaned_monitor1.csv'
df.to_csv(output_path, index=False)