import re
import pandas as pd
import numpy as np


def clean_monitors_ebay(df):
    # 1. Nettoyage de la colonne Price
    if 'Brand' not in df.columns:
        print("⚠️ La colonne 'Brand' est absente. Une valeur par défaut sera utilisée.")
        df['Brand'] = 'Unknown'
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
    df = df.drop('Max_Resolution', axis=1)
    df = df.dropna(thresh=df.shape[1] - 2)
    # Suppression des doublons en gardant celui avec le prix minimum
    def remove_duplicates_with_min_price(dataframe, columns_to_check, price_column):

        dataframe = dataframe.sort_values(by=price_column, ascending=True)  # Trier par prix croissant
        dataframe = dataframe.drop_duplicates(subset=columns_to_check,keep='first')  # Conserver le premier (prix le plus bas)
        return dataframe


    # Appliquer la fonction sur le DataFrame
    columns_to_check_for_duplicates = ['Brand','Model', 'Screen_Size_in', 'Refresh_Rate_Hz', 'Response_Time_ms']
    df = remove_duplicates_with_min_price(df, columns_to_check_for_duplicates, 'Price')

    return df


file_path_ebay = r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\ebay\gaming_monitors_2025-01-28.csv'
df_ebay = pd.read_csv(file_path_ebay)
cleaned_df_ebay = clean_monitors_ebay(df_ebay)
# Export final
output_path_ebay = r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\cleaned\ebay\cleaned_monitor1.csv'
cleaned_df_ebay.to_csv(output_path_ebay, index=False)

def clean_monitor_flipkart(df):
    # Garder seulement les colonnes nécessaires
    def find_column(df, aliases):

        for alias in aliases:
            if alias in df.columns:
                return alias
        return None
    columns_aliases = {
        'title': ['title', 'name', 'product_title'],
        'price': ['price', 'cost', 'amount'],
        'Resolution': ['Resolution', 'Max Resolution', 'Display Resolution'],
        'Aspect Ratio': ['Aspect Ratio', 'View Ratio', 'Screen Ratio'],
        'Screen Size': ['Screen Size', 'Display', 'Standing screen display size'],
        'Model': ['Model', 'Model Name', 'Item Model Number'],
        'Refresh Rate': ['Refresh Rate', 'Max Refresh Rate', 'Recommended Uses For Product'],
        'Response Time': ['Response Time', 'Response Time (GTG)', 'Response Time (ms)']
    }

    # Identifier les colonnes disponibles en utilisant les alias
    selected_columns = {}
    for common_name, aliases in columns_aliases.items():
        column_name = find_column(df, aliases)
        if column_name:
            selected_columns[common_name] = column_name
        else:
            print(
                f"⚠️ Colonne manquante pour : '{common_name}'. Elle sera ignorée ou remplie avec des valeurs par défaut.")
            df[common_name] = None  # Ajouter une colonne manquante avec des valeurs NaN

    # Renommer les colonnes trouvées pour correspondre à des noms standard
    df = df.rename(columns=selected_columns)

    # Colonnes finales à conserver
    columns_to_keep = list(columns_aliases.keys())  # ['title', 'price', 'Resolution', 'Aspect Ratio']
    df = df[columns_to_keep]

    # 2. Nettoyage du prix (Price)
    conversion_rate_to_usd = 0.012
    df['Price'] = df['Price'].replace({'₹': '', ',': ''}, regex=True)  # Supprimer le symbole ₹ et les virgules
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')  # Convertir en numérique, NaN si non convertible
    df['Price (USD)'] = (df['Price'] * conversion_rate_to_usd).round(2)  #
    # 3. Nettoyage de la taille de l'écran (Screen Size)
    def extract_screen_size(text):
        match = re.search(r'(\d+\.?\d*)\s?(cm|inch)', str(text), re.IGNORECASE)
        if match:
            size = float(match.group(1))
            unit = match.group(2).lower()
            return size   # Convertir cm en inch
        return None

    df['Screen Size'] = df['Screen Size'].apply(extract_screen_size)

    # 4. Nettoyage de la résolution maximale (Maximum Resolution)
    def clean_resolution(text):
        match = re.search(r'(\d+)\s?[xX*]\s?(\d+)', str(text))
        if match:
            return f"{match.group(1)}x{match.group(2)}"
        return None

    df['Maximum Resolution'] = df['Maximum Resolution'].apply(clean_resolution)



    # 7. Nettoyage du modèle (Model)
    df['Model'] = df['Model'].str.strip()
    df['Model'] = df['Model'].apply(lambda x: re.sub(r'\s+', ' ', str(x)))

    # 8. Nettoyage du taux de rafraîchissement maximal (Maximum Refresh Rate)
    def clean_refresh_rate(text):
        match = re.search(r'(\d+)\s?Hz', str(text), re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None

    df['Refresh Rate'] = df['Refresh Rate'].apply(clean_refresh_rate)

    cleand_ebay = clean_monitors_ebay(df)


    return df


# Charger les données depuis le fichier CSV
file_path_flipkart = r"C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\flipkart\monitors_2025_01_29_scrape1.csv"
df_flipkart = pd.read_csv(file_path_flipkart)

cleaned_df = clean_monitor_flipkart(df_flipkart)
cleaned_df.to_csv(r"C:\Users\AdMin\Desktop\ecommerce_scraper\data\cleaned\flipkart\cleaned_monitor1.csv", index=False)
# Afficher les premières lignes du DataFrame nettoyé
print(cleaned_df.head())

file_path_ubuy = r"C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\ubuy\monitors_2025_01_29_scrape1.csv"
df_ubuy = pd.read_csv(file_path_ubuy)

cleaned_df = clean_monitor_flipkart(df_ubuy)
cleaned_df.to_csv(r"C:\Users\AdMin\Desktop\ecommerce_scraper\data\cleaned\ubuy\cleaned_monitor1.csv", index=False)
# Afficher les premières lignes du DataFrame nettoyé
print(cleaned_df.head())