import pandas as pd
import re


def clean_monitor_flipkart(df):
    # Garder seulement les colonnes nécessaires
    columns_to_keep = ['title', 'price', 'Display', 'Resolution', 'Aspect Ratio',
                       'Response Time', 'Model Name', 'Maximum Refresh Rate']
    df = df[columns_to_keep]

    # Renommer les colonnes pour correspondre aux noms demandés
    df.columns = ['Title', 'Price', 'Screen Size', 'Maximum Resolution',
                  'Aspect Ratio', 'Response Time', 'Model', 'Refresh Rate']


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




    return df


# Charger les données depuis le fichier CSV
file_path_flipkart = "9579ceae-3e77-4ab3-bd31-0c1775c217b4_monitors_2025_01_29_scrape1.csv"
df = pd.read_csv(file_path_flipkart)

cleaned_df = clean_monitor_flipkart(df)

# Afficher les premières lignes du DataFrame nettoyé
print(cleaned_df.head())