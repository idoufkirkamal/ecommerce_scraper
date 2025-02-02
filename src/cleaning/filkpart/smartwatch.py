
import pandas as pd
import re


def clean_smartwatch_data(file_path):
    # Charger les données
    df = pd.read_csv(file_path, encoding='utf-8')

    # Remplacer les valeurs manquantes
    df.replace(['Data not available', 'Image not available', 'URL not available'], pd.NA, inplace=True)

    # Colonnes attendues
    expected_columns = ['title', 'price', 'Display Size', 'Battery Life', 'Brand', 'Model Name',
                        'Compatible OS', 'Internal Memory']

    # Vérifier la présence des colonnes nécessaires
    available_columns = [col for col in expected_columns if col in df.columns]

    # Alerte en cas de colonnes manquantes
    missing_columns = set(expected_columns) - set(available_columns)
    if missing_columns:
        print(f"Attention : Les colonnes suivantes sont manquantes dans le fichier CSV : {missing_columns}")

    # Sélectionner uniquement les colonnes disponibles
    df = df[available_columns]

    # Renommer les colonnes si elles existent
    rename_mapping = {
        'title': 'Title',
        'price': 'Price',
        'Display Size': 'Case Size',
        'Battery Life': 'Battery Capacity',
        'Brand': 'Brand',
        'Model Name': 'Model',
        'Compatible OS': 'Operating System',
        'Internal Memory': 'Storage Capacity'
    }
    df.rename(columns={col: rename_mapping[col] for col in available_columns if col in rename_mapping}, inplace=True)

    # Fonctions d'extraction
    def extract_brand(title):
        if pd.isna(title): return pd.NA
        brand = title.split()[0].split(',')[0].strip()
        return ''.join(e for e in brand if e.isalnum() or e in ['&', ' '])

    def extract_model(title, brand):
        if pd.isna(title) or pd.isna(brand): return pd.NA
        model = title.replace(brand, '', 1).strip()
        return model.split(',')[0].split('|')[0].strip()

    def extract_case_size(title):
        if pd.isna(title): return pd.NA
        matches = re.findall(r'(\d+\.?\d*)\s?(?:mm|in(?:ch)?|")', title, flags=re.IGNORECASE)
        return f"{matches[0]}mm" if matches else pd.NA

    def extract_battery(title):
        if pd.isna(title): return pd.NA
        match = re.search(r'(\d+)\s?(?:mAh|mah|MAH)', title, re.IGNORECASE)
        return f"{match.group(1)}mAh" if match else pd.NA

    def extract_os(title):
        if pd.isna(title): return pd.NA
        if 'android' in title.lower() and 'ios' in title.lower():
            return 'Android, iOS'
        elif 'android' in title.lower():
            return 'Android'
        elif 'ios' in title.lower():
            return 'iOS'
        return pd.NA

    def extract_storage(title):
        if pd.isna(title): return pd.NA
        match = re.search(r'(\d+)\s?(?:GB|gb)', title, re.IGNORECASE)
        return f"{match.group(1)}GB" if match else pd.NA

    # Nettoyage des colonnes
    if 'Brand' in df.columns:
        df['Brand'] = df['Brand'].fillna(df['Title'].apply(extract_brand))
    if 'Model' in df.columns and 'Brand' in df.columns:
        df['Model'] = df.apply(lambda x: x['Model'] if pd.notna(x['Model'])
        else extract_model(x['Title'], x['Brand']), axis=1)
    if 'Case Size' in df.columns:
        df['Case Size'] = df['Case Size'].fillna(df['Title'].apply(extract_case_size))
    if 'Battery Capacity' in df.columns:
        df['Battery Capacity'] = df['Battery Capacity'].fillna(df['Title'].apply(extract_battery))
    if 'Operating System' in df.columns:
        df['Operating System'] = df['Operating System'].fillna(df['Title'].apply(extract_os))
    if 'Storage Capacity' in df.columns:
        df['Storage Capacity'] = df['Storage Capacity'].fillna(df['Title'].apply(extract_storage))

    # Nettoyage du prix
    if 'Price' in df.columns:
        df['Price'] = df['Price'].str.replace('₹', '', regex=False).str.replace(',', '', regex=False)
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        df.dropna(subset=['Price'], inplace=True)
    if 'Storage Capacity' in df.columns:
        # Extraire les valeurs numériques uniquement
        df['Storage Capacity'] = df['Storage Capacity'].str.extract(r'(\d+)').astype(float)
    # Nettoyage de la taille du boîtier (Case Size) et conversion en numérique
    if 'Case Size' in df.columns:
        # Extraire uniquement les valeurs numériques
        df['Case Size'] = df['Case Size'].str.extract(r'(\d+\.?\d*)').astype(float)

    # Conversion de la colonne 'Price' (INR vers USD)
    if 'Price' in df.columns:
        conversion_rate = 0.012  # Taux de conversion actuel (vous pouvez le mettre à jour selon le taux du jour)
        df['Price (USD)'] = df['Price'] * conversion_rate
    # Supprimer les lignes vides
    df.dropna(subset=['Title', 'Price'], how='all', inplace=True)

    return df


# Utilisation
cleaned_df = clean_smartwatch_data(
    r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\flipkart\smart_watches\smart_watches_2025_01_29_scrape1.csv')
cleaned_df.to_csv('cleaned_smart_watches.csv', index=False)
print("Nettoyage terminé ! Données sauvegardées dans cleaned_smart_watches.csv")
