import os
import pandas as pd
import numpy as np
import re
from src.cleaning.ebay.clean_monitors import clean_monitors_ebay  # Importer la fonction commune si nécessaire

# Définir les chemins relatifs pour eBay
base_dir = os.path.dirname(os.path.abspath(__file__))
raw_data_dir_ebay = os.path.join(base_dir, '..', '..', '..', 'data', 'raw', 'ebay', 'monitors')
cleaned_data_dir_ebay = os.path.join(base_dir, '..', '..', '..', 'data', 'cleaned', 'ebay', 'monitors')

# Définir les chemins relatifs pour Flipkart
raw_data_dir_flipkart = os.path.join(base_dir, '..', '..', '..', 'data', 'raw', 'flipkart', 'monitors')
cleaned_data_dir_flipkart = os.path.join(base_dir, '..', '..', '..', 'data', 'cleaned', 'flipkart', 'monitors')

# Créer les dossiers de sortie s'ils n'existent pas
os.makedirs(cleaned_data_dir_ebay, exist_ok=True)
os.makedirs(cleaned_data_dir_flipkart, exist_ok=True)


# Fonction pour nettoyer les moniteurs Flipkart

# Fonction pour nettoyer les moniteurs Flipkart
def clean_monitor_flipkart(df):
    def find_column(df, aliases):
        """Trouve la colonne correspondant à un des alias donnés."""
        for alias in aliases:
            if alias in df.columns:
                return alias
        return None

    # Dictionnaire des alias pour les colonnes cibles
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

    # Renommer les colonnes selon les alias trouvés
    selected_columns = {}
    for common_name, aliases in columns_aliases.items():
        column_name = find_column(df, aliases)
        if column_name:
            selected_columns[common_name] = column_name
        else:
            print(f"⚠️ Colonne manquante pour : '{common_name}'. Elle sera ignorée ou remplie avec des valeurs par défaut.")
            df[common_name] = None  # Remplir avec des valeurs nulles pour les manquants

    # Appliquer le renommage des colonnes
    df = df.rename(columns=selected_columns)

    # Valider les colonnes nécessaires avant de tenter de filtrer
    columns_to_keep = list(columns_aliases.keys())
    missing_columns = [col for col in columns_to_keep if col not in df.columns]

    if missing_columns:
        print(f"⚠️ Les colonnes suivantes sont manquantes et seront ajoutées avec des valeurs par défaut : {missing_columns}")
        for col in missing_columns:
            df[col] = None  # Ajouter les colonnes manquantes avec des valeurs par défaut

    # Garder uniquement les colonnes cibles
    df = df[columns_to_keep]

    # Nettoyage des colonnes spécifiques
    conversion_rate_to_usd = 0.012
    df['Price'] = df['Price'].replace({'₹': '', ',': ''}, regex=True)
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['Price (USD)'] = (df['Price'] * conversion_rate_to_usd).round(2)

    # Nettoyage de la taille de l'écran
    def extract_screen_size(text):
        match = re.search(r'(\d+\.?\d*)\s?(cm|inch)', str(text), re.IGNORECASE)
        if match:
            size = float(match.group(1))
            unit = match.group(2).lower()
            if unit == 'cm':
                size /= 2.54  # Convertir cm en inch
            return size
        return None

    df['Screen Size'] = df['Screen Size'].apply(extract_screen_size)

    # Nettoyage de la résolution maximale
    def clean_resolution(text):
        match = re.search(r'(\d+)\s?[xX*]\s?(\d+)', str(text))
        if match:
            return f"{match.group(1)}x{match.group(2)}"
        return None

    df['Resolution'] = df['Resolution'].apply(clean_resolution)

    # Nettoyage du modèle
    df['Model'] = df['Model'].str.strip()
    df['Model'] = df['Model'].apply(lambda x: re.sub(r'\s+', ' ', str(x)))

    # Nettoyage du taux de rafraîchissement maximal
    def clean_refresh_rate(text):
        match = re.search(r'(\d+)\s?Hz', str(text), re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None

    df['Refresh Rate'] = df['Refresh Rate'].apply(clean_refresh_rate)

    # Utiliser la fonction commune d'eBay si nécessaire
    cleaned_df = clean_monitors_ebay(df)
    return cleaned_df


### **Corrections principales :**



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

# Traiter les fichiers Flipkart
for filename in os.listdir(raw_data_dir_flipkart):
    if filename.endswith('.csv'):
        file_path = os.path.join(raw_data_dir_flipkart, filename)
        df = pd.read_csv(file_path)
        cleaned_df = clean_monitor_flipkart(df)
        cleaned_filename = f"cleaned_{filename}"
        cleaned_file_path = os.path.join(cleaned_data_dir_flipkart, cleaned_filename)
        cleaned_df.to_csv(cleaned_file_path, index=False)
        print(f"Fichier nettoyé (Flipkart) : {cleaned_file_path}")