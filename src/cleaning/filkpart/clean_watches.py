#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import re
from pathlib import Path
from fuzzywuzzy import fuzz
from unidecode import unidecode

# =============================================================================
# Définition des répertoires de travail
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # Racine du projet
RAW_DATA_DIR_EBAY = BASE_DIR / 'data' / 'raw' / 'flipkart' / 'smart_watches'
CLEANED_DATA_DIR_EBAY = BASE_DIR / 'data' / 'cleaned' / 'flipkart' / 'smart_watches'

# Créer le dossier de données nettoyées s'il n'existe pas
CLEANED_DATA_DIR_EBAY.mkdir(parents=True, exist_ok=True)

# =============================================================================
# Fonctions utilitaires de nettoyage
# =============================================================================

def clean_price(price, conversion_rate=0.0125):
    """
    Convertit une chaîne représentant un prix en une valeur numérique en dollars.
    Si le prix commence par "₹", le symbole est retiré et le montant converti.
    """
    if isinstance(price, str):
        price = price.strip().replace(" ", "")
        if price.startswith("₹"):
            price_num = price.replace("₹", "").replace(",", "")
            try:
                price_num = float(price_num) * conversion_rate
            except Exception:
                price_num = np.nan
        elif price.startswith("$"):
            price_num = price.replace("$", "").replace(",", "")
            try:
                price_num = float(price_num)
            except Exception:
                price_num = np.nan
        else:
            try:
                price_num = float(price)
            except Exception:
                price_num = np.nan
        return price_num
    return price

def extract_case_size(title):
    """
    Extrait la taille du boitier (par exemple "40mm") depuis le titre.
    Retourne un float ou np.nan.
    """
    if isinstance(title, str):
        match = re.search(r'(\d+(?:\.\d+)?)\s*mm', title, re.IGNORECASE)
        if match:
            return float(match.group(1))
    return np.nan

def extract_brand(title, brand_list):
    """
    Parcourt la liste des marques et retourne la première présente dans le titre.
    Sinon, retourne "Unknown".
    """
    if isinstance(title, str):
        for brand in brand_list:
            if brand.lower() in title.lower():
                return brand
    return "Unknown"

def extract_storage(memory_str):
    """
    Extrait la première valeur numérique depuis une chaîne (exemple: "16 GB" → 16.0).
    """
    if isinstance(memory_str, str):
        match = re.search(r'(\d+)', memory_str)
        if match:
            return float(match.group(1))
    return np.nan

# =============================================================================
# Fonction de traitement d'un fichier CSV
# =============================================================================

def process_file(file_path: Path) -> pd.DataFrame:
    """
    Charge le CSV, normalise les colonnes et effectue le nettoyage :
      - Conversion du prix
      - Extraction de la taille du boitier depuis le titre
      - Extraction de la marque depuis le titre à partir d'une liste connue
      - Extraction du modèle, du système d'exploitation, du stockage, etc.
    Renvoie un DataFrame final avec les colonnes cibles.
    """
    # Charger le CSV en s'assurant de lire tous les types correctement
    df = pd.read_csv(file_path, encoding="utf-8", low_memory=False)

    # Normalisation des noms de colonnes : suppression des espaces et passage en minuscules
    df.columns = df.columns.str.strip().str.lower()
    df = df.copy()  # Pour défragmenter le DataFrame

    # Définir la liste des marques connues
    known_brands = ["Samsung", "Fastrack", "Yash Enterprises", "Apple", "AICase"]

    # --- Traitement du prix ---
    if 'price' in df.columns:
        df['price'] = df['price'].apply(lambda x: clean_price(x, conversion_rate=0.0125))
    else:
        print(f"Attention : la colonne 'price' n'est pas présente dans {file_path}")

    # --- Traitement du titre et extraction d'informations depuis le titre ---
    if 'title' in df.columns:
        # Normaliser le texte : suppression des accents et espaces inutiles
        df['title'] = df['title'].apply(lambda x: unidecode(x) if isinstance(x, str) else x)
        df['title'] = df['title'].str.strip()
        df['case size'] = df['title'].apply(extract_case_size)
        df['brand'] = df['title'].apply(lambda x: extract_brand(x, known_brands))
    else:
        df['case size'] = np.nan
        df['brand'] = "Unknown"

    # --- Extraction du modèle ---
    if 'model name' in df.columns:
        df['model'] = df['model name']
    elif 'model' in df.columns:
        df['model'] = df['model']
    else:
        df['model'] = np.nan

    # --- Système d'exploitation ---
    if 'operating system' not in df.columns:
        df['operating system'] = np.nan

    # --- Capacité de stockage ---
    if 'internal memory' in df.columns:
        df['storage capacity'] = df['internal memory'].apply(extract_storage)
    else:
        df['storage capacity'] = np.nan

    # --- Batterie ---
    df['battery capacity'] = np.nan  # Pas d'information de batterie dans le CSV d'origine

    # --- Date de collecte ---
    if 'collection date' in df.columns:
        df['collection date'] = pd.to_datetime(df['collection date'], errors='coerce')
    elif 'collection_date' in df.columns:
        df['collection date'] = pd.to_datetime(df['collection_date'], errors='coerce')
    else:
        df['collection date'] = pd.NaT

    # =============================================================================
    # Constitution du DataFrame final avec les colonnes cibles
    # =============================================================================

    final_df = pd.DataFrame({
        "Title": df['title'] if 'title' in df.columns else np.nan,
        "Price": df['price'] if 'price' in df.columns else np.nan,
        "Case Size": df['case size'],
        "Battery Capacity": df['battery capacity'],
        "Brand": df['brand'],
        "Model": df['model'],
        "Operating System": df['operating system'],
        "Storage Capacity": df['storage capacity'],
        "Collection Date": df['collection date']
    })

    # --- Imputation des valeurs manquantes ---

    # Pour les colonnes numériques, imputer avec la médiane
    numeric_cols = ["Price", "Case Size", "Battery Capacity", "Storage Capacity"]
    for col in numeric_cols:
        if not final_df[col].isnull().all():
            final_df[col] = final_df[col].fillna(final_df[col].median())

    # Pour les colonnes catégorielles, imputer avec le mode
    categorical_cols = ["Title", "Brand", "Model", "Operating System", "Collection Date"]
    for col in categorical_cols:
        if not final_df[col].isnull().all():
            final_df[col] = final_df[col].fillna(final_df[col].mode()[0])

    # Suppression des doublons
    final_df.drop_duplicates(inplace=True)

    return final_df

# =============================================================================
# Fonction principale : traitement de tous les CSV du dossier raw
# =============================================================================

def main():
    # Parcourir tous les fichiers CSV dans le dossier RAW_DATA_DIR_EBAY
    csv_files = list(RAW_DATA_DIR_EBAY.glob("*.csv"))
    if not csv_files:
        print("Aucun fichier CSV trouvé dans", RAW_DATA_DIR_EBAY)
        return

    for file_path in csv_files:
        print(f"Traitement de {file_path} ...")
        try:
            cleaned_df = process_file(file_path)
            # Enregistrer le fichier nettoyé dans le dossier CLEANED_DATA_DIR_EBAY
            output_file = CLEANED_DATA_DIR_EBAY / file_path.name
            cleaned_df.to_csv(output_file, index=False, encoding="utf-8")
            print(f"Fichier nettoyé enregistré sous {output_file}\n")
        except Exception as e:
            print(f"Erreur lors du traitement de {file_path} : {e}")

if __name__ == "__main__":
    main()
