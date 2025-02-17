#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import re
from pathlib import Path
from unidecode import unidecode
from fuzzywuzzy import fuzz  # Si vous souhaitez étendre l'extraction de marque

# =============================================================================
# Définition des répertoires
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # Racine du projet
RAW_DATA_DIR_MONITORS = BASE_DIR / 'data' / 'raw' / 'flipkart' / 'monitors'
CLEANED_DATA_DIR_MONITORS = BASE_DIR / 'data' / 'cleaned' / 'flipkart' / 'monitors'
CLEANED_DATA_DIR_MONITORS.mkdir(parents=True, exist_ok=True)


# =============================================================================
# Fonctions utilitaires de nettoyage
# =============================================================================

def clean_price(price, conversion_rate=0.0125):

    if isinstance(price, str):
        price = price.strip().replace(" ", "")
        if price.startswith("₹"):
            try:
                return float(price.replace("₹", "").replace(",", "")) * conversion_rate
            except Exception:
                return np.nan
        elif price.startswith("$"):
            try:
                return float(price.replace("$", "").replace(",", ""))
            except Exception:
                return np.nan
        else:
            try:
                return float(price)
            except Exception:
                return np.nan
    return price


def extract_screen_size_in(title):
    """
    Extrait la taille de l'écran en pouces à partir du titre.
    Par exemple, dans "50.8 cm (20 inch)" la fonction retourne 20.0.
    """
    if isinstance(title, str):
        match = re.search(r'\((\d+(?:\.\d+)?)\s*(?:in|inch)', title, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1))
            except Exception:
                return np.nan
    return np.nan


def extract_aspect_ratio(title):
    """
    Si la colonne "aspect ratio" n'existe pas dans le CSV, tente d'extraire une valeur
    du type "16:9" depuis le titre.
    """
    if isinstance(title, str):
        match = re.search(r'(\d+:\d+)', title)
        if match:
            return match.group(1)
    return np.nan


def extract_refresh_rate(text):

    if isinstance(text, str):
        match = re.search(r'(\d+(?:\.\d+)?)', text)
        if match:
            try:
                return float(match.group(1))
            except Exception:
                return np.nan
    return np.nan


def extract_response_time(text):
    """
    Extrait la première valeur numérique d'une chaîne (par exemple "5 ms" → 5.0).
    """
    if isinstance(text, str):
        match = re.search(r'(\d+(?:\.\d+)?)', text)
        if match:
            try:
                return float(match.group(1))
            except Exception:
                return np.nan
    return np.nan


def extract_brand(title, brand_list):
    """
    Retourne la première marque présente dans le titre à partir d'une liste connue.
    Sinon, retourne "Unknown".
    """
    if isinstance(title, str):
        for brand in brand_list:
            if brand.lower() in title.lower():
                return brand
    return "Unknown"


# =============================================================================
# Fonction de traitement d'un fichier CSV (moniteurs)
# =============================================================================

def process_file(file_path: Path) -> pd.DataFrame:
    """
    Traite le fichier CSV brut et reconstruit un DataFrame ne conservant que :
      - Title
      - Price
      - Screen_Size_in
      - Aspect_Ratio
      - Refresh_Rate_Hz
      - Response_Time_ms
      - Brand
      - Model
      - Collection Date
    """
    # Charger le fichier CSV
    df = pd.read_csv(file_path, encoding="utf-8", low_memory=False)

    # Normaliser les noms de colonnes (mettre en minuscules et supprimer les espaces superflus)
    df.columns = df.columns.str.strip().str.lower()
    df = df.copy()  # Pour éviter la fragmentation

    # Définir la liste des marques connues pour les moniteurs
    known_brands = ["HP", "Lenovo", "Sceptre", "Acer", "ASUS", "Samsung", "Frontech", "ZEBRONICS"]

    # --- Traitement du titre ---
    if 'title' in df.columns:
        df['title'] = df['title'].apply(lambda x: unidecode(x) if isinstance(x, str) else x)
        df['title'] = df['title'].str.strip()
    else:
        df['title'] = np.nan

    # --- Traitement du prix ---
    if 'price' in df.columns:
        df['price'] = df['price'].apply(lambda x: clean_price(x, conversion_rate=0.0125))
    else:
        df['price'] = np.nan

    # --- Extraction de la taille de l'écran en pouces ---
    # Si la colonne "screen_size_in" existe déjà, on la convertit en numérique ; sinon, on l'extrait depuis le titre
    if 'screen_size_in' in df.columns:
        df['screen_size_in'] = pd.to_numeric(df['screen_size_in'], errors='coerce')
    else:
        df['screen_size_in'] = df['title'].apply(extract_screen_size_in)

    # --- Aspect Ratio ---
    # Si la colonne "aspect ratio" existe, on la garde, sinon on tente de l'extraire depuis le titre
    if 'aspect ratio' in df.columns:
        df['aspect_ratio'] = df['aspect ratio']
    else:
        df['aspect_ratio'] = df['title'].apply(extract_aspect_ratio)

    # --- Refresh Rate (Hz) ---
    # On recherche la colonne "maximum refresh rate" (si présente) ou "refresh_rate_hz"
    if 'maximum refresh rate' in df.columns:
        df['refresh_rate_hz'] = df['maximum refresh rate'].apply(extract_refresh_rate)
    elif 'refresh_rate_hz' in df.columns:
        df['refresh_rate_hz'] = df['refresh_rate_hz'].apply(extract_refresh_rate)
    else:
        df['refresh_rate_hz'] = np.nan

    # --- Response Time (ms) ---
    if 'response time' in df.columns:
        df['response_time_ms'] = df['response time'].apply(extract_response_time)
    else:
        df['response_time_ms'] = np.nan

    # --- Brand ---
    # Si la colonne "brand" existe, on l'utilise ; sinon, on l'extrait depuis le titre
    if 'brand' in df.columns:
        df['brand'] = df['brand'].fillna(df['title'].apply(lambda x: extract_brand(x, known_brands)))
    else:
        df['brand'] = df['title'].apply(lambda x: extract_brand(x, known_brands))

    # --- Model ---
    if 'model name' in df.columns:
        df['model'] = df['model name']
    elif 'model' in df.columns:
        df['model'] = df['model']
    else:
        df['model'] = np.nan

    # --- Collection Date ---
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
        "Title": df['title'],
        "Price": df['price'],
        "Screen_Size_in": df['screen_size_in'],
        "Aspect_Ratio": df['aspect_ratio'],
        "Refresh_Rate_Hz": df['refresh_rate_hz'],
        "Response_Time_ms": df['response_time_ms'],
        "Brand": df['brand'],
        "Model": df['model'],
        "Collection Date": df['collection date']
    })

    # --- Imputation des valeurs manquantes ---
    # Pour les colonnes numériques, utiliser la médiane
    numeric_cols = ["Price", "Screen_Size_in", "Refresh_Rate_Hz", "Response_Time_ms"]
    for col in numeric_cols:
        if not final_df[col].isnull().all():
            final_df[col] = final_df[col].fillna(final_df[col].median())

    # Pour les colonnes catégorielles, utiliser le mode
    categorical_cols = ["Title", "Aspect_Ratio", "Brand", "Model", "Collection Date"]
    for col in categorical_cols:
        if not final_df[col].isnull().all():
            try:
                final_df[col] = final_df[col].fillna(final_df[col].mode()[0])
            except Exception:
                pass

    # Suppression des doublons
    final_df.drop_duplicates(inplace=True)

    return final_df


# =============================================================================
# Traitement de tous les fichiers CSV du dossier raw pour les moniteurs
# =============================================================================

def main():
    csv_files = list(RAW_DATA_DIR_MONITORS.glob("*.csv"))
    if not csv_files:
        print("Aucun fichier CSV trouvé dans", RAW_DATA_DIR_MONITORS)
        return

    for file_path in csv_files:
        print(f"Traitement de {file_path} ...")
        try:
            cleaned_df = process_file(file_path)
            output_file = CLEANED_DATA_DIR_MONITORS / file_path.name
            cleaned_df.to_csv(output_file, index=False, encoding="utf-8")
            print(f"Fichier nettoyé enregistré sous {output_file}\n")
        except Exception as e:
            print(f"Erreur lors du traitement de {file_path} : {e}")


if __name__ == "__main__":
    main()
