import pandas as pd
import numpy as np
import os

# Définir le chemin du fichier
file_path = r"/data/raw/ebay/laptops_results.csv"

# Vérifier si le fichier existe
if not os.path.exists(file_path):
    raise FileNotFoundError(
        f"Le fichier '{file_path}' est introuvable. Vérifiez le chemin ou l'emplacement du fichier.")

# Charger les données
data = pd.read_csv(file_path)


# --- 1. Suppression des variations inutiles dans les titres ---
def clean_title(title):
    # Retirer les mentions inutiles (couleur, taille, etc.)
    keywords_to_remove = ["Rouge", "Bleu", "Noir", "\"", "inch", "in.", "Edition", "Magic Keyboard"]
    for word in keywords_to_remove:
        title = title.replace(word, "")
    return title.strip()


data["Title"] = data["Title"].apply(clean_title)

# --- 2. Gestion des valeurs manquantes ---


# Imputation pour d'autres colonnes
columns_to_fill_mean = ["RAM", "Storage"]
for col in columns_to_fill_mean:
    if col in data.columns:
        data[col] = data[col].fillna(data[col].mode()[0] if data[col].dtype == 'O' else data[col].mean())

# --- 3. Suppression des doublons ---
data.drop_duplicates(inplace=True)


# --- 4. Standardisation des formats ---
def standardize_units(value):
    if isinstance(value, str):
        value = value.replace("GB", "").replace("TB", "000").strip()
    try:
        return int(value)
    except ValueError:
        return np.nan


data["RAM"] = data["RAM"].apply(standardize_units)
data["Storage"] = data["Storage"].apply(standardize_units)

# --- 5. Gestion des valeurs aberrantes ---
# Nettoyer la colonne 'Price' (retirer $, virgules et convertir en float)
data["Price"] = data["Price"].str.replace("$", "").str.replace(",", "").astype(float)

# Exclusion des prix excessivement bas/élevés (exemple arbitraire)


# --- 6. Variables indicatrices/dummies ---
if "Brand" in data.columns:
    data = pd.get_dummies(data, columns=["Brand"], prefix="Brand", drop_first=True)

# Sauvegarde des données nettoyées
output_path = r"/data/cleaned/ebay/cleaned_laptops_results.csv"
data.to_csv(output_path, index=False)

print(f"Nettoyage des données terminé. Fichier sauvegardé sous '{output_path}'.")