import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.impute import SimpleImputer
from fuzzywuzzy import process
from unidecode import unidecode

# Load the data
file_path = r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\ebay\smart_watches_2025-01-28.csv'
df = pd.read_csv(file_path)


# Function to clean Title
import re



# Function to clean Price
def clean_price(price):
    if pd.isna(price):
        return np.nan
    price = str(price).replace(',', '').replace('$', '').replace('GBP', '').replace('US', '').replace('/ea', '').replace('AU', '').replace('EUR', '').strip()
    try:
        return float(price)
    except ValueError:
        return np.nan

df['Price'] = df['Price'].apply(clean_price)
price_imputer = SimpleImputer(strategy='median')
df['Price'] = price_imputer.fit_transform(df[['Price']])

# Function to clean Case Size
# Fonction modifiée
# Fonction pour nettoyer et arrondir Case Size
def clean_case_size(case_size):
    if pd.isna(case_size) or case_size == 'Does not apply':
        return np.nan
    sizes = [float(s) for s in str(case_size).replace('mm', '').split(',') if s.strip().replace('.', '', 1).isdigit()]
    return round(np.mean(sizes), 2) if sizes else np.nan


# Application de la fonction
df['Case Size'] = df['Case Size'].apply(clean_case_size)

# Remplacement des NaN et arrondi final
case_size_imputer = SimpleImputer(strategy='mean')
df['Case Size'] = case_size_imputer.fit_transform(df[['Case Size']])
df['Case Size'] = df['Case Size'].round(2)  # Arrondi après imputations

# Fonction de nettoyage et conversion en entier
def clean_battery_capacity(capacity):
    if pd.isna(capacity):  # Vérifie les valeurs manquantes
        return np.nan
    capacity = str(capacity).replace('mAh', '').strip()  # Suppression de 'mAh'
    try:
        return int(round(float(capacity)))  # Conversion en float, arrondi puis entier
    except ValueError:
        return np.nan
df['Battery Capacity'] = df['Battery Capacity'].apply(clean_battery_capacity)

# Remplacement des valeurs manquantes (NaN) avec la moyenne et conversion en entier
battery_capacity_imputer = SimpleImputer(strategy='mean')  # Moyenne pour combler les NaN
df['Battery Capacity'] = battery_capacity_imputer.fit_transform(df[['Battery Capacity']])
df['Battery Capacity'] = df['Battery Capacity'].round(0).astype(int)

brands = ['Google', 'Apple', 'Samsung', 'Xiaomi', 'Fitbit', 'Garmin',
          'Apple' , 'Samsung','Huawei','IOWODO','iPhone', 'Pebble','TOZO','Fossil','Amazfit','Ticwatch','Mobvoi',
          'Verizon','COLMI','AICase','Haylou','HUAWEI','Honor','Xiaomi','Garmin','Fitbit','Fossil',
          'Withings', 'Nothing', 'T-Mobile']

def clean_brand(brand, title):

    if pd.isna(brand) or brand == 'Does not apply':  # Marque vide, invalide, ou non renseignée
        for b in brands:
            if b.lower() in title.lower():
                return b  # Trouve une marque dans le titre
        return 'Unknown'  # Si aucune correspondance
    else:
        # Sinon, nettoie et standardise la marque existante
        match = process.extractOne(unidecode(str(brand)), brands)
        return match[0] if match and match[1] >= 80 else 'Unknown'


# Application
df['Brand'] = df.apply(lambda row: clean_brand(row['Brand'], row['Title']), axis=1)


# Function to clean Model
models = list(df['Model'].dropna().unique())

def clean_model(model):
    if pd.isna(model) or model == 'Does not apply':
        return 'Unknown'
    match = process.extractOne(unidecode(str(model)), models)
    return match[0] if match[1] >= 80 else 'Unknown'

df['Model'] = df['Model'].apply(clean_model)

# Function to clean Operating System
os_list = ['Wear OS', 'Android Wear', 'Apple Watch OS', 'Tizen', 'LiteOS', 'Moto Watch OS', 'Pebble OS']

def clean_os(os):
    if pd.isna(os):
        return 'Unknown'
    match = process.extractOne(unidecode(str(os)), os_list)
    return match[0] if match[1] >= 80 else 'Unknown'

df['Operating System'] = df['Operating System'].apply(clean_os)

# Function to clean Storage Capacity
def clean_storage_capacity(storage):
    if pd.isna(storage):
        return np.nan
    storage = str(storage).replace('GB', '').replace('MB', '').strip()
    try:
        return float(storage) / 1024 if 'MB' in str(storage) else float(storage)
    except ValueError:
        return np.nan

df['Storage Capacity'] = df['Storage Capacity'].apply(clean_storage_capacity)
storage_capacity_imputer = SimpleImputer(strategy='median')
df['Storage Capacity'] = storage_capacity_imputer.fit_transform(df[['Storage Capacity']])

def clean_title(title):
    # Supprimer des mots inutiles ou non pertinents
    words_to_remove = [
        "smart watch", "smartwatch", "watch", "fitness tracker", "activity tracker", "sports watch", "wristwatch",
        "bluetooth", "gps", "wifi", "heart rate monitor", "blood pressure monitor", "waterproof", "ip67", "ip68",
        "touch screen", "phone function", "sos", "sleep monitor", "pedometer", "for men", "for women", "men's",
        "women's", "kids", "unisex", "new", "2024", "2025", "latest", "original", "brand new", "used", "mm", "gb",
        "mah", "android", "ios" "lte", "cellular", "wifi", "bluetooth",
        "amoled", "display", "screen", "flashlight", "compass", "military","Women","Good","Very","Modes","Black","White","Silver",
        "Gold","Blue","Red","Green","Pink","Purple","Yellow","Orange","Brown","Gray","Grey","Beige","Ivory","Cream","Copper","Bronze",
        "Coral","Turquoise","Aqua","Lavender","Lilac","Indigo",
        "Maroon","Olive","Mint","Teal","Navy","Apricot","Azure","Lime","Violet","Peach","Plum","Tan","Khaki","Crimson","Magenta",
        "Salmon","Charcoal","Mauve","Fuchsia","Watches","Watch","Smart","Smartwatch","Fitness","Tracker","Activity","Sports",
        "Wristwatch","Bluetooth","Gps","Wifi","Heart","Rate","Monitor","Blood","Pressure","Waterproof","Ip67","Ip68","Touch",
        "Screen","Phone","Function","Sos","Sleep","Pedometer"
    ]
    for word in words_to_remove:
        title = re.sub(rf'\b{word}\b', '', title, flags=re.IGNORECASE)

    # Normaliser les générations
    title = re.sub(r'1st Gen', 'I', title)

    # Supprimer les codes produits (séquences de 5 caractères et plus en majuscule/chiffres)
    title = re.sub(r'\b[A-Z0-9]{5,}\b', '', title)

    # Supprimer les espaces superflus
    title = re.sub(r'\s+', ' ', title).strip()

    return title


df['Title'] = df['Title'].apply(clean_title)

# Verify if there are still any missing values
print(df.isnull().sum())
df = df[df.apply(lambda row: list(row).count('Unknown') < 2, axis=1)]

output_path = r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\cleaned\ebay\cleaned_watch1.csv'
df.to_csv(output_path, index=False)

print("Data cleaning completed successfully.")