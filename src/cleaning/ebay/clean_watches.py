import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.impute import SimpleImputer
from fuzzywuzzy import process
from unidecode import unidecode
from pathlib import Path
import re

# Define paths using relative paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # Root folder of the project
RAW_DATA_DIR_EBAY = BASE_DIR / 'data' / 'raw' / 'ebay' / 'smart_watches'
CLEANED_DATA_DIR_EBAY = BASE_DIR / 'data' / 'cleaned' / 'ebay' / 'smart_watches'

# Ensure the cleaned data directories exist
CLEANED_DATA_DIR_EBAY.mkdir(parents=True, exist_ok=True)


# Debugging: Print paths
print(f"Base directory: {BASE_DIR}")
print(f"Raw data directory (eBay): {RAW_DATA_DIR_EBAY}")
print(f"Cleaned data directory (eBay): {CLEANED_DATA_DIR_EBAY}")

# Check if raw data directories exist
if not RAW_DATA_DIR_EBAY.exists():
    raise FileNotFoundError(f"Raw data directory not found: {RAW_DATA_DIR_EBAY}")

# Function to clean eBay smart watches data
def clean_smart_watches_ebay(df):
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
    def clean_case_size(case_size):
        if pd.isna(case_size) or case_size == 'Does not apply':
            return np.nan
        sizes = [float(s) for s in str(case_size).replace('mm', '').split(',') if s.strip().replace('.', '', 1).isdigit()]
        return round(np.mean(sizes), 2) if sizes else np.nan

    df['Case Size'] = df['Case Size'].apply(clean_case_size)
    case_size_imputer = SimpleImputer(strategy='mean')
    df['Case Size'] = case_size_imputer.fit_transform(df[['Case Size']])
    df['Case Size'] = df['Case Size'].round(2)

    # Function to clean Battery Capacity
    def clean_battery_capacity(capacity):
        if pd.isna(capacity):
            return np.nan
        capacity = str(capacity).replace('mAh', '').strip()
        try:
            return int(round(float(capacity)))
        except ValueError:
            return np.nan

    df['Battery Capacity'] = df['Battery Capacity'].apply(clean_battery_capacity)
    battery_capacity_imputer = SimpleImputer(strategy='mean')
    df['Battery Capacity'] = battery_capacity_imputer.fit_transform(df[['Battery Capacity']])
    df['Battery Capacity'] = df['Battery Capacity'].round(0).astype(int)

    # Function to clean Brand
    brands = ['Google', 'Apple', 'Samsung', 'Xiaomi', 'Fitbit', 'Garmin',
              'Apple', 'Samsung', 'Huawei', 'IOWODO', 'iPhone', 'Pebble', 'TOZO', 'Fossil', 'Amazfit', 'Ticwatch', 'Mobvoi',
              'Verizon', 'COLMI', 'AICase', 'Haylou', 'HUAWEI', 'Honor', 'Xiaomi', 'Garmin', 'Fitbit', 'Fossil',
              'Withings', 'Nothing', 'T-Mobile']

    def clean_brand(brand, title):
        if pd.isna(brand) or brand == 'Does not apply':
            for b in brands:
                if b.lower() in title.lower():
                    return b
            return 'Unknown'
        else:
            match = process.extractOne(unidecode(str(brand)), brands)
            return match[0] if match and match[1] >= 80 else 'Unknown'

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

    # Function to clean Title
    def clean_title(title):
        words_to_remove = [
            "smart watch", "smartwatch", "watch", "fitness tracker", "activity tracker", "sports watch", "wristwatch",
            "bluetooth", "gps", "wifi", "heart rate monitor", "blood pressure monitor", "waterproof", "ip67", "ip68",
            "touch screen", "phone function", "sos", "sleep monitor", "pedometer", "for men", "for women", "men's",
            "women's", "kids", "unisex", "new", "2024", "2025", "latest", "original", "brand new", "used", "mm", "gb",
            "mah", "android", "ios", "lte", "cellular", "wifi", "bluetooth",
            "amoled", "display", "screen", "flashlight", "compass", "military", "Women", "Good", "Very", "Modes", "Black", "White", "Silver",
            "Gold", "Blue", "Red", "Green", "Pink", "Purple", "Yellow", "Orange", "Brown", "Gray", "Grey", "Beige", "Ivory", "Cream", "Copper", "Bronze",
            "Coral", "Turquoise", "Aqua", "Lavender", "Lilac", "Indigo",
            "Maroon", "Olive", "Mint", "Teal", "Navy", "Apricot", "Azure", "Lime", "Violet", "Peach", "Plum", "Tan", "Khaki", "Crimson", "Magenta",
            "Salmon", "Charcoal", "Mauve", "Fuchsia", "Watches", "Watch", "Smart", "Smartwatch", "Fitness", "Tracker", "Activity", "Sports",
            "Wristwatch", "Bluetooth", "Gps", "Wifi", "Heart", "Rate", "Monitor", "Blood", "Pressure", "Waterproof", "Ip67", "Ip68", "Touch",
            "Screen", "Phone", "Function", "Sos", "Sleep", "Pedometer"
        ]
        for word in words_to_remove:
            title = re.sub(rf'\b{word}\b', '', title, flags=re.IGNORECASE)
        title = re.sub(r'1st Gen', 'I', title)
        title = re.sub(r'\b[A-Z0-9]{5,}\b', '', title)
        title = re.sub(r'\s+', ' ', title).strip()
        return title

    df['Title'] = df['Title'].apply(clean_title)

    # Verify if there are still any missing values
    print(df.isnull().sum())
    df = df[df.apply(lambda row: list(row).count('Unknown') < 2, axis=1)]

    # Function to remove duplicates
    def remove_duplicates(df, subset_columns, price_column='Price'):
        df = df.sort_values(by=price_column, ascending=True)
        df = df.drop_duplicates(subset=subset_columns, keep='first')
        return df

    subset_columns = ['Brand', 'Model', 'Storage Capacity', 'Case Size', 'Battery Capacity']
    df = remove_duplicates(df, subset_columns)

    return df



try:
    for file in RAW_DATA_DIR_EBAY.glob('*.csv'):
        print(f"Processing eBay file: {file}")
        df_ebay = pd.read_csv(file)
        cleaned_df_ebay = clean_smart_watches_ebay(df_ebay)
        output_filename = CLEANED_DATA_DIR_EBAY / f"{file.stem}_cleaned.csv"
        cleaned_df_ebay.to_csv(output_filename, index=False)
        print(f"Cleaned data saved to {output_filename}")
except Exception as e:
    print(f"An error occurred while processing eBay data: {e}")

