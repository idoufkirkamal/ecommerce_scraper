import pandas as pd
import re
from pathlib import Path

# Define paths using relative paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # Root folder of the project
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw' / 'ubuy' / 'laptops'
CLEANED_DATA_DIR = BASE_DIR / 'data' / 'cleaned' / 'ubuy' / 'laptops'

# Ensure the cleaned data directory exists
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Debugging: Print paths
print(f"Base directory: {BASE_DIR}")
print(f"Raw data directory: {RAW_DATA_DIR}")
print(f"Cleaned data directory: {CLEANED_DATA_DIR}")

# Check if raw data directory exists
if not RAW_DATA_DIR.exists():
    raise FileNotFoundError(f"Raw data directory not found: {RAW_DATA_DIR}")

# Check if there are CSV files to process
files = list(RAW_DATA_DIR.glob('*.csv'))
if not files:
    print(f"No CSV files found in {RAW_DATA_DIR}")
else:
    print(f"Found {len(files)} CSV files to process")

# Fonctions de nettoyage
def clean_title(title):
    return re.sub(
        r'(?i)\b(8GB|. |PC|Notebook|Ryzen|UHD|Graphics|DDR4|AMD|W11|Win11|Win|11|Cond|!!|LOADED|TouchBar|Mac OS|Black| i3StorageWin|Gaming|Laptop|Touchscreen|Pro|15.6|Windows|RTX|FHD|LaptopWin11|HDD| ,|French|13inch|'
        r' - | /|macOS|VENTURA|FREE|SHIPPIN|i9|13.3|inches|TURBO|"|- | , |13INCH|EXCELLENT|'
        r'REFURBISHED|NEW|MWTK2LL|Qwerty|Spanish|Keyboard|British|\d+GB|\d+TB|[\d\.]+ ?GHz| GB |'
        r'rouge|Gray|BIG SUR|WEBCAM|WIFI|BLUETOOTHGB|TB|space gray|silver|gold|touch bar|GHz|'
        r'Intel|Core|i7|th|Gen|GB|Very|RAM|i5| GB| TB|GB GB|.GHZ| CPU | GPU|-|SSD|256|512|Good|'
        r'Condition|magic keyboard|✅|🔋|grade [A-B]|warranty\.\.\.)',
        '',
        str(title)
    ).strip().replace('  ', '')

def clean_price(price):
    if not isinstance(price, str):
        return None
    price = re.sub(r'[^\d\.]', '', price)
    try:
        return float(price)
    except ValueError:
        return None

def convert_to_usd(price_mad, exchange_rate=10):
    if price_mad is None:
        return None
    return round(price_mad / exchange_rate, 2)

def clean_ram(ram):
    if not isinstance(ram, str):
        return None
    match = re.search(r'(\d+)\s*GB', ram, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None

def clean_cpu(cpu):
    if pd.isna(cpu): return None
    cpu = re.sub(r'\d+\.\d+ GHz|GHz', '', str(cpu)).strip()
    return cpu if cpu else None

def clean_model(title):
    models = ['Pavilion', 'IdeaPad', 'ThinkPad', 'ROG', 'Predator', 'Vostro']
    if pd.isna(title): return None
    for model in models:
        if model.lower() in title.lower():
            return model
    return title.split()[0].strip()

VALID_BRANDS = ["hp", "dell", "lenovo", "acer", "msi", "asus"]

def clean_brand(title):
    brands = ['HP', 'Lenovo', 'Dell', 'ASUS', 'Acer', 'MSI', 'Alienware']
    if pd.isna(title): return None
    for brand in brands:
        if brand.lower() in title.lower():
            return brand
    return title.split()[0].strip()

def clean_gpu(gpu):
    if not isinstance(gpu, str):
        return ""
    gpu = gpu.lower()
    gpu = re.sub(r'[^a-zA-Z0-9\s]', '', gpu)
    gpu = re.sub(r'\s+', ' ', gpu).strip()
    return gpu

def clean_screen_size(screen_size):
    if not isinstance(screen_size, str):
        return None
    match = re.search(r'(\d+\.?\d*)\s*inch', screen_size, re.IGNORECASE)
    if match:
        return float(match.group(1))
    return None

def clean_storage(storage):
    if not isinstance(storage, str):
        return None
    storage = storage.lower()
    if "tb" in storage:
        match = re.search(r'(\d+)\s*tb', storage)
        if match:
            return int(match.group(1)) * 1024
    elif "gb" in storage:
        match = re.search(r'(\d+)\s*gb', storage)
        if match:
            return int(match.group(1))
    return None

# Process all CSV files in the raw data directory
try:
    for file in RAW_DATA_DIR.glob('*.csv'):
        print(f"Processing file: {file}")
        df = pd.read_csv(file)

        # Appliquer les fonctions de nettoyage
        df['Title'] = df['title'].apply(clean_title)
        df['Price'] = df['price'].apply(clean_price)
        df['Price'] = df['Price'].apply(convert_to_usd)
        df['RAM'] = df['Ram Memory Installed Size'].apply(clean_ram)
        df['CPU'] = df['CPU Model'].apply(clean_cpu)
        df['Model'] = df['title'].apply(clean_model)
        df['Brand'] = df['title'].apply(clean_brand)
        df['GPU'] = df['Graphics Coprocessor'].apply(clean_gpu)
        df['Screen Size'] = df['Screen Size'].apply(clean_screen_size)
        df['Storage'] = df['Hard Disk Size'].apply(clean_storage)

        # Sélectionner les colonnes nécessaires
        cleaned_df = df[['Title', 'Price', 'RAM', 'CPU', 'Model', 'Brand', 'GPU', 'Screen Size', 'Storage']]

        # Sauvegarder le résultat nettoyé
        output_filename = CLEANED_DATA_DIR / f"{file.stem}_cleaned.csv"
        cleaned_df.to_csv(output_filename, index=False)
        print(f"Cleaned data saved to {output_filename}")
except Exception as e:
    print(f"An error occurred: {e}")