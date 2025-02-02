import pandas as pd
import re
from sklearn.impute import SimpleImputer
from pathlib import Path

# Define paths using relative paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # Root folder of the project
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw' / 'ubuy' / 'smart_watches'
CLEANED_DATA_DIR = BASE_DIR / 'data' / 'cleaned' / 'ubuy' / 'smart_watches'

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
    if isinstance(title, str):  # Ensure the title is a string
        return title.strip()
    return str(title).strip()  # Convert non-string values to string before stripping

def clean_price(price):
    # Convert to string if it's not already
    if isinstance(price, float):
        price = str(price)

    price = price.replace("\n", "").replace("\r", "").strip()  # Remove newlines and spaces
    price = re.sub(r'\s+', '', price)  # Remove all unnecessary spaces

    if price.startswith('MAD'):
        price = price.replace('MAD', '').replace(',', '')

    try:
        return float(price)
    except ValueError:
        return None

def convert_to_usd(price_mad, exchange_rate=10):
    if price_mad is None:
        return None
    return round(price_mad / exchange_rate, 2)

def extract_case_size(title):
    match = re.search(r'(\d+(\.\d+)?)\s*"', title)
    if match:
        return float(match.group(1))
    return None

def extract_battery_capacity(battery_capacity_str):
    if isinstance(battery_capacity_str, float) or pd.isna(battery_capacity_str):
        return None
    battery_capacity_str = str(battery_capacity_str)
    match = re.search(r'(\d+)\s*Milliamp Hours', battery_capacity_str)
    if match:
        return int(match.group(1))
    match = re.search(r'(\d+)\s*Milliamp', battery_capacity_str)
    if match:
        return int(match.group(1))
    match = re.search(r'(\d+)\s*mAh', battery_capacity_str)
    if match:
        return int(match.group(1))
    return None

def extract_brand(title):
    brands = ['Masis', 'motsfit', 'LOJUSIMEH', 'Joautrial', 'WalkerFit', 'SGDDFIT', 'Erkwei', 'KEEPONFIT', 'rowatch',
              'IOWODO', 'Bemtava']
    for brand in brands:
        if brand.lower() in title.lower():
            return brand
    return None

def extract_model(title):
    models = ['KCBK80', 'T19P', 'HY2473B3', 'ST2-A PRO-XYX', 'K35 Black', 'rowatch F12', 'ZL73E', 'A2 Pro', 'R30 Pro']
    for model in models:
        if model.lower() in title.lower():
            return model
    return None

def extract_os(os_str):
    if isinstance(os_str, float) or pd.isna(os_str):
        return None

    os_str = str(os_str)
    if 'Android' in os_str and 'iOS' in os_str:
        return 'Android/iOS'
    elif 'Android' in os_str:
        return 'Android'
    elif 'iOS' in os_str:
        return 'iOS'
    return None

def extract_storage(storage_str):
    if isinstance(storage_str, float) or pd.isna(storage_str):
        return None

    storage_str = str(storage_str)

    match = re.search(r'(\d+)\s*MB', storage_str)
    if match:
        return int(match.group(1))  # Keep value in MB
    match = re.search(r'(\d+)\s*GB', storage_str)
    if match:
        return int(match.group(1))  # Keep value in GB (unchanged)
    return None

def clean_data(df):
    df['Title'] = df['title'].apply(clean_title)
    df['Price'] = df['price'].apply(clean_price)
    df['Price'] = df['Price'].apply(convert_to_usd)
    df['Case Size'] = df['Title'].apply(extract_case_size)
    df['Battery Capacity'] = df['Battery Capacity'].apply(extract_battery_capacity)
    df['Brand'] = df['Title'].apply(extract_brand)
    df['Model'] = df['Title'].apply(extract_model)
    df['Operating System'] = df['Operating System'].apply(extract_os)
    df['Storage Capacity'] = df['Memory Storage Capacity'].apply(extract_storage)

    # Handle missing values with imputers
    imputer_case_size = SimpleImputer(strategy='median')
    df['Case Size'] = imputer_case_size.fit_transform(df[['Case Size']]).ravel()

    imputer_battery_capacity = SimpleImputer(strategy='median')
    df['Battery Capacity'] = imputer_battery_capacity.fit_transform(df[['Battery Capacity']]).ravel()

    imputer_brand = SimpleImputer(strategy='most_frequent')
    df['Brand'] = imputer_brand.fit_transform(df[['Brand']]).ravel()

    imputer_model = SimpleImputer(strategy='most_frequent')
    df['Model'] = imputer_model.fit_transform(df[['Model']]).ravel()

    imputer_os = SimpleImputer(strategy='most_frequent')
    df['Operating System'] = imputer_os.fit_transform(df[['Operating System']]).ravel()

    imputer_storage = SimpleImputer(strategy='median')
    df['Storage Capacity'] = imputer_storage.fit_transform(df[['Storage Capacity']]).ravel()

    columns_to_keep = ['Title', 'Price', 'Case Size', 'Battery Capacity', 'Brand', 'Model', 'Operating System',
                       'Storage Capacity']
    df_cleaned = df[columns_to_keep]

    return df_cleaned

# Process all CSV files in the raw data directory
try:
    for file in RAW_DATA_DIR.glob('*.csv'):
        print(f"Processing file: {file}")
        df = pd.read_csv(file)

        # Clean the data
        df_cleaned = clean_data(df)

        # Save the cleaned DataFrame
        output_filename = CLEANED_DATA_DIR / f"{file.stem}_cleaned.csv"
        df_cleaned.to_csv(output_filename, index=False)
        print(f"Cleaned data saved to {output_filename}")
except Exception as e:
    print(f"An error occurred: {e}")