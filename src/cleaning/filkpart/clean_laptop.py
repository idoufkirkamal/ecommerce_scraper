import pandas as pd
import re
from pathlib import Path

# Define paths using relative paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # Root folder of the project
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw' / 'flipkart' / 'laptops'
CLEANED_DATA_DIR = BASE_DIR / 'data' / 'cleaned' / 'flipkart' / 'laptops'

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

# Function to clean and extract relevant title details
def clean_title(title):
    return re.sub(
        r'(?i)\b(8GB|PC|Notebook|Ryzen|UHD|Graphics|DDR4|AMD|Win11|Win|Cond|TouchBar|Mac OS|Black|Gaming|Laptop|Touchscreen|Pro|Windows|RTX|FHD|SSD|HDD|French|13inch|'
        r' /|macOS|VENTURA|SHIPPIN|i9|inches|TURBO|- | , |EXCELLENT|REFURBISHED|NEW|Qwerty|Spanish|Keyboard|British|\d+GB|\d+TB|[\d\.]+ ?GHz|'
        r'rouge|Gray|BIG SUR|WEBCAM|WIFI|BLUETOOTH|space gray|silver|gold|GHz|Intel|Core|i7|th|Gen|Very|RAM|i5| CPU | GPU|-|Good|'
        r'Condition|magic keyboard|✅|🔋|grade [A-B]|warranty\.\.\.)',
        '',
        str(title)
    ).strip().replace('  ', '')

# Function to extract price in USD
def extract_price(row):
    price = row['price']
    if pd.isna(price) and isinstance(row['Sales Package'], str):
        match = re.search(r'₹(\d{1,3}(?:,\d{3})*\.\d{2})', row['Sales Package'])
        if match:
            price = match.group(1)
    
    try:
        if isinstance(price, str):
            price = float(price.replace(',', '').replace('₹', '')) / 80  # Convert to USD (approx ₹1 = $0.0125)
        else:
            price = float(price) / 80
    except ValueError:
        price = None
    return price


def extract_ram(row):
    ram = row['RAM']
    if pd.isna(ram) and isinstance(row['Sales Package'], str):
        match = re.search(r'(\d+)\s*GB', row['Sales Package'])
        if match:
            ram = match.group(1)
    try:
        ram = int(ram.replace('GB', '').strip())
    except (ValueError, AttributeError):
        ram = None
    return ram

def extract_storage(row):
    storage = row['SSD Capacity']
    if pd.isna(storage) and isinstance(row['Sales Package'], str):
        match = re.search(r'(\d+)\s*GB', row['Sales Package'])
        if match:
            storage = match.group(1)
        else:
            match = re.search(r'(\d+)\s*TB', row['Sales Package'])
            if match:
                storage = int(match.group(1)) * 1024  # Convert TB to GB
    try:
        storage = int(storage.replace('GB', '').strip())
    except (ValueError, AttributeError):
        storage = None
    return storage
def extract_cpu(row):
    cpu = row['Processor Name']
    if pd.isna(cpu) and isinstance(row['Sales Package'], str):
        match = re.search(r'(Intel\s*Core\s*i\d+|AMD\s*Ryzen\s*\d+)', row['Sales Package'])
        if match:
            cpu = match.group(0)
    return cpu

def extract_model(row):
    model = row['Model Name']
    if pd.isna(model) and isinstance(row['Sales Package'], str):
        match = re.search(r'Model Name=\s*(.+)', row['Sales Package'])
        if match:
            model = match.group(1)
    return model

def extract_brand(row):
    for col in ['title', 'Sales Package']:
        value = row[col]
        if isinstance(value, str):
            match = re.search(r'(ASUS|Lenovo|HP|Samsung|Acer|MSI|Apple|Dell|Zebronics|Thomson|Infinix|Jio|Ultimus)', value, re.IGNORECASE)
            if match:
                return match.group(1)
    return None

def extract_gpu(row):
    gpu = row['Graphic Processor']
    if pd.isna(gpu) and isinstance(row['Sales Package'], str):
        match = re.search(r'(NVIDIA\s*GeForce\s*.+?|Intel\s*Integrated\s*.+?)', row['Sales Package'])
        if match:
            gpu = match.group(0)
    return gpu

def extract_screen_size(row):
    screen_size = row['Screen Size']
    if pd.isna(screen_size) and isinstance(row['Sales Package'], str):
        match = re.search(r'(\d+\.\d+)\s*inch', row['Sales Package'])
        if match:
            return float(match.group(1))
    return screen_size

def extract_collection_date(filename):
    match = re.search(r'(\d{4}_\d{2}_\d{2})', filename)
    if match:
        return match.group(1).replace('_', '-')
    return None

# Process all CSV files in the raw data directory
try:
    for file in files:

        df = pd.read_csv(file)
         # Debugging
        collection_date = extract_collection_date(file.stem)

        # Apply cleaning and extraction functions
        df['Title'] = df['title'].apply(clean_title)
        df['Price'] = df.apply(extract_price, axis=1)
        df['RAM'] = df.apply(extract_ram, axis=1)
        df['CPU'] = df.apply(extract_cpu, axis=1)
        df['Model'] = df.apply(extract_model, axis=1)
        df['Brand'] = df.apply(extract_brand, axis=1)
        df['GPU'] = df.apply(extract_gpu, axis=1)
        df['Screen Size'] = df.apply(extract_screen_size, axis=1)
        df['Storage'] = df.apply(extract_storage, axis=1)
        df['Collection Date'] = collection_date
        # Keep only necessary columns
        columns_to_keep = ['Title', 'Price', 'RAM', 'CPU', 'Model', 'Brand', 'GPU', 'Screen Size', 'Storage',
                           'Collection Date']
        df_cleaned = df[columns_to_keep].copy()

        # Drop rows with missing values
        df_cleaned.dropna(inplace=True)

        # Preview cleaned data
        print(df_cleaned.head())

        # Save cleaned data to CSV with UTF-8 encoding
        output_filename = CLEANED_DATA_DIR / f"{file.stem}_cleaned.csv"
        df_cleaned.to_csv(output_filename, index=False, encoding='utf-8')
        print(f"Cleaned data saved to {output_filename}")

except Exception as e:
    print(f"An error occurred: {e}")
