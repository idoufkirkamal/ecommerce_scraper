import pandas as pd
import re
from collections import defaultdict
from pathlib import Path

# Define paths using relative paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # Root folder of the project
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw' / 'ubuy' / 'graphics_cards'
CLEANED_DATA_DIR = BASE_DIR / 'data' / 'cleaned' / 'ubuy' / 'graphics_cards'

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

# Function to clean Price
def clean_price(price):
    price = re.sub(r'MAD\s*', '', price)
    price = re.sub(r'[^\d.]', '', price).strip()
    try:
        return float(price)
    except ValueError:
        return None

# Function to convert price to USD
def convert_to_usd(price_mad, exchange_rate=10):
    if price_mad is None:
        return None
    return round(price_mad / exchange_rate, 2)

# Function to clean Brand
def clean_brand(brand_name, title):
    if pd.notna(brand_name):
        return brand_name.strip()
    else:
        match = re.search(r'\b(?:AMD|NVIDIA|ZOTAC|ASUS|PNY|EVGA|SAPLOS|GALAX|VISIONTEK|YESTON)\b', title)
        return match.group().strip() if match else None

# Function to clean Memory Size
def clean_memory_size(memory_ram, graphics_card_ram_size, title):
    def sanitize(value):
        if pd.notna(value):
            value = re.sub(r'[^\d]', '', value)
            return value.strip()
        return None
    memory_ram = sanitize(memory_ram)
    graphics_card_ram_size = sanitize(graphics_card_ram_size)
    try:
        if memory_ram:
            return int(memory_ram)
        elif graphics_card_ram_size:
            return int(graphics_card_ram_size)
        else:
            match = re.search(r'(\d+)\s*GB', title)
            return int(match.group(1)) if match else None
    except ValueError:
        return None

# Function to clean Memory Type
def clean_memory_type(memory_speed, graphics_ram_type, title):
    if pd.notna(memory_speed):
        return 'GDDR6' if 'GDDR6' in memory_speed else 'GDDR5'
    elif pd.notna(graphics_ram_type):
        return graphics_ram_type.strip()
    else:
        match = re.search(r'(?:GDDR[56])', title)
        return match.group().strip() if match else None

# Function to clean Chipset/GPU Model
def clean_chipset_gpu_model(graphics_processor_manufacturer, gpu_clock_speed, title):
    if pd.notna(graphics_processor_manufacturer):
        return graphics_processor_manufacturer.strip()
    elif pd.notna(gpu_clock_speed):
        match = re.search(r'(?:(?:AMD|NVIDIA)\s*Radeon|GeForce)\s*(\w+\s*\d+)', title)
        return match.group().strip() if match else None
    else:
        match = re.search(r'(?:(?:AMD|NVIDIA)\s*Radeon|GeForce)\s*(\w+\s*\d+)', title)
        return match.group().strip() if match else None

# Function to clean Connectors
def clean_connectors(video_output_interface, title):
    if pd.notna(video_output_interface):
        return video_output_interface.replace(',', ';').strip()
    else:
        match = re.search(r'(?:DisplayPort|DVI|HDMI|VGA)(?:,\s*(?:DisplayPort|DVI|HDMI|VGA))*', title)
        return match.group().replace(',', ';').strip() if match else None

# Function to clean Title
def clean_title(row):
    gpu_model = str(row.get("Chipset/GPU Model", "")).strip() if pd.notna(row.get("Chipset/GPU Model", "")) else None
    brand = str(row.get("Brand", "")).strip() if pd.notna(row.get("Brand", "")) else None
    title = str(row.get("title", "")).strip().lower() if pd.notna(row.get("title", "")) else ""
    unwanted_terms = [
        r'\bnew\b', r'\bused\b', r'\bgraphics\b', r'\bcard\b', r'\bgpu\b', r'\bvideo\b', r'\bhdmi\b', r'\bvga\b',
        r'\bdvi\b', r'\bdisplayport\b', r'\bminidisplayport\b', r'\busb-c\b', r'\boc\b', r'\bgddr5\b', r'\bgddr6\b',
        r'\bgddr6x\b', r'\b256-bit\b', r'\b128-bit\b', r'\b512mb\b', r'\b1gb\b', r'\b2gb\b', r'\b4gb\b', r'\b8gb\b',
        r'\b12gb\b', r'\b16gb\b', r'\b24gb\b', r'\b32gb\b', r'\b64gb\b', r'\b128mb\b', r'\b256mb\b', r'\b512mb\b',
        r'\b1yr\b', r'\bwarranty\b', r'\bfast\b', r'\bship\b', r'\btested\b', r'\bworking\b', r'\bnot working\b',
        r'\bexcellent\b', r'\bcondition\b', r'\brefurbished\b', r'\bgrade\b', r'\bwith box\b', r'\bwithout box\b',
        r'\bbulk\b', r'\boem\b', r'\bretail\b', r'\bpackage\b', r'\bpackaging\b', r'\bmodel\b', r'\bseries\b',
        r'\bversion\b', r'\bgen\b', r'\bpcie\b', r'\bexpress\b', r'\bslot\b', r'\bconnector\b',
        r'\binterface\b', r'\bdual\b', r'\bsingle\b', r'\btriple\b', r'\bquad\b', r'\bhex\b'
    ]
    clean_name = title
    for term in unwanted_terms:
        clean_name = re.sub(term, "", clean_name)
    clean_name = re.sub(r"[^a-zA-Z0-9\s]", "", clean_name).strip()
    clean_name = re.sub(r"\s+", " ", clean_name)
    if gpu_model and gpu_model.lower() in clean_name and brand and brand.lower() in clean_name:
        return f"{brand} {gpu_model}"
    elif gpu_model and gpu_model.lower() in clean_name:
        return gpu_model
    elif brand and brand.lower() in clean_name:
        return f"{brand} {clean_name}"
    else:
        return f"{brand} {gpu_model}" if brand and gpu_model else gpu_model or brand or clean_name

# Process all CSV files in the raw data directory
try:
    for file in RAW_DATA_DIR.glob('*.csv'):
        print(f"Processing file: {file}")
        df = pd.read_csv(file)
        # Apply cleaning functions
        df['Price'] = df['price'].apply(clean_price)
        df['Price'] = df['Price'].apply(convert_to_usd)
        df['Brand'] = df.apply(lambda row: clean_brand(row['Brand Name'], row['title']), axis=1)
        df['Memory Size'] = df.apply(lambda row: clean_memory_size(row['RAM'], row['Graphics Card Ram Size'], row['title']), axis=1)
        df['Memory Type'] = df.apply(lambda row: clean_memory_type(row['Memory Speed'], row['Graphics RAM Type'], row['title']), axis=1)
        df['Chipset/GPU Model'] = df.apply(lambda row: clean_chipset_gpu_model(row['Graphics Processor Manufacturer'], row['GPU Clock Speed'], row['title']), axis=1)
        df['Connectors'] = df.apply(lambda row: clean_connectors(row['Video Output Interface'], row['title']), axis=1)
        df['Title'] = df.apply(clean_title, axis=1)

        # Fill missing values using forward fill and backward fill
        df.ffill(inplace=True)
        df.bfill(inplace=True)

        # Fill missing Memory Size using defaults
        memory_size_defaults = defaultdict(lambda: None, {
            'AMD Radeon RX 580': 8,
            'NVIDIA GeForce GTX 1050 Ti': 4,
            'NVIDIA GeForce GTX 1060': 6,
            'NVIDIA GeForce GTX 1070 Ti': 8,
            'NVIDIA GeForce GTX 1080 Ti': 11,
            'NVIDIA GeForce RTX 3060': 12,
            'NVIDIA GeForce RTX 3070': 8,
            'NVIDIA GeForce RTX 3080': 10,
            'NVIDIA GeForce RTX 3090': 24,
            'NVIDIA GeForce RTX 4060 Ti': 8,
            'NVIDIA GeForce RTX 4070': 12,
            'NVIDIA GeForce RTX 4070 Ti': 12,
            'NVIDIA GeForce RTX 4080': 16,
            'NVIDIA GeForce RTX 4080 Ti': 16,
            'AMD Radeon RX 550': 4,
            'AMD Radeon RX 560': 4,
            'AMD Radeon RX 5700 XT': 8,
            'AMD Radeon RX 5500 XT': 8,
            'AMD Radeon RX 590': 8,
            'AMD Radeon RX 6600 XT': 8,
            'AMD Radeon RX 6400': 4,
            'AMD Radeon RX 7600 EVO': 8,
            'AMD Radeon RX 7700 XT': 12
        })

        def fill_missing_memory_size(row):
            if pd.isna(row['Memory Size']):
                return memory_size_defaults[row['Chipset/GPU Model']]
            else:
                return row['Memory Size']

        df['Memory Size'] = df.apply(fill_missing_memory_size, axis=1)

        # Drop rows where Memory Size is still NaN
        df.dropna(subset=['Memory Size'], inplace=True)

        # Keep only necessary columns (including Collection Date)
        columns_to_keep = ['Title', 'Price', 'Brand', 'Memory Size', 'Memory Type', 'Chipset/GPU Model', 'Connectors', 'Collection Date']
        existing_columns = [col for col in columns_to_keep if col in df.columns]
        df_cleaned = df[existing_columns]

        # Save cleaned data
        output_filename = CLEANED_DATA_DIR / f"{file.stem}_cleaned.csv"
        df_cleaned.to_csv(output_filename, index=False)
        print(f"Cleaned data saved to {output_filename}")
except Exception as e:
    print(f"An error occurred: {e}")