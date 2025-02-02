import pandas as pd
import re
import os
from pathlib import Path

# Define paths using relative paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent  # Root folder of the project
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw' / 'flipkart' / 'graphics_cards'
CLEANED_DATA_DIR = BASE_DIR / 'data' / 'cleaned' / 'flipkart' / 'graphics_cards'

# Debugging: Print paths
print(f"Base directory: {BASE_DIR}")
print(f"Raw data directory: {RAW_DATA_DIR}")
print(f"Cleaned data directory: {CLEANED_DATA_DIR}")

# Ensure the cleaned data directory exists
CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Check if raw data directory exists
if not RAW_DATA_DIR.exists():
    raise FileNotFoundError(f"Raw data directory not found: {RAW_DATA_DIR}")

# Check if there are CSV files to process
files = list(RAW_DATA_DIR.glob('*.csv'))
if not files:
    print(f"No CSV files found in {RAW_DATA_DIR}")
else:
    print(f"Found {len(files)} CSV files to process")

# Exchange rate for INR to USD
EXCHANGE_RATE_INR_TO_USD = 83  # Update this value as needed

# Function to clean the price and convert it to USD
def clean_price_and_convert_to_usd(price):
    if pd.isna(price) or price == 'Data not available':
        return None
    # Remove non-numeric characters and convert to integer
    price_inr = int(re.sub(r'[^\d]', '', price))
    # Convert INR to USD
    price_usd = round(price_inr / EXCHANGE_RATE_INR_TO_USD, 2)
    return price_usd

# Function to extract the memory size
def extract_memory_size(memory):
    if pd.isna(memory) or memory == 'Data not available':
        return None
    match = re.search(r'(\d+) GB', memory)
    return int(match.group(1)) if match else None

# Function to extract the memory type
def extract_memory_type(memory):
    if pd.isna(memory) or memory == 'Data not available':
        return None
    match = re.search(r'\d+ GB (\w+)', memory)
    return match.group(1) if match else None

# Function to extract the chipset/GPU model
def extract_gpu_model(model_id):
    if pd.isna(model_id) or model_id == 'Data not available':
        return None
    return model_id

# Function to extract connectors
def extract_connectors(dvi_hmdi_interface):
    if pd.isna(dvi_hmdi_interface) or dvi_hmdi_interface == 'Data not available':
        return None
    connectors = []
    if 'HDMI' in dvi_hmdi_interface:
        connectors.append('HDMI')
    if 'DVI' in dvi_hmdi_interface:
        connectors.append('DVI')
    if 'Mini HDMI' in dvi_hmdi_interface:
        connectors.append('Mini HDMI')
    if 'DisplayPort' in dvi_hmdi_interface:
        connectors.append('DisplayPort')
    if 'VGA' in dvi_hmdi_interface:
        connectors.append('VGA')
    return ', '.join(connectors) if connectors else None

# Extract missing data
def extract_missing_data(df):
    df['Memory Size'] = df['Memory'].apply(extract_memory_size)
    df['Memory Type'] = df['Memory'].apply(extract_memory_type)
    df['Chipset/GPU Model'] = df['Model ID'].apply(extract_gpu_model)
    df['Connectors'] = df['DVI and HDMI Interface'].apply(extract_connectors)
    return df

# Drop unnecessary columns
def drop_unnecessary_columns(df):
    # Add 'collection_date' to the list of columns to keep
    columns_to_keep = ['title', 'Price', 'Brand', 'Memory Size', 'Memory Type', 'Chipset/GPU Model', 'Connectors', 'collection_date']
    existing_columns = [col for col in columns_to_keep if col in df.columns]
    return df[existing_columns]

# Handle missing values
def fill_missing_values(df):
    if 'price' in df.columns:
        df.loc[:, 'Price'] = df['price'].apply(clean_price_and_convert_to_usd)  # Convert price to USD
    if 'Brand' in df.columns:
        df.loc[:, 'Brand'] = df['Brand'].fillna('Unknown')
    if 'Memory Size' in df.columns:
        df.loc[:, 'Memory Size'] = df['Memory Size'].fillna(0)
    if 'Memory Type' in df.columns:
        df.loc[:, 'Memory Type'] = df['Memory Type'].fillna('Unknown')
    if 'Chipset/GPU Model' in df.columns:
        df.loc[:, 'Chipset/GPU Model'] = df['Chipset/GPU Model'].fillna('Unknown')
    if 'Connectors' in df.columns:
        df.loc[:, 'Connectors'] = df['Connectors'].fillna('Unknown')
    return df

# Save cleaned data
def save_cleaned_data(df, filename):
    df.to_csv(filename, index=False)

# Remove rows with exactly three 'Unknown' values
def remove_rows_with_three_unknowns(df):
    unknown_counts = df.isin(['Unknown']).sum(axis=1)
    return df[unknown_counts < 3]

# Rename and drop the original price column
def rename_and_drop_price_column(df):
    if 'price' in df.columns:
        df.drop(columns=['price'], inplace=True)  # Drop the old price column
    return df

# Rename the 'collection_date' column to 'Collection Date'
def rename_collection_date_column(df):
    if 'collection_date' in df.columns:
        df.rename(columns={'collection_date': 'Collection Date'}, inplace=True)
    return df

# Full data cleaning pipeline
def clean_data(df):
    df = df.copy()
    df = extract_missing_data(df)
    df = fill_missing_values(df)  # Convert price to USD here
    df = remove_rows_with_three_unknowns(df)
    df = rename_and_drop_price_column(df)  # Drop the old price column
    df = drop_unnecessary_columns(df)
    df = rename_collection_date_column(df)  # Rename 'collection_date' to 'Collection Date'
    return df

# Process all CSV files in the raw data directory
try:
    for file in RAW_DATA_DIR.glob('*.csv'):
        print(f"Processing file: {file}")
        df = pd.read_csv(file)
        print(f"Loaded {len(df)} rows from {file.name}")
        print(f"Columns in the file: {df.columns.tolist()}")
        
        df_cleaned = clean_data(df)
        print(f"Cleaned data has {len(df_cleaned)} rows")
        
        output_filename = CLEANED_DATA_DIR / f"{file.stem}_cleaned.csv"
        save_cleaned_data(df_cleaned, output_filename)
        print(f"Cleaned data saved to {output_filename}")
except Exception as e:
    print(f"An error occurred: {e}")