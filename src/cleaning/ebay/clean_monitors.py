import os
import pandas as pd

# Define directories
base_dir = r"C:\Users\hp\Desktop\ecommerce_scraper"
raw_ebay_dir = os.path.join(base_dir, "data", "raw", "ebay", "monitors")
cleaned_ebay_dir = os.path.join(base_dir, "data", "cleaned", "ebay", "monitors")

# Ensure the cleaned data directory exists
os.makedirs(cleaned_ebay_dir, exist_ok=True)

def process_ebay_file(file_path):
    try:
        # Load data
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # Perform basic cleaning (customize as needed)
        df = df.dropna(how='all')  # Remove empty rows
        
        # Save cleaned data
        cleaned_filename = os.path.basename(file_path).replace(".csv", "_cleaned.csv")
        cleaned_file_path = os.path.join(cleaned_ebay_dir, cleaned_filename)
        df.to_csv(cleaned_file_path, index=False, encoding='utf-8')
        
        print(f"Cleaned data saved to {cleaned_file_path}")
    except Exception as e:
        print(f"An error occurred while processing {file_path}: {e}")

# Process only eBay files
for filename in os.listdir(raw_ebay_dir):
    if filename.endswith(".csv"):
        file_path = os.path.join(raw_ebay_dir, filename)
        print(f"Processing eBay file: {file_path}")
        process_ebay_file(file_path)
