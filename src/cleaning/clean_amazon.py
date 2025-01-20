import pandas as pd
import os

# Load the CSV file
file_path = r'/src/scraping/data/raw/laptop_day1_amazon.csv'
data = pd.read_csv(file_path)

# Normalize missing values ("Data not available" → NaN)
data.replace("Data not available", pd.NA, inplace=True)

# Drop unnecessary columns (if needed)
columns_to_drop = ['promo', 'coupon', 'product_url']  # Example
data = data.drop(columns=columns_to_drop, errors='ignore')

# Clean and convert 'price'


# Extract numeric values from specific columns
def extract_numeric(value):
    if pd.isna(value):
        return value
    return float(''.join(filter(str.isdigit, str(value))) or 0)

# Apply extraction on specific columns
data['screen_size'] = data['screen_size'].apply(extract_numeric)
data['hard_disk_size'] = data['hard_disk_size'].apply(extract_numeric)
data['ram_memory'] = data['ram_memory'].apply(extract_numeric)

# Convert columns to numeric to avoid further issues
data['screen_size'] = pd.to_numeric(data['screen_size'], errors='coerce')
data['hard_disk_size'] = pd.to_numeric(data['hard_disk_size'], errors='coerce')
data['ram_memory'] = pd.to_numeric(data['ram_memory'], errors='coerce')

# Replace missing values with the most frequent value or a suitable default
fill_values = {
    'brand': data['brand'].mode()[0] if not data['brand'].mode().empty else 'No Brand',
    'model_name': data['model_name'].mode()[0] if not data['model_name'].mode().empty else 'No Model',
    'screen_size': data['screen_size'].median(skipna=True) if not data['screen_size'].isna().all() else 0,
    'hard_disk_size': data['hard_disk_size'].median(skipna=True) if not data['hard_disk_size'].isna().all() else 0,
    'ram_memory': data['ram_memory'].median(skipna=True) if not data['ram_memory'].isna().all() else 0,
    'operating_system': data['operating_system'].mode()[0] if not data['operating_system'].mode().empty else 'No OS',
    'special_feature': data['special_feature'].mode()[0] if not data['special_feature'].mode().empty else 'None'
}
data.fillna(value=fill_values, inplace=True)

# Clean text values
def clean_text(value):
    if pd.isna(value):
        return value
    return str(value).strip()

# Apply cleaning on each column individually
data = data.apply(lambda col: col.map(clean_text) if col.dtypes == 'object' else col)

# Remove remaining duplicates
data = data.drop_duplicates()

# Path to save the cleaned file
output_file_path = r'/src/scraping/data/raw/amazon_results_cleaned_day1.csv'

# Check if the target directory exists, and create it if necessary
output_dir = os.path.dirname(output_file_path)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Save the cleaned results to a CSV file
data.to_csv(output_file_path, index=False)
print(f"Cleaning completed. File saved to '{output_file_path}'.")