import pandas as pd
import os

# Constants
RAW_DATA_FILE_PATH = r'/src/scraping/data/raw/laptop_day1_amazon.csv'
OUTPUT_FILE_PATH = r'/src/scraping/data/raw/amazon_results_cleaned_day1.csv'
COLUMNS_TO_DROP = ['promo', 'coupon', 'product_url']


def load_and_clean_data(file_path):
    """Loads the CSV data and performs initial cleaning."""
    data = pd.read_csv(file_path)
    data.replace("Data not available", pd.NA, inplace=True)
    data.drop(columns=COLUMNS_TO_DROP, errors='ignore', inplace=True)
    return data


def clean_numeric_columns(data, columns):
    """Extracts and converts numeric values for specified columns."""

    def extract_numeric(value):
        if pd.isna(value):
            return value
        return float(''.join(filter(str.isdigit, str(value))) or 0)

    for column in columns:
        data[column] = data[column].apply(extract_numeric).astype(float, errors='coerce')
    return data


def fill_missing_values(data):
    """Fills missing values with defaults or the most frequent values."""
    fill_values = {
        'brand': data['brand'].mode()[0] if not data['brand'].mode().empty else 'No Brand',
        'model_name': data['model_name'].mode()[0] if not data['model_name'].mode().empty else 'No Model',
        'screen_size': data['screen_size'].median(skipna=True) if not data['screen_size'].isna().all() else 0,
        'hard_disk_size': data['hard_disk_size'].median(skipna=True) if not data['hard_disk_size'].isna().all() else 0,
        'ram_memory': data['ram_memory'].median(skipna=True) if not data['ram_memory'].isna().all() else 0,
        'operating_system': data['operating_system'].mode()[0] if not data[
            'operating_system'].mode().empty else 'No OS',
        'special_feature': data['special_feature'].mode()[0] if not data['special_feature'].mode().empty else 'None'
    }
    data.fillna(value=fill_values, inplace=True)
    return data


def clean_text_columns(data):
    """Cleans text values by stripping unnecessary whitespace."""

    def clean_text(value):
        if pd.isna(value):
            return value
        return str(value).strip()

    data = data.apply(lambda col: col.map(clean_text) if col.dtypes == 'object' else col)
    return data


def save_cleaned_data(data, output_file_path):
    """Saves the cleaned data to a CSV file."""
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    data.to_csv(output_file_path, index=False)
    print(f"Cleaning completed. File saved to '{output_file_path}'.")


# Main Process
def main():
    data = load_and_clean_data(RAW_DATA_FILE_PATH)
    numeric_columns = ['screen_size', 'hard_disk_size', 'ram_memory']
    data = clean_numeric_columns(data, numeric_columns)
    data = fill_missing_values(data)
    data = clean_text_columns(data)
    data.drop_duplicates(inplace=True)
    save_cleaned_data(data, OUTPUT_FILE_PATH)


if __name__ == "__main__":
    main()