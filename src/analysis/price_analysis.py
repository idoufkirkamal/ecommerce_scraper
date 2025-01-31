import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pathlib import Path
from fuzzywuzzy import process, fuzz

# Configure paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLEANED_DATA_PATH = PROJECT_ROOT / 'data' / 'cleaned'
RESULTS_PATH = PROJECT_ROOT / 'results'
RESULTS_PATH.mkdir(exist_ok=True)

def load_cleaned_data(category: str):
    """Load cleaned data from all platforms for a specific category"""
    platforms = ['ebay', 'flipkart', 'ubuy']
    dfs = []
    
    for platform in platforms:
        path = CLEANED_DATA_PATH / platform / category / f'*.csv'
        for file in path.parent.glob(path.name):
            df = pd.read_csv(file)
            df['platform'] = platform  # Add platform identifier
            dfs.append(df)
    
    return pd.concat(dfs, ignore_index=True)

def clean_data(df: pd.DataFrame):
    """Clean the data by removing empty rows and invalid values"""
    # Drop rows where all values are missing
    df = df.dropna(how='all')
    
    # Fill missing values in key columns with an empty string
    for col in ['Title', 'Memory Size', 'Memory Type', 'Chipset/GPU Model']:
        df[col] = df[col].fillna('')
    
    # Ensure key columns are treated as strings
    for col in ['Title', 'Memory Size', 'Memory Type', 'Chipset/GPU Model']:
        df[col] = df[col].astype(str)
    
    return df

def clean_price_column(df: pd.DataFrame):
    """Clean the price column by removing currency symbols and converting to numeric"""
    df['Price'] = df['Price'].replace(r'[\$,GBP,EUR]', '', regex=True)  # Use raw string for regex
    df['Price'] = df['Price'].str.replace('/ea', '', regex=False)  # Remove "/ea" if present
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')  # Convert to numeric, handle errors
    return df

def standardize_product_names(df: pd.DataFrame):
    """Standardize product names using fuzzy matching"""
    # Extract unique product names
    unique_names = df['Title'].unique()
    
    # Create a dictionary to map similar names to a standardized name
    name_mapping = {}
    for name in unique_names:
        if not name:  # Skip empty names
            name_mapping[name] = name
            continue
        
        # Find the best match among existing standardized names
        if name_mapping:  # Ensure name_mapping is not empty
            match, score = process.extractOne(name, name_mapping.keys(), scorer=fuzz.token_sort_ratio)
            if score > 80:  # Threshold for considering a match
                name_mapping[name] = match
            else:
                name_mapping[name] = name  # Use the original name if no good match is found
        else:
            name_mapping[name] = name  # Use the original name if no mappings exist yet
    
    # Apply the mapping to the DataFrame
    df['standardized_name'] = df['Title'].map(name_mapping)
    return df

def analyze_price_differences(df: pd.DataFrame, category: str):
    """Analyze price differences for similar products with the same specifications"""
    # Create results directory for category
    category_results = RESULTS_PATH / category
    category_results.mkdir(exist_ok=True)
    
    # Clean the data
    df = clean_data(df)
    
    # Clean the price column
    df = clean_price_column(df)
    
    # Standardize product names
    df = standardize_product_names(df)
    
    # Group by standardized name and specifications
    grouped = df.groupby(
        ['standardized_name', 'Memory Size', 'Memory Type', 'Chipset/GPU Model', 'platform']
    )['Price'].mean().unstack()
    
    # Filter products that are available on all three platforms
    grouped = grouped.dropna()
    
    # Calculate price differences
    grouped['price_diff'] = grouped.max(axis=1) - grouped.min(axis=1)
    
    # Sort by price difference to highlight the most significant differences
    grouped = grouped.sort_values(by='price_diff', ascending=False)
    
    # Select the top 5 products with the largest price differences
    top_5 = grouped.head(5)
    
    # Save the top 5 results
    top_5.to_csv(category_results / 'top_5_price_differences_same_specs.csv')
    
    # Visualization: Price differences for the top 5 products
    plt.figure(figsize=(12, 6))
    top_5.drop(columns=['price_diff']).plot(kind='bar', figsize=(12, 6))
    plt.title(f'Price differences between the 3 platforms for a maximum of 5 products - {category}')
    plt.ylabel('Price (USD)')
    plt.xlabel('Product Name and Specifications')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(category_results / 'top_5_price_differences_same_specs.png')
    plt.close()

if __name__ == "__main__":
    categories = ['graphics_cards', 'laptops', 'monitors', 'smart_watches']
    
    for category in categories:
        try:
            print(f"Analyzing {category}...")
            df = load_cleaned_data(category)
            analyze_price_differences(df, category)
        except Exception as e:
            print(f"Error processing {category}: {str(e)}")