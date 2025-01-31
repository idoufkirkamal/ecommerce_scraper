import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pathlib import Path
from matplotlib.dates import DateFormatter

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
    df['Price'] = df['Price'].replace(r'[\$,GBP,EUR]', '', regex=True)
    df['Price'] = df['Price'].str.replace('/ea', '', regex=False)
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    return df

def filter_products_on_all_platforms(df: pd.DataFrame):
    """Filter products available on all three platforms based on exact specifications"""
    # Group by key specifications
    grouped = df.groupby(
        ['Memory Size', 'Memory Type', 'Chipset/GPU Model']
    )['platform'].nunique().reset_index()
    
    # Filter products that appear on all three platforms
    common_products = grouped[grouped['platform'] == 3]
    
    # Merge with the original dataframe to get the full data for these products
    df_filtered = pd.merge(
        df,
        common_products[['Memory Size', 'Memory Type', 'Chipset/GPU Model']],
        on=['Memory Size', 'Memory Type', 'Chipset/GPU Model'],
        how='inner'
    )
    
    return df_filtered

def analyze_price_differences(df: pd.DataFrame, category: str):
    """Analyze price differences and plot trends for products available on all platforms"""
    category_results = RESULTS_PATH / category
    category_results.mkdir(exist_ok=True)
    
    # Clean and preprocess data
    df = clean_data(df)
    df = clean_price_column(df)
    
    # Filter products available on all three platforms based on exact specifications
    df = filter_products_on_all_platforms(df)
    
    # If no products are available on all platforms, skip analysis
    if df.empty:
        print(f"No products available on all platforms for {category}. Skipping analysis.")
        return
    
    # Convert Collection Date to datetime and remove time component
    df['Collection Date'] = pd.to_datetime(df['Collection Date']).dt.strftime('%Y-%m-%d')
    
    # Aggregate data by day and platform
    df_aggregated = df.groupby(
        ['Memory Size', 'Memory Type', 'Chipset/GPU Model', 'platform', 'Collection Date']
    )['Price'].mean().reset_index()
    
    # Sort by date to ensure correct plotting order
    df_aggregated = df_aggregated.sort_values('Collection Date')
    
    # Group data for analysis
    grouped = df_aggregated.groupby(
        ['Memory Size', 'Memory Type', 'Chipset/GPU Model', 'platform', 'Collection Date']
    )['Price'].mean().unstack(level='platform')
    
    # Initialize a counter for dynamic file naming
    product_counter = 1
    
    # Plot column graphs for each product
    for product, data in grouped.groupby(level=[0, 1, 2]):
        # Extract the title for the product
        product_title = df[
            (df['Memory Size'] == product[0]) &
            (df['Memory Type'] == product[1]) &
            (df['Chipset/GPU Model'] == product[2])
        ]['Title'].iloc[0]  # Take the first title for the product
        
        plt.figure(figsize=(12, 6))
        
        # Plot each platform's prices as columns
        data.plot(kind='bar', figsize=(12, 6))
        
        plt.title(f'Price Trends for {product_title}')
        plt.xlabel('Collection Date')
        plt.ylabel('Price (USD)')
        
        # Set x-ticks to only show dates
        plt.xticks(range(len(data.index)), data.index.get_level_values('Collection Date'), rotation=45, ha='right')
        
        plt.legend(title='Platform')
        plt.grid(True)
        plt.tight_layout()
        
        # Create a sanitized title for the file name
        sanitized_title = "_".join(product_title.split()).replace("/", "_").replace("\\", "_").replace(":", "_").replace("*", "_").replace("?", "_").replace('"', "_").replace("<", "_").replace(">", "_").replace("|", "_")
        
        # Save the plot with dynamic file naming
        file_name = f"Product{product_counter:02d}_{sanitized_title}.png"
        plt.savefig(category_results / file_name)
        plt.close()
        
        # Increment the counter for the next product
        product_counter += 1
                   
if __name__ == "__main__":
    categories = ['graphics_cards', 'laptops', 'monitors', 'smart_watches']
    for category in categories:
        try:
            print(f"Analyzing {category}...")
            df = load_cleaned_data(category)
            analyze_price_differences(df, category)
        except Exception as e:
            print(f"Error processing {category}: {str(e)}")