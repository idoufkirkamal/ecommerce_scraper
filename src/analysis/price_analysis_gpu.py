import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import re

# Configure paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLEANED_DATA_PATH = PROJECT_ROOT / 'data' / 'cleaned'
RESULTS_PATH = PROJECT_ROOT / 'results'
RESULTS_PATH.mkdir(exist_ok=True)

def normalize_gpu_model(gpu_model):
    """
    Normalize GPU model names by removing unnecessary text and standardizing formats.
    """
    if pd.isna(gpu_model) or not isinstance(gpu_model, str):
        return "Unknown_GPU"
    
    # Convert to lowercase and remove leading/trailing whitespace
    gpu_model = gpu_model.lower().strip()
    
    # Remove common prefixes/suffixes
    gpu_model = re.sub(r"(geforce|amd|radeon|nvidia)\s*", "", gpu_model)
    
    # Remove extra text like "twin edge", "gaming oc", etc.
    gpu_model = re.sub(r"\b(twin edge|gaming oc|ventus|super|ti|oc|edition|white|black|gold|pro|mech|trinity|aorus|eagle|sg|lhr)\b", "", gpu_model)
    
    # Remove numbers followed by "gb" (e.g., "4gb", "8gb")
    gpu_model = re.sub(r"\b\d+gb\b", "", gpu_model)
    
    # Remove special characters and extra spaces
    gpu_model = re.sub(r"[^\w\s]", "", gpu_model)
    gpu_model = re.sub(r"\s+", " ", gpu_model).strip()
    
    # Handle empty strings after normalization
    if not gpu_model:
        return "Unknown_GPU"
    
    return gpu_model

def load_cleaned_data():
    """Load cleaned data for graphics cards from all platforms"""
    platforms = ['ebay', 'flipkart', 'ubuy']
    dfs = []
    
    for platform in platforms:
        path = CLEANED_DATA_PATH / platform / 'graphics_cards' / f'*.csv'
        for file in path.parent.glob(path.name):
            df = pd.read_csv(file)
            
            # Standardize column names
            df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
            
            # Add platform identifier
            df['platform'] = platform
            
            # Normalize Chipset/GPU Model
            df['chipset/gpu_model'] = df['chipset/gpu_model'].apply(normalize_gpu_model)
            
            # Ensure 'collection_date' exists
            if 'collection_date' not in df.columns:
                print(f"Warning: 'collection_date' column missing in {file}. Skipping file.")
                continue
            
            # Convert 'collection_date' to datetime and handle errors
            df['collection_date'] = pd.to_datetime(df['collection_date'], errors='coerce')
            
            # Normalize collection_date to only include the date (ignore time)
            df['collection_date'] = df['collection_date'].dt.date
            
            dfs.append(df)
    
    return pd.concat(dfs, ignore_index=True)


def filter_products_by_platforms(df: pd.DataFrame):
    """Filter products based on their availability across platforms"""
    # Group by key specifications
    grouped = df.groupby(
        ['memory_size', 'memory_type', 'chipset/gpu_model']
    )['platform'].unique().reset_index()
    
    # Add a column indicating the platforms each product is available on
    grouped['platforms'] = grouped['platform'].apply(lambda x: sorted(x))
    grouped['platform_count'] = grouped['platforms'].apply(len)
    
    # Filter products available on all three platforms
    all_platforms = grouped[grouped['platform_count'] == 3]
    
    # Filter products available on any two platforms
    two_platforms = grouped[grouped['platform_count'] == 2]
    
    return all_platforms, two_platforms

def analyze_price_differences(all_platforms_df: pd.DataFrame, two_platforms_df: pd.DataFrame, original_df: pd.DataFrame):
    """Analyze price differences and plot trends for products available on multiple platforms"""
    category_results = RESULTS_PATH / 'graphics_cards'
    category_results.mkdir(exist_ok=True)
    
    # Combine both datasets for analysis
    combined_df = pd.concat([all_platforms_df, two_platforms_df], ignore_index=True)
    
    # If no products are available on multiple platforms, skip analysis
    if combined_df.empty:
        print("No products available on multiple platforms. Skipping analysis.")
        return
    
    # Merge with the original dataframe to retain collection_date and price
    merged_df = pd.merge(
        original_df,
        combined_df[['memory_size', 'memory_type', 'chipset/gpu_model']],
        on=['memory_size', 'memory_type', 'chipset/gpu_model'],
        how='inner'
    )
    
    # Ensure 'collection_date' exists
    if 'collection_date' not in merged_df.columns:
        print("Error: 'collection_date' column missing after merging. Skipping analysis.")
        return
    
    # Drop rows with missing collection_date
    merged_df = merged_df.dropna(subset=['collection_date'])
    
    # Aggregate data by day and platform
    df_aggregated = merged_df.groupby(
        ['memory_size', 'memory_type', 'chipset/gpu_model', 'platform', 'collection_date']
    )['price'].mean().reset_index()
    
    # Sort by date to ensure correct plotting order
    df_aggregated = df_aggregated.sort_values('collection_date')
    
    # Group data for analysis
    grouped = df_aggregated.groupby(
        ['memory_size', 'memory_type', 'chipset/gpu_model', 'platform', 'collection_date']
    )['price'].mean().unstack(level='platform')
    
    # Initialize a counter for dynamic file naming
    product_counter = 1
    
    # Plot column graphs for each product
    for product, data in grouped.groupby(level=[0, 1, 2]):
        # Format Memory Size to remove decimals
        memory_size = str(int(product[0])) if isinstance(product[0], (float, int)) else product[0]
        
        # Construct a default title using product specifications
        product_title = f"{memory_size}_{product[1]}_{product[2]}"
        
        plt.figure(figsize=(12, 6))
        
        # Plot each platform's prices as columns
        data.plot(kind='bar', figsize=(12, 6))
        
        plt.title(f'Price Trends for {product_title}')
        plt.xlabel('Collection Date')
        plt.ylabel('Price (USD)')
        
        # Set x-ticks to only show dates
        plt.xticks(range(len(data.index)), data.index.get_level_values('collection_date'), rotation=45, ha='right')
        
        plt.legend(title='Platform')
        plt.grid(True)
        plt.tight_layout()
        
        # Sanitize the title for file naming
        sanitized_title = (
            "_".join(product_title.split())
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
            .replace("*", "_")
            .replace("?", "_")
            .replace('"', "_")
            .replace("<", "_")
            .replace(">", "_")
            .replace("|", "_")
        )
        
        # Save the plot with dynamic file naming
        file_name = f"Product{product_counter:02d}_{sanitized_title}.png"
        plt.savefig(category_results / file_name)
        plt.close()
        
        # Increment the counter for the next product
        product_counter += 1

if __name__ == "__main__":
    try:
        print("Analyzing graphics cards...")
        
        # Load cleaned data for graphics cards
        df = load_cleaned_data()
        
        # Filter products based on their availability across platforms
        all_platforms_df, two_platforms_df = filter_products_by_platforms(df)
        
        # Print summary of identified products
        print(f"Products available on all platforms: {len(all_platforms_df)}")
        print(f"Products available on any two platforms: {len(two_platforms_df)}")
        
        # Analyze price differences
        analyze_price_differences(all_platforms_df, two_platforms_df, df)
    except Exception as e:
        print(f"Error processing graphics cards: {str(e)}")