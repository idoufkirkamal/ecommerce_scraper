import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLEANED_DATA_PATH = PROJECT_ROOT / 'data' / 'cleaned'
RESULTS_PATH = PROJECT_ROOT / 'results'

def normalize_os(os):
    """Normalize operating system names"""
    if pd.isna(os) or not isinstance(os, str):
        return "unknown"
    os = os.lower().strip()
    if "android" in os and "ios" in os:
        return "android_ios"
    elif "android" in os:
        return "android"
    elif "ios" in os:
        return "ios"
    return os

def load_cleaned_data():
    """Load cleaned data for smart watches from eBay and Flipkart"""
    platforms = ['ebay', 'flipkart']
    dfs = []
    
    required_columns = {
        'title', 'price', 'case_size', 'battery_capacity', 
        'operating_system', 'storage_capacity', 'collection_date'
    }
    
    for platform in platforms:
        data_dir = CLEANED_DATA_PATH / platform / 'smart_watches'
        if not data_dir.exists():
            logger.warning(f"Missing data directory: {data_dir}")
            continue
            
        for file in data_dir.glob('*.csv'):
            try:
                df = pd.read_csv(file)
                df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
                
                # Validate columns
                missing_cols = required_columns - set(df.columns)
                if missing_cols:
                    logger.warning(f"Missing columns in {file}: {missing_cols}")
                    continue
                
                # Add platform identifier
                df['platform'] = platform
                
                # Normalize key fields
                df['operating_system'] = df['operating_system'].apply(normalize_os)
                df['case_size'] = df['case_size'].apply(
                    lambda x: round(float(x)) if pd.notnull(x) else None
                )
                df['battery_capacity'] = df['battery_capacity'].apply(
                    lambda x: int(x) if pd.notnull(x) else None
                )
                df['storage_capacity'] = df['storage_capacity'].apply(
                    lambda x: int(x) if pd.notnull(x) else None
                )
                
                # Handle collection date
                df['collection_date'] = pd.to_datetime(
                    df['collection_date'], errors='coerce'
                ).dt.date
                
                dfs.append(df)
                
            except Exception as e:
                logger.error(f"Error loading {file}: {str(e)}")
                continue
    
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def filter_products_by_platforms(df: pd.DataFrame):
    """Find smart watches with matching specifications across platforms (excluding brand)"""
    # Create a unique product identifier
    df['product_id'] = (
        df['case_size'].astype(str) + "|" + 
        df['battery_capacity'].astype(str) + "|" + 
        df['operating_system'].astype(str) + "|" + 
        df['storage_capacity'].astype(str)
    )
    
    # Find products present on both platforms
    platform_groups = df.groupby('product_id')['platform'].unique()
    cross_platform = platform_groups[platform_groups.apply(
        lambda x: len(set(x) & {'ebay', 'flipkart'}) >= 2
    )].index.tolist()
    
    return df[df['product_id'].isin(cross_platform)]

def analyze_price_differences(filtered_df: pd.DataFrame):
    """Analyze and visualize price differences for matched smart watches"""
    if filtered_df.empty:
        logger.info("No cross-platform smart watches found for analysis")
        return
    
    # Group by product_id and platform
    grouped = filtered_df.groupby(['product_id', 'platform', 'collection_date'])['price'].mean().unstack(level='platform')
    
    # Generate bar plots for each smart watch
    for product_id, data in grouped.groupby(level=0):
        plt.figure(figsize=(12, 6))
        
        # Plot bar graph
        data.plot(kind='bar', figsize=(12, 6))
        
        # Set title and labels
        plt.title(f"Price Trends for {product_id.replace('|', ' ')}")
        plt.xlabel("Collection Date")
        plt.ylabel("Price (USD)")
        
        # Format x-axis dates
        plt.xticks(range(len(data.index)), data.index.get_level_values('collection_date'), rotation=45, ha='right')
        
        # Add legend and grid
        plt.legend(title='Platform')
        plt.grid(True)
        plt.tight_layout()
        
        # Save the plot
        sanitized_title = re.sub(r"[^\w\s]", "_", product_id.replace("|", "_"))
        file_name = f"SmartWatch_{sanitized_title}.png"
        plt.savefig(RESULTS_PATH / 'smart_watches' / file_name)
        plt.close()

if __name__ == "__main__":
    RESULTS_PATH.mkdir(exist_ok=True)
    (RESULTS_PATH / 'smart_watches').mkdir(exist_ok=True)
    
    try:
        logger.info("Loading and processing smart watch data...")
        df = load_cleaned_data()
        
        if df.empty:
            logger.error("No cleaned data found. Check data/cleaned directories.")
            exit(1)
            
        logger.info(f"Loaded {len(df)} records from cleaned data")
        
        filtered_df = filter_products_by_platforms(df)
        logger.info(f"Found {len(filtered_df)} cross-platform smart watch entries")
        
        analyze_price_differences(filtered_df)
        
    except Exception as e:
        logger.error(f"Critical error: {str(e)}", exc_info=True)