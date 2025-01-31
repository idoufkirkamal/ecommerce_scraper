import pandas as pd
import os
import glob
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# 1. Data Loading and Preparation
# --------------------------------
def load_cleaned_data(base_path):
    """
    Load cleaned data from the specified base path.
    Assumes data is organized by website and category.
    """
    websites = ["ebay", "flipkart", "ubuy"]
    categories = ["graphics_cards", "laptops", "monitors", "smart_watches"]
    all_data = []

    for website in websites:
        for category in categories:
            path = os.path.join(base_path, website, category, "*.csv")
            files = glob.glob(path)
            
            for file in files:
                try:
                    df = pd.read_csv(file)
                    # Extract date from filename
                    date_str = '_'.join(os.path.basename(file).split('_')[2:5])
                    df['scrape_date'] = pd.to_datetime(date_str, format='%Y_%m_%d')
                    df['website'] = website
                    df['category'] = category
                    all_data.append(df)
                except Exception as e:
                    print(f"Error loading {file}: {str(e)}")
    
    return pd.concat(all_data, ignore_index=True)

# 2. Price Comparison Analysis
# --------------------------------
def plot_price_comparison(df):
    """
    Compare prices across platforms for each category.
    """
    plt.figure(figsize=(14, 8))
    sns.boxplot(x='category', y='price', hue='website', data=df)
    plt.title('Price Distribution Comparison Across Platforms')
    plt.xlabel('Product Category')
    plt.ylabel('Price (converted to USD)')
    plt.xticks(rotation=45)
    plt.legend(title='Platform')
    plt.tight_layout()
    plt.show()

# 3. Promotion Analysis
# --------------------------------
def analyze_promotions(df):
    """
    Analyze promotion frequency and trends over time.
    """
    # Promotion frequency by category
    promo_freq = df.groupby(['category', 'website'])['promotion'].mean().reset_index()
    
    plt.figure(figsize=(14, 6))
    sns.barplot(x='category', y='promotion', hue='website', data=promo_freq)
    plt.title('Promotion Frequency by Category and Platform')
    plt.xlabel('Product Category')
    plt.ylabel('Promotion Rate (%)')
    plt.xticks(rotation=45)
    plt.show()

    # Time-based promotion analysis example for graphics cards
    gpu_promo = df[df['category'] == 'graphics_cards']
    gpu_promo = gpu_promo.groupby(['scrape_date', 'website'])['promotion'].mean().reset_index()
    
    plt.figure(figsize=(14, 6))
    sns.lineplot(x='scrape_date', y='promotion', hue='website', data=gpu_promo, marker='o')
    plt.title('Promotion Trends for Graphics Cards')
    plt.xlabel('Date')
    plt.ylabel('Promotion Rate (%)')
    plt.show()

# 4. Time Series Price Analysis
# --------------------------------
def plot_price_trends(df, target_category='graphics_cards'):
    """
    Analyze price trends over time for a specific category.
    """
    category_data = df[df['category'] == target_category]
    
    plt.figure(figsize=(14, 6))
    sns.lineplot(
        x='scrape_date', 
        y='price', 
        hue='website', 
        data=category_data, 
        estimator='median', 
        errorbar=None,
        marker='o'
    )
    plt.title(f'Price Trends for {target_category.replace("_", " ").title()}')
    plt.xlabel('Date')
    plt.ylabel('Price (USD)')
    plt.xticks(rotation=45)
    plt.show()

# 5. Comparative Analysis Dashboard
# --------------------------------
def create_analysis_dashboard(df):
    """
    Create a dashboard of visualizations for comparative analysis.
    """
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    
    # Price distribution
    sns.boxplot(ax=axes[0, 0], x='category', y='price', hue='website', data=df)
    axes[0, 0].set_title('Price Distribution Comparison')
    axes[0, 0].tick_params(axis='x', rotation=45)
    
    # Promotion frequency
    promo_data = df.groupby(['category', 'website'])['promotion'].mean().reset_index()
    sns.barplot(ax=axes[0, 1], x='category', y='promotion', hue='website', data=promo_data)
    axes[0, 1].set_title('Promotion Frequency by Category')
    axes[0, 1].tick_params(axis='x', rotation=45)
    
    # Price trends for graphics cards
    gpu_data = df[df['category'] == 'graphics_cards']
    sns.lineplot(ax=axes[1, 0], 
                x='scrape_date', 
                y='price', 
                hue='website', 
                data=gpu_data,
                estimator='median',
                errorbar=None)
    axes[1, 0].set_title('Graphics Cards Price Trends')
    
    # Price trends for laptops
    laptop_data = df[df['category'] == 'laptops']
    sns.lineplot(ax=axes[1, 1], 
                x='scrape_date', 
                y='price', 
                hue='website', 
                data=laptop_data,
                estimator='median',
                errorbar=None)
    axes[1, 1].set_title('Laptops Price Trends')
    
    plt.tight_layout()
    plt.show()

# Main function to run the analysis
if __name__ == "__main__":
    # Load cleaned data
    df = load_cleaned_data("data/cleaned")
    
    # Perform analysis
    plot_price_comparison(df)
    analyze_promotions(df)
    plot_price_trends(df, 'graphics_cards')
    plot_price_trends(df, 'laptops')
    create_analysis_dashboard(df)