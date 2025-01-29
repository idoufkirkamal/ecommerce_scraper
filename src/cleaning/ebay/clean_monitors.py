import pandas as pd
import re
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler


def clean_monitors():
    # Load data
    df = pd.read_csv('../../../data/raw/ebay/gaming_monitors_2025-01-26.csv')

    # Improved currency conversion helper (previous fix)
    def convert_price(price):
        price_str = str(price).replace('/ea', '').strip()
        conversions = {
            'EUR': 1.08, 'C$': 0.74, '£': 1.27, 'US$': 1,
            'CA$': 0.74, 'A$': 0.66, '¥': 0.0069
        }
        match = re.search(r'([A-Za-z£¥$]{1,3})?\s*([\d,]+\.?\d*)', price_str)
        if not match:
            return np.nan
        currency, amount = match.groups()
        amount = float(amount.replace(',', ''))
        rate = 1.0
        if currency:
            for key, val in conversions.items():
                if key in currency.upper():
                    rate = val
                    break
        return amount * rate

    # Clean prices
    df['Price'] = df['Price'].apply(convert_price)
    df = df.drop(columns=['Maximum Resolution', 'Aspect Ratio'])
    # Safer numeric extraction with fallback
    def safe_extract(value):
        try:
            # First try normal conversion
            return float(value)
        except:
            # Then try regex extraction
            matches = re.findall(r'\d+\.?\d*', str(value))
            return float(matches[0]) if matches else np.nan

    numerical_cols = ['Screen Size', 'Refresh Rate', 'Response Time']
    for col in numerical_cols:
        df[col] = df[col].apply(safe_extract)
        # Convert to numeric as final fallback
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Advanced numerical imputation
    num_df = df[numerical_cols]
    if not num_df.empty:
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(num_df)
        imputer = KNNImputer(n_neighbors=3)
        imputed_data = scaler.inverse_transform(imputer.fit_transform(scaled_data))
        for i, col in enumerate(numerical_cols):
            df[col] = imputed_data[:, i]
            df[col] = df[col].round(1 if col == 'Screen Size' else 0)

    # Categorical imputation
    cat_cols = ['Brand', 'Model']
    for col in cat_cols:
        mode_val = df[col].mode()[0] if not df[col].mode().empty else 'Unknown'
        df[col] = df[col].fillna(mode_val)

    # Condition extraction
    df['Condition'] = df['Title'].str.contains('Cracked|Broken|As-is', case=False) \
        .map({True: 'Damaged', False: 'New'})

    # Save cleaned data
    df.to_csv('cleaned_gaming_monitors.csv', index=False)
    print(f"Cleaned gaming monitors data saved. Missing values: {df.isna().sum().sum()}")


if __name__ == "__main__":
    clean_monitors()