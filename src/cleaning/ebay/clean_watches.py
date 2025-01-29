import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer, SimpleImputer
from sklearn.preprocessing import StandardScaler


def clean_watches():
    # Load data
    df = pd.read_csv('../../../data/raw/ebay/smart_watches_2025-01-26.csv')

    # ---- Price Cleaning ----
    df['Price'] = pd.to_numeric(df['Price'].str.replace(r'[^\d.]', '', regex=True), errors='coerce')

    # ---- Numerical Features ----
    num_cols = ['Case Size', 'Battery Capacity']
    for col in num_cols:
        df[col] = df[col].str.extract(r'(\d+\.?\d*)').astype(float)

    # KNN Imputation for numerical
    scaler = StandardScaler()
    scaled_num = scaler.fit_transform(df[num_cols])
    num_imp = KNNImputer(n_neighbors=5)
    df[num_cols] = scaler.inverse_transform(num_imp.fit_transform(scaled_num))

    # ---- Categorical Features ----
    cat_cols = ['Brand', 'Model', 'Operating System']
    cat_imp = SimpleImputer(strategy='constant', fill_value='Unknown')
    df[cat_cols] = cat_imp.fit_transform(df[cat_cols])

    # ---- Storage Conversion ----
    if 'Storage Capacity' in df.columns:
        df['Storage (GB)'] = df['Storage Capacity'].replace({'MB': '', 'GB': ''}, regex=True)
        df['Storage (GB)'] = pd.to_numeric(df['Storage (GB)'], errors='coerce')
        df['Storage (GB)'] = np.where(
            df['Storage Capacity'].str.contains('GB', na=False),
            df['Storage (GB)'],
            df['Storage (GB)'] / 1024
        )
        df['Storage (GB)'] = df['Storage (GB)'].fillna(df['Storage (GB)'].median())

    # ---- Final Checks ----
    df.dropna(axis=1, how='all', inplace=True)  # Remove empty columns
    assert df.isna().sum().sum() == 0, "Missing values remain!"

    df.to_csv('cleaned_smart_watches.csv', index=False)
    print(f"Smart watches cleaned. Brands: {df['Brand'].nunique()} unique")


if __name__ == "__main__":
    clean_watches()