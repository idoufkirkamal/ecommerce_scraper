import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.preprocessing import KBinsDiscretizer, OneHotEncoder
from scipy import stats
import re
from fuzzywuzzy import process, fuzz
import warnings

warnings.filterwarnings('ignore')


def clean_gpu():
    # Chargement des données
    df = pd.read_csv('../../../data/raw/ebay/graphics_cards_results.csv')

    # 1. Nettoyage du prix
    def extract_numeric_price(price):
        """
        Extrait la valeur numérique du prix, en ignorant la devise.
        """
        try:
            # Supprime tous les caractères non numériques (sauf le point pour les décimales)
            numeric_value = re.sub(r'[^\d.]', '', str(price))
            return float(numeric_value)
        except ValueError:
            return np.nan  # Retourne NaN si la conversion échoue

    df['Price'] = df['Price'].apply(extract_numeric_price)

    # 2. Nettoyage du titre avec expressions régulières
    def clean_title(title):
        """
        Supprime les variations inutiles (couleur, taille, édition) du titre.
        """
        # Supprime les informations superflues (couleurs, tailles, éditions, etc.)
        title = re.sub(r'\b(rouge|bleu|noir|vert|blanc|rose|jaune|or|argent|xl|xxl|s|m|l)\b', '', title,
                       flags=re.IGNORECASE)
        title = re.sub(r'\b(\d+gb|\d+go|\d+mo|\d+tb|\d+"|\d+inch|\d+cm|\d+mm)\b', '', title, flags=re.IGNORECASE)
        title = re.sub(r'\b(édition|edition|spéciale|special|limited|standard|pro|plus|max|mini|version)\b', '', title,
                       flags=re.IGNORECASE)
        title = re.sub(r'[^\w\s]', '', title)  # Supprime les caractères spéciaux
        title = re.sub(r'\s+', ' ', title).strip().upper()  # Normalise les espaces et met en majuscules
        return title

    df['Title'] = df['Title'].apply(clean_title)

    # 3. Fuzzy Matching pour les titres similaires
    titles = df['Title'].unique()
    matches = {}
    for title in titles:
        match = process.extractOne(title, titles, scorer=fuzz.token_sort_ratio)
        if match[1] > 90 and match[0] != title:
            matches[title] = match[0]
    df['Title'] = df['Title'].replace(matches)

    # 4. Suppression des doublons après nettoyage
    df = df.drop_duplicates(subset=['Title', 'Price', 'Memory Size'], keep='first')

    # 5. Transformation des données
    brand_mapping = {
        r'\bNVIDIA\b': 'NVIDIA',
        r'\bAMD\b': 'AMD',
        r'\bRADEON\b': 'AMD',
        r'\bATI\b': 'AMD'
    }
    df['Brand'] = df['Brand'].replace(brand_mapping, regex=True)
    df['Brand'] = df['Brand'].fillna('UNKNOWN').str.upper()

    # 6. Remplissage automatique des valeurs manquantes
    num_cols = ['Price', 'Memory Size']
    cat_cols = ['Brand', 'Memory Type']

    # Imputation numérique combinée
    for col in num_cols:
        df[col] = df[col].fillna(df[col].median())  # Médiane par défaut
        df[col] = df[col].interpolate(method='linear')  # Interpolation linéaire

    # Imputation catégorielle avec KNN
    encoder = OneHotEncoder(sparse_output=False)
    encoded_cats = encoder.fit_transform(df[cat_cols])
    imputer = KNNImputer(n_neighbors=3)
    df[cat_cols] = encoder.inverse_transform(imputer.fit_transform(encoded_cats))

    # 7. Gestion des valeurs aberrantes
    z_scores = stats.zscore(df[num_cols])
    df = df[(np.abs(z_scores) < 3).all(axis=1)]  # Suppression des outliers > 3σ

    # 8. Discrétisation et regroupement
    discretizer = KBinsDiscretizer(n_bins=4, encode='ordinal', strategy='quantile')
    df['Price_Category'] = discretizer.fit_transform(df[['Price']]).astype(int)
    df['Memory_Category'] = pd.cut(df['Memory Size'],
                                   bins=[0, 4, 8, 12, 24],
                                   labels=['Small', 'Medium', 'Large', 'X-Large'])

    # 9. Variables indicatrices
    dummies = pd.get_dummies(df['Memory_Category'], prefix='Memory')
    df = pd.concat([df, dummies], axis=1)

    # 10. Remplacement des valeurs spécifiques
    memory_type_corrections = {
        'DDR3': 'GDDR3',
        'DDR5': 'GDDR5',
        'DDR6': 'GDDR6'
    }
    df['Memory Type'] = df['Memory Type'].replace(memory_type_corrections)

    # 11. Permutation aléatoire
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    # Validation finale
    assert df.duplicated().sum() == 0, "Des doublons persistent!"
    assert df.isna().sum().sum() == 0, "Valeurs manquantes résiduelles!"
    assert (df['Price'] > 0).all(), "Prix non valides détectés!"

    df.to_csv('cleaned_graphics_cards_advanced.csv', index=False)
    print(f"Nettoyage terminé. Statistiques finales:\n{df.describe()}")


if __name__ == "__main__":
    clean_gpu()