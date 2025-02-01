import pandas as pd
import re

# Charger le fichier CSV
file_path = r'C:\Users\AdMin\Desktop\ecommerce_scraper\data\raw\flipkart\laptops\laptops_2025_01_29_scrape1.csv'
df = pd.read_csv(file_path)

# Afficher les colonnes du DataFrame
print(df.columns)

# Fonctions pour extraire les informations
def clean_title(title):
    return re.sub(
        r'(?i)\b(8GB|. |PC|Notebook|Ryzen|UHD|Graphics|DDR4|AMD|W11|Win11|Win|11|Cond|!!|LOADED|TouchBar|Mac OS|Black| i3StorageWin|Gaming|Laptop|Touchscreen|Pro|15.6|Windows|RTX|FHD|LaptopWin11|HDD| ,|French|13inch|'
        r' - | /|macOS|VENTURA|FREE|SHIPPIN|i9|13.3|inches|TURBO|"|- | , |13INCH|EXCELLENT|'
        r'REFURBISHED|NEW|MWTK2LL|Qwerty|Spanish|Keyboard|British|\d+GB|\d+TB|[\d\.]+ ?GHz| GB |'
        r'rouge|Gray|BIG SUR|WEBCAM|WIFI|BLUETOOTHGB|TB|space gray|silver|gold|touch bar|GHz|'
        r'Intel|Core|i7|th|Gen|GB|Very|RAM|i5| GB| TB|GB GB|.GHZ| CPU | GPU|-|SSD|256|512|Good|'
        r'Condition|magic keyboard|✅|🔋|grade [A-B]|warranty\.\.\.)',
        '',
        str(title)
    ).strip().replace('  ', '')



def extract_price(row):
    price = row['price']
    if pd.isna(price):
        # Extraire le prix à partir de 'Sales Package' si le prix est manquant
        match = re.search(r'₹(\d{1,3}(?:,\d{3})*\.\d{2})', row['Sales Package'])
        if match:
            price = match.group(1)

    # Tentative de conversion du prix en dollars
    try:
        if isinstance(price, str):
            price = float(price.replace(',', '').replace('₹', '')) / 80  # Taux de change approximatif ₹1 = $0.0125
        else:
            price = float(price) / 80  # Assurer une conversion même si pas string
    except ValueError:
        # Si la conversion échoue, attribuer None ou une autre valeur par défaut
        price = None

    return price

def extract_ram(row):
    ram = row['RAM']
    if pd.isna(ram):
        sales_package = row['Sales Package']
        if isinstance(sales_package, str):
            match = re.search(r'(\d+)\s*GB', sales_package)
            if match:
                ram = match.group(1)
    return ram

def extract_cpu(row):
    cpu = row['Processor Name']
    if pd.isna(cpu):
        sales_package = row['Sales Package']
        if isinstance(sales_package, str):
            match = re.search(r'Intel\s*(Core\s*i\d+)\s*|\s*AMD\s*(Ryzen\s*\d+\s*Core)', sales_package)
            if match:
                cpu = match.group(0)
    return cpu

def extract_model(row):
    model = row['Model Name']
    if pd.isna(model):
        sales_package = row['Sales Package']
        if isinstance(sales_package, str):
            match = re.search(r'Model Name=\s*(.+)', sales_package)
            if match:
                model = match.group(1)
    return model

def extract_brand(row):
    brand = None
    # Essayez d'extraire la marque à partir de plusieurs colonnes potentielles
    for col in ['title', 'Sales Package']:
        value = row[col]
        if isinstance(value, str):
            match = re.search(r'(ASUS|Lenovo|HP|Samsung|Acer|MSI|Apple|Dell|Zebronics|Thomson|Infinix|Jio|Ultimus)', value)
            if match:
                brand = match.group(1)
                break
    return brand

def extract_gpu(row):
    gpu = row['Graphic Processor']
    if pd.isna(gpu):
        sales_package = row['Sales Package']
        if isinstance(sales_package, str):
            match = re.search(r'NVIDIA\s*(GeForce\s*.+?)\s*|Intel\s*(Integrated\s*.+?)\s*', sales_package)
            if match:
                gpu = match.group(0)
    return gpu

def extract_screen_size(row):
    screen_size = row['Screen Size']
    if pd.isna(screen_size):
        sales_package = row['Sales Package']
        if isinstance(sales_package, str):
            match = re.search(r'(\d+\.\d+)\s*inch', sales_package)
            if match:
                screen_size = match.group(1)
    # Extraire la taille de l'écran en inch
    if isinstance(screen_size, str):
        match = re.search(r'(\d+\.\d+)\s*inch', screen_size)
        if match:
            screen_size = float(match.group(1))
        else:
            match = re.search(r'(\d+\.\d+)\s*Inch', screen_size)
            if match:
                screen_size = float(match.group(1))
    return screen_size

def extract_storage(row):
    storage = row['SSD Capacity']
    if pd.isna(storage):
        sales_package = row['Sales Package']
        if isinstance(sales_package, str):
            match = re.search(r'(\d+)\s*GB', sales_package)
            if match:
                storage = match.group(1)
            else:
                match = re.search(r'(\d+)\s*TB', sales_package)
                if match:
                    storage = int(match.group(1)) * 1024  # Convertir TB en GB
    # Convertir la valeur de stockage en nombre
    if isinstance(storage, str):
        match_gb = re.search(r'(\d+)\s*GB', storage)
        if match_gb:
            storage = int(match_gb.group(1))
        else:
            match_tb = re.search(r'(\d+)\s*TB', storage)
            if match_tb:
                storage = int(match_tb.group(1)) * 1024  # Convertir TB en GB
    return storage

# Appliquer les fonctions pour extraire les informations
df['Title'] = df.apply(clean_title, axis=1)
df['Price'] = df.apply(extract_price, axis=1)
df['RAM'] = df.apply(extract_ram, axis=1)
df['CPU'] = df.apply(extract_cpu, axis=1)
df['Model'] = df.apply(extract_model, axis=1)
df['Brand'] = df.apply(extract_brand, axis=1)
df['GPU'] = df.apply(extract_gpu, axis=1)
df['Screen Size'] = df.apply(extract_screen_size, axis=1)
df['Storage'] = df.apply(extract_storage, axis=1)

# Filtrer les colonnes pertinentes
columns_to_keep = ['Title', 'Price', 'RAM', 'CPU', 'Model', 'Brand', 'GPU', 'Screen Size', 'Storage']
df_cleaned = df[columns_to_keep]

# Supprimer les lignes avec des données manquantes
df_cleaned.dropna(inplace=True)

# Afficher le DataFrame nettoyé
print(df_cleaned.head())

# Sauvegarder le DataFrame nettoyé dans un nouveau fichier CSV
output_file_path = 'laptops_cleaned.csv'
df_cleaned.to_csv(output_file_path, index=False)