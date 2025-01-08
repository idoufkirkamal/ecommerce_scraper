import requests as req
from bs4 import BeautifulSoup as bs
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
import time
import random

# Amazon Product Advertising API credentials
ACCESS_KEY = 'YOUR_ACCESS_KEY'
SECRET_KEY = 'YOUR_SECRET_KEY'
ASSOCIATE_TAG = 'YOUR_ASSOCIATE_TAG'

# Initialize the Amazon Product Advertising API client
client = boto3.client(
    'advertising',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name='us-east-1'
)

# List of proxies
proxies = [
    'http://proxy1.example.com:8080',
    'http://proxy2.example.com:8080',
    'http://proxy3.example.com:8080'
]

# Function to get product details
def get_product_details(keywords):
    try:
        response = client.search_items(
            Keywords=keywords,
            SearchIndex='All',
            Resources=['ItemInfo.Title', 'Offers.Listings.Price']
        )
        item = response['SearchResult']['Items'][0]
        title = item['ItemInfo']['Title']['DisplayValue']
        price = item['Offers']['Listings'][0]['Price']['DisplayAmount']
        return title, price
    except NoCredentialsError:
        print("Credentials not available")
        return None, None
    except KeyError:
        print("Key not found in response")
        return None, None

# Function to get product details with delay and proxy rotation
def get_product_details_with_delay_and_proxy(keywords):
    proxy = random.choice(proxies)
    print(f"Using proxy: {proxy}")
    title, price = get_product_details(keywords)
    time.sleep(random.uniform(1, 5))  # Random delay between 1 and 5 seconds
    return title, price

# Get product details for 'iphone 11'
title, price = get_product_details_with_delay_and_proxy('iphone 11')

# Display the results
print("Titre :", title)
print("Prix :", price)