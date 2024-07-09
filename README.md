# Data-Engineering-Challenge

# Data Extraction
Task: Automate the downloading of the dataset from the Year 2019.
Requirements:Write a script that downloads CSV files from the Year 2019.Ensure the script can handle network errors and retries.

# 
import requests
import re
import time
import os

# Function to download a file
def download_file(url, destination, retries=5, backoff_factor=0.3):
    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Will raise an HTTPError for bad responses
            with open(destination, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192): 
                    f.write(chunk)
            print(f"Downloaded {destination}")
            return True
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed with error: {e}")
            attempt += 1
            time.sleep(backoff_factor * (2 ** attempt))  # Exponential backoff
    return False

# Function to get all CSV file links for the year 2019
def get_csv_links(url, year):
    response = requests.get(url)
    response.raise_for_status()      
    # Use regular expressions to find links ending with '.csv' and containing the year
    csv_links = re.findall(r'href=[\'"]?([^\'" >]+)', response.text)
    csv_links = [link for link in csv_links if f'{year}' in link and link.endswith('.csv')]
    return csv_links

# Main function
def main():
    base_url = 'https://www.nyc.gov'
    page_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    download_dir = 'downloads/2019/'

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    csv_links = get_csv_links(page_url, 2019)
    
    for link in csv_links:
        if not link.startswith('http'):
            full_url = base_url + link
        else:
            full_url = link
        file_name = os.path.join(download_dir, link.split('/')[-1])
        download_file(full_url, file_name)

if __name__ == "__main__":
    main()

    # Data Processing

Task: Clean and transform the data using Python and Pandas.
 Requirements:

Remove any trips that have missing or corrupt data.
Derive new columns such as trip duration and average speed.
Aggregate data to calculate total trips and average fare per day.

#  
import requests
import re
import time
import os
import pandas as pd
from datetime import datetime

# Function to download a file
def download_file(url, destination, retries=5, backoff_factor=0.3):
    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Will raise an HTTPError for bad responses
            with open(destination, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192): 
                    f.write(chunk)
            print(f"Downloaded {destination}")
            return True
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed with error: {e}")
            attempt += 1
            time.sleep(backoff_factor * (2 ** attempt))  # Exponential backoff
    return False

# Function to get all CSV file links for the year 2019
def get_csv_links(url, year):
    response = requests.get(url)
    response.raise_for_status()
    # Use regular expressions to find links ending with '.csv' and containing the year
    csv_links = re.findall(r'href=[\'"]?([^\'" >]+)', response.text)
    csv_links = [link for link in csv_links if f'{year}' in link and link.endswith('.csv')]
    return csv_links

# Function to clean data and derive new columns
def process_data(file_path):
    df = pd.read_csv(file_path)
    
    # Remove rows with missing or corrupt data
    df.dropna(inplace=True)
    
    # Convert columns to appropriate data types
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    # Derive new columns
    df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60  # in minutes
    df['average_speed'] = df['trip_distance'] / (df['trip_duration'] / 60)  # in miles per hour
    
    # Aggregate data to calculate total trips and average fare per day
    df['pickup_date'] = df['tpep_pickup_datetime'].dt.date
    aggregated_data = df.groupby('pickup_date').agg(total_trips=('VendorID', 'count'), average_fare=('fare_amount', 'mean')).reset_index()
    
    return aggregated_data

# Main function
def main():
    base_url = 'https://www.nyc.gov'
    page_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    download_dir = 'downloads/2019/'
    output_file = 'aggregated_data_2019.csv'

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    csv_links = get_csv_links(page_url, 2019)
    
    all_aggregated_data = pd.DataFrame()
    
    for link in csv_links:
        if not link.startswith('http'):
            full_url = base_url + link
        else:
            full_url = link
        file_name = os.path.join(download_dir, link.split('/')[-1])
        
        if download_file(full_url, file_name):
            aggregated_data = process_data(file_name)
            all_aggregated_data = pd.concat([all_aggregated
