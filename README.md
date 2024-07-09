# Data-Engineering-Challenge

# Data Extraction
# Task: Automate the downloading of the dataset from the Year 2019.

# Requirements:

# Write a script that downloads CSV files from the Year 2019.Ensure the script can handle network errors and retries.

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

    
