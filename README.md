# Data-Engineering-Challenge

# 1.Data Extraction
Task: Automate the downloading of the dataset from the Year 2019.
Requirements:Write a script that downloads CSV files from the Year 2019.Ensure the script can handle network errors and retries.

#Function to download a file

import requests
import re
import time
import os


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

#Function to get all CSV file links for the year 2019

def get_csv_links(url, year):
    response = requests.get(url)
    response.raise_for_status()      
    # Use regular expressions to find links ending with '.csv' and containing the year
    csv_links = re.findall(r'href=[\'"]?([^\'" >]+)', response.text)
    csv_links = [link for link in csv_links if f'{year}' in link and link.endswith('.csv')]
    return csv_links

#Main function

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
    
 # 2.Data Processing

Task: Clean and transform the data using Python and Pandas.
 Requirements:

Remove any trips that have missing or corrupt data.
Derive new columns such as trip duration and average speed.
Aggregate data to calculate total trips and average fare per day.

#Function to download a file

import requests
import re
import time
import os
import pandas as pd
from datetime import datetime

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

#Function to get all CSV file links for the year 2019
def get_csv_links(url, year):
    response = requests.get(url)
    response.raise_for_status()
    # Use regular expressions to find links ending with '.csv' and containing the year
    csv_links = re.findall(r'href=[\'"]?([^\'" >]+)', response.text)
    csv_links = [link for link in csv_links if f'{year}' in link and link.endswith('.csv')]
    return csv_links

#Function to clean data and derive new columns
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

#Main function

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
# 3. Data Loading

Task: Load the processed data into an SQLite database.

Requirements:

Design and implement a schema suitable for querying trip metrics.
Use SQL to load data into the database efficiently.

#Schema Design

vendor_id (integer)
tpep_pickup_datetime (datetime)
tpep_dropoff_datetime (datetime)
passenger_count (integer)
trip_distance (float)
rate_code_id (integer)
store_and_fwd_flag (varchar)
payment_type (integer)
fare_amount (float)
extra (float)
mta_tax (float)
tip_amount (float)
tolls_amount (float)
improvement_surcharge (float)
total_amount (float)
pickup_location_id (integer)
dropoff_location_id (integer)
import pandas as pd
from sqlalchemy import create_engine
import os

#Create a connection to the SQLite database (or any other supported database)

engine = create_engine('sqlite:///taxi_data.db')

#SQL to create the taxi_data table

create_table_query = """
CREATE TABLE IF NOT EXISTS taxi_data (
    vendor_id INTEGER,
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count INTEGER,
    trip_distance FLOAT,
    rate_code_id INTEGER,
    store_and_fwd_flag VARCHAR(1),
    payment_type INTEGER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER
);
"""

#Execute the create table query

with engine.connect() as conn:
    conn.execute(create_table_query)

#Directory containing the CSV files

download_dir = 'downloads/2019/'

#Iterate through each CSV file and load it into the database

for file_name in os.listdir(download_dir):
    if file_name.endswith('.csv'):
        file_path = os.path.join(download_dir, file_name)
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        
        # Load the DataFrame into the SQL table
        df.to_sql('taxi_data', engine, if_exists='append', index=False)

print("Data loaded successfully.")

# 4.Data Analysis and Reporting

Task: Generate insights and reports from the database.

Requirements:

Develop SQL queries to answer the following questions:
    What are the peak hours for taxi usage?
    How does passenger count affect the trip fare?
    What are the trends in usage over the year?
Create visualizations to represent the findings.

#--Peak Hours for Taxi Usage
SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour, COUNT(*) AS total_trips
FROM taxi_data
GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime)
ORDER BY total_trips DESC;

#--How Passenger Count Affects the Trip Fare
SELECT passenger_count, AVG(total_amount) AS average_fare
FROM taxi_data
GROUP BY passenger_count
ORDER BY passenger_count;

#--Trends in Usage Over the Year
SELECT DATE(tpep_pickup_datetime) AS date, COUNT(*) AS total_trips
FROM taxi_data
GROUP BY DATE(tpep_pickup_datetime)
ORDER BY date;

#--visualization using python

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

#Load the aggregated data into a DataFrame
df = pd.read_csv('aggregated_data_2019.csv')

#Create a connection to the SQLite database (or any other supported database)
engine = create_engine('sqlite:///taxi_data.db')

#Load the CSV data into the database for SQL querying
df.to_sql('taxi_data', engine, if_exists='replace', index=False)

#Query to find peak hours for taxi usage

peak_hours_query = """
SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour, COUNT(*) AS total_trips
FROM taxi_data
GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime)
ORDER BY total_trips DESC;
"""
peak_hours = pd.read_sql_query(peak_hours_query, engine)

#Query to find how passenger count affects the trip fare

passenger_fare_query = """
SELECT passenger_count, AVG(total_amount) AS average_fare
FROM taxi_data
GROUP BY passenger_count
ORDER BY passenger_count;
"""
passenger_fare = pd.read_sql_query(passenger_fare_query, engine)

#Query to find trends in usage over the year

trends_query = """
SELECT DATE(tpep_pickup_datetime) AS date, COUNT(*) AS total_trips
FROM taxi_data
GROUP BY DATE(tpep_pickup_datetime)
ORDER BY date;
"""
trends = pd.read_sql_query(trends_query, engine)

#Visualization for peak hours for taxi usage
plt.figure(figsize=(12, 6))
sns.barplot(x='hour', y='total_trips', data=peak_hours, palette='viridis')
plt.title('Peak Hours for Taxi Usage')
plt.xlabel('Hour of Day')
plt.ylabel('Total Trips')
plt.xticks(rotation=45)
plt.show()

#Visualization for passenger count vs. average fare
plt.figure(figsize=(12, 6))
sns.barplot(x='passenger_count', y='average_fare', data=passenger_fare, palette='coolwarm')
plt.title('Passenger Count vs. Average Fare')
plt.xlabel('Passenger Count')
plt.ylabel('Average Fare ($)')
plt.xticks(rotation=45)
plt.show()

#Visualization for trends in usage over the year
plt.figure(figsize=(14, 7))
plt.plot(trends['date'], trends['total_trips'], marker='o', linestyle='-')
plt.title('Trends in Taxi Usage Over the Year')
plt.xlabel('Date')
plt.ylabel('Total Trips')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
