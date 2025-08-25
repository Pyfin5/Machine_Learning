# Import necessary libraries
import pandas as pd 
import os 
import datetime as dt
import requests 
import zipfile
import io 
import plotly.express as px
import matplotlib.pyplot as plt 
import time
import math
import itertools
import data_cleaning  # Import the class directly
from glob import glob
import psutil  # For memory monitoring
import gc  # For garbage collection
import json


 

class Get_BikeShareData:    
    """Class to interact with the Bike Share API and process data"""
    def __init__(self, base_url="https://ckan0.cf.opendata.inter.prod-toronto.ca",
                 station_url='https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_information'):
        """Initialize BikeShareAPI with base URLs for data access"""
        self.base_url = base_url
        self.station_url = station_url
        self.schema_path = os.path.join(os.path.dirname(__file__), 'table_schema.json')

    def bikeshare_api(self, limit=2):
        """Retrieve bike share data URLs from the API"""
        url = f"{self.base_url}/api/3/action/package_show"
        params = {"id": "bike-share-toronto-ridership-data"}
        package = requests.get(url, params=params).json()
        meta_data = []

        for resource in package["result"]["resources"]:
            if not resource["datastore_active"]:
                url = f"{self.base_url}/api/3/action/resource_show?id={resource['id']}"
                resource_metadata = requests.get(url).json()
                meta_data.append(resource_metadata['result']['url'])

        return meta_data[-limit:]  # Return last n URLs

    def api_download(self, urls):
        """Download data from provided URLs"""
        return [requests.get(url) for url in urls]

    def save_zip(self, responses):
        """Save and process zip files from responses"""
        zip_files = []
        for response in responses:
            z = zipfile.ZipFile(io.BytesIO(response.content))
            zip_files.append(z)
        return zip_files

    def data_load_chunked(self, zip_file, chunk_size=50000):
        """Load data from zip file into pandas DataFrame using chunked processing"""
        all_chunks = []
        
        for filename in zip_file.namelist():
            print(f"Processing file: {filename}")
            
            # Read CSV in chunks
            chunk_iter = pd.read_csv(zip_file.open(filename), encoding='cp1252', chunksize=chunk_size)
            
            for i, chunk in enumerate(chunk_iter):
                all_chunks.append(chunk)
                print(f"Loaded chunk {i+1} from {filename}")
                
                # Monitor memory usage
                memory_usage = psutil.virtual_memory().percent
                print(f"Memory usage: {memory_usage:.1f}%")
                
                # Force garbage collection if memory usage is high
                if memory_usage > 80:
                    gc.collect()
                    print("Garbage collection performed due to high memory usage")
        
        # Combine all chunks
        if all_chunks:
            result = pd.concat(all_chunks, ignore_index=True)
            del all_chunks
            gc.collect()
            return result
        else:
            return pd.DataFrame()

    def data_load(self, zip_file):
        """Load data from zip file into pandas DataFrame - optimized version"""
        # Check available memory
        available_memory = psutil.virtual_memory().available / (1024**3)  # GB
        print(f"Available memory: {available_memory:.2f} GB")
        
        # Use chunked loading for large files
        if available_memory < 2:  # Less than 2GB available
            print("Low memory detected. Using chunked loading...")
            return self.data_load_chunked(zip_file, chunk_size=25000)
        else:
            # Original method for smaller datasets
            data_appended = pd.concat(
                [pd.read_csv(zip_file.open(i), encoding='cp1252') for i in zip_file.namelist()],
                ignore_index=True
            )
            return data_appended

    def get_stations(self):
        """Retrieve station information"""
        response = requests.get(self.station_url).json()
        stations = []

        for station in response['data']['stations']:
            stations.append([
                station['station_id'],
                station['lat'],
                station['lon']
            ])

        stations_df = pd.DataFrame(stations, columns=['station_id', 'lat', 'lon'])
        stations_df['station_id'] = stations_df['station_id'].astype(int)
        return stations_df

    def get_complete_data(self, limit=2):
        """Convenience method to get all data in one call - optimized"""
        print("Starting data download and processing...")
        
        # Get URLs
        urls = self.bikeshare_api(limit=limit)
        print(f"Found {len(urls)} data sources")
        
        # Download responses
        responses = self.api_download(urls)
        print("Downloaded all responses")
        
        # Process zip files
        zips = self.save_zip(responses)
        print(f"Processed {len(zips)} zip files")
        
        # Load all data with memory monitoring
        data_append = []
        for i, z in enumerate(zips):
            print(f"Processing zip file {i+1}/{len(zips)}")
            data = self.data_load(z)
            data_append.append(data)
            
            # Monitor memory after each file
            memory_usage = psutil.virtual_memory().percent
            print(f"Memory usage after file {i+1}: {memory_usage:.1f}%")
            
            # Force garbage collection
            gc.collect()
        
        # Combine all data
        if data_append:
            result = pd.concat(data_append, ignore_index=True)
            del data_append
            gc.collect()
            print(f"Final dataset size: {len(result)} rows")
            return result
        else:
            return pd.DataFrame()

    def load_schema(self):
        """Load table schema from JSON file"""
        try:
            with open(self.schema_path, 'r') as f:
                schema = json.load(f)
            print(f"Schema loaded successfully from {self.schema_path}")
            return schema
        except FileNotFoundError:
            print(f"Schema file not found at {self.schema_path}")
            return None

def main():
    """Main function with memory monitoring and error handling"""
    try:
        print("=== Bike Share Data Processor (Optimized) ===")
        print(f"Initial memory usage: {psutil.virtual_memory().percent:.1f}%")
        
        # Initialize the API client
        api = Get_BikeShareData()
        
        # Get URLs
        jsons = api.bikeshare_api(limit=2)
        print(f"Retrieved {len(jsons)} data sources")
        
        # Get the data with memory monitoring
        print("Downloading and processing data...")
        data = api.get_complete_data(limit=2)
        
        if data.empty:
            print("No data retrieved. Exiting.")
            return
        
        print(f"Downloaded {len(data)} rides")
        print(f"Memory usage after data download: {psutil.virtual_memory().percent:.1f}%")
        
        # Get station information
        print("Retrieving station information...")
        stations = api.get_stations()
        print(f"Found {len(stations)} stations")
        
        # Initialize the data cleaner and clean the data
        print("Cleaning data...")
        cleaner = data_cleaning.bike_share_data_clean(bucket_name=os.getenv('S3_BUCKET_NAME'), 
                                                      df=data,
                                                      upload_to_s3 = False,
                                                      upload_to_GCS = True)  # Set to True to upload to S3
        print(os.getenv('S3_BUCKET_NAME'))
        # Clean data with memory monitoring
        table_schema = api.load_schema()
        print('Schema loaded:', table_schema)   

        data = cleaner.clean_data(data)
        print(f"Data cleaned. Final size: {len(data)} rows")
        print(f"Memory usage after cleaning: {psutil.virtual_memory().percent:.1f}%")
        
        print('Data columns datatypes:', data.dtypes)
        print('stations columns datatypes:', stations.dtypes)
        # Save to S3
        print("data columns:", data.columns)
        print("Saving data to S3...")
        cleaner.save_to_s3(df=data, key='bike_share_data', df_name='bike_share_data')
        cleaner.save_to_s3(df=stations, key='stations_data', df_name='stations_data')
        
        print("Saving data to GCS...")
        cleaner.save_to_GCS(df=data, key='bike_share_data', df_name='bike_share_data')
        cleaner.save_to_GCS(df=stations, key='stations_data', df_name='stations_data')
        cleaner.save_to_GCS(df=table_schema, key='table_schema', df_name='table_schema')
        

        data[:1000].to_csv('bike_share_data_sample.csv', index=False)
        print("=== Processing Complete ===")
        print(f"Final memory usage: {psutil.virtual_memory().percent:.1f}%")
                
    except Exception as e:
        print(f"An error occurred: {e}")
        print("Memory usage at error:", psutil.virtual_memory().percent)
        
if __name__ == "__main__":
    main() 
    

