import boto3
import pandas as pd
from io import StringIO
import os 
from datetime import datetime
from google.cloud import storage
import sys
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

class bike_share_data_clean:
    """Class to clean bike share data from S3"""
    def __init__(self, bucket_name, df, 
                 upload_to_s3=False, 
                 upload_to_GCS=False):
        self.s3 = boto3.client('s3', 
                               aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                               aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
        print(os.getenv('AWS_ACCESS_KEY_ID'))
        print(os.getenv('AWS_SECRET_ACCESS_KEY'))
        print(os.getenv('GCS_BUCKET_NAME'))
        self.bucket_name = os.getenv('GCS_BUCKET_NAME') or bucket_name  # Use GCS bucket name if available
        self.df = df  # Initialize the DataFrame in __init__
        self.upload_to_s3 = upload_to_s3  # Flag to control S3 upload
        self.upload_to_GCS = upload_to_GCS  # Flag to control GCS upload
        
        self.credentials_path = os.path.join(os.path.dirname(__file__), 'GOOGLE_APPLICATION_CREDENTIALS.json')
        self.credentials = storage.Client.from_service_account_json(self.credentials_path)
        print(f"Credentials path: {self.credentials_path}")


    
    def clean_data(self, df):
        """Basic data cleaning"""
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        #df = df.dropna(subset=['important_column'])  # Replace with your key column
        
        # Standardize text columns
        text_cols = df.select_dtypes(include=['object']).columns

        # Strip whitespace and convert to lowercase
        for col in text_cols:
            df[col] = df[col].str.strip().str.lower()
        
        # Cleaning 'Trip Id' column which appears twice in the dataset
        df['Trip Id'] = df['Trip Id'].fillna(value = df['ï»¿Trip Id'])
        df['Trip Id'] = [int(i) for i in df['Trip Id']]
        df = df.drop(columns = ['ï»¿Trip Id'])
        
        # Convert date columns to datetime format
        df['Start Time'] = pd.to_datetime(df['Start Time'],
                                               format = "%m/%d/%Y %H:%M") 
        
        df['End Time'] = pd.to_datetime(df['End Time'],
                                               format = "%m/%d/%Y %H:%M") 
                                               
        df['ym_id'] = df['Start Time'].dt.strftime('%Y-%m-%d')
        
        df['End Station Id'] = df['End Station Id'].fillna(9999)
        df['Start Station Id'] = df['Start Station Id'].fillna(9999)
        
        if df['End Station Id'].dtype is not int:
            df['End Station Id'] = df['End Station Id'].astype(int)

        col_dict = {'Start Station Id': 'Start_Station_Id', 
                    'End Station Id': 'End_Station_Id', 
                    'Trip Id': 'Trip_Id',
                    'Start Time': 'Start_Time',
                    'End Time': 'End_Time',
                    'Start Station Name': 'Start_Station_Name',
                    'End Station Name': 'End_Station_Name',
                    'Start Station Latitude': 'Start_Station_Latitude',
                    'Start Station Longitude': 'Start_Station_Longitude',
                    'End Station Latitude': 'End_Station_Latitude',
                    'End Station Longitude': 'End_Station_Longitude',
                    'statation_id': 'station_Id',
                    'Bike Id': 'Bike_Id',
                    'User Type': 'User_Type',
                    'Trip  Duration': 'Trip_Duration'
                    }
        
        df = df.rename(columns=col_dict)

        return df

    
    def save_to_s3(self, df, key, df_name):
        if self.upload_to_s3:
            df.to_csv('{}.csv'.format(df_name), sep=',', index=False)
            self.s3.upload_file('{}.csv'.format(df_name),self.bucket_name,'{}.csv'.format(key))
            print("Data Uploaded to S3")
        else: 
            print("Upload to S3 is disabled. Data not uploaded.")
            
    def save_to_GCS(self, df, key, df_name):
        """Save DataFrame to Google Cloud Storage"""
        if self.upload_to_GCS: 
            print('Initializing GCS client...')
            client = storage.Client.from_service_account_json(self.credentials_path)
            print('GCS client initialized.')
            
            bucket = client.bucket(self.bucket_name)
            print(f"bucket initialized: {bucket.name}")
            if isinstance(df, pd.DataFrame) == True: 
                blob = bucket.blob(f'{key}.csv')
                df.to_csv('{}.csv'.format(df_name), sep=',', index=False)
                print(f"Saving data to GCS at {key}/{df_name}.csv")
                blob.upload_from_filename('{}.csv'.format(df_name), 'text/csv')
                print(f"Data saved to GCS at {key}/{df_name}.csv")
            else: 
                blob = bucket.blob(f'{key}.json)')
                blob.upload_from_filename('{}.json'.format(key), 'application/json')
                print("JSON Data Uploaded to GCS")
            

        else: 
            print("Upload to GCS is disabled. Data not uploaded.")
