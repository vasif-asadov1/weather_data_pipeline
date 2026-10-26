import os
import requests
import pandas as pd
import boto3
import logging
import sys
import time  # <-- NEW: Added for rate limiting
from io import StringIO
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BronzeIngestionPipeline:
    def __init__(self):
        load_dotenv()
        self.bucket_name = os.getenv('BRONZE_BUCKET_NAME')
        
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('AWS_ENDPOINT_URL'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION'),
            config=Config(s3={'addressing_style': 'path'}) 
        )
        
        self.cities = {
            "Rome_Italy": {"lat": 41.90, "lon": 12.49},
            "Paris_France": {"lat": 48.85, "lon": 2.35},
            "Madrid_Spain": {"lat": 40.41, "lon": -3.70},
            "London_UK": {"lat": 51.50, "lon": -0.12},
            "Oslo_Norway": {"lat": 59.91, "lon": 10.75},
            "Athens_Greece": {"lat": 37.98, "lon": 23.72},
            "Bern_Switzerland": {"lat": 46.94, "lon": 7.44},
            "Brussels_Belgium": {"lat": 50.85, "lon": 4.35},
            "Berlin_Germany": {"lat": 52.52, "lon": 13.40},
            "Moscow_Russia": {"lat": 55.75, "lon": 37.61},
            "Izmir_Turkiye": {"lat": 38.42, "lon": 27.14},
            "Istanbul_Turkiye": {"lat": 41.00, "lon": 28.97},
            "Ankara_Turkiye": {"lat": 39.92, "lon": 32.85},
            "Baku_Azerbaijan": {"lat": 40.40, "lon": 49.86},
            "Tehran_Iran": {"lat": 35.68, "lon": 51.38},
            "Riyadh_SaudiArabia": {"lat": 24.71, "lon": 46.67},
            "Dubai_UAE": {"lat": 25.20, "lon": 55.27},
            "Kabul_Afghanistan": {"lat": 34.52, "lon": 69.17},
            "Islamabad_Pakistan": {"lat": 33.72, "lon": 73.04},
            "NewDelhi_India": {"lat": 28.61, "lon": 77.20},
            "Astana_Kazakhstan": {"lat": 51.16, "lon": 71.47},
            "Beijing_China": {"lat": 39.90, "lon": 116.40},
            "Tokyo_Japan": {"lat": 35.67, "lon": 139.65},
            "NewYork_USA": {"lat": 40.71, "lon": -74.00},
            "Washington_USA": {"lat": 38.90, "lon": -77.03}
        }

    def check_health(self):
        logging.info("Checking connection to S3Mock...")
        try:
            self.s3_client.list_buckets()
            logging.info("✅ S3Mock connection successful!")
        except Exception as e:
            logging.error(f"❌ Failed to connect! Is Docker running? Error: {e}")
            sys.exit(1)

    def setup_storage(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logging.info(f"Bucket '{self.bucket_name}' already exists.")
        except ClientError:
            logging.info(f"Creating bucket '{self.bucket_name}'...")
            self.s3_client.create_bucket(Bucket=self.bucket_name)

    def file_exists_in_s3(self, file_key):
        """NEW: Checks if a file is already in the Bronze layer to avoid duplicate API calls."""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
            return True
        except ClientError:
            return False

    def fetch_historical_weather(self, city_name, lat, lon):
        logging.info(f"Fetching historical data for {city_name}...")
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": "2015-01-01",
            "end_date": "2024-12-31",
            "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
            "timezone": "auto"
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        df = pd.DataFrame(data['daily'])
        df['city'] = city_name
        return df

    def upload_to_s3(self, df, file_name):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=file_name,
            Body=csv_buffer.getvalue()
        )
        logging.info(f"Uploaded {file_name} to Bronze layer.")

    def run(self):
        self.check_health()
        self.setup_storage()
        
        for city, coords in self.cities.items():
            file_path = f"weather/{city}_historical.csv"
            
            # NEW: Resumability check
            if self.file_exists_in_s3(file_path):
                logging.info(f"⏭️ Skipping {city}: Already exists in S3.")
                continue
                
            try:
                df = self.fetch_historical_weather(city, coords['lat'], coords['lon'])
                self.upload_to_s3(df, file_path)
                
                # NEW: Politeness delay to avoid 429 limits
                time.sleep(3) 
                
            except Exception as e:
                logging.error(f"Failed to process {city}: {e}")
                time.sleep(10) # Back off longer if we hit an error
                
        logging.info("Bronze layer ingestion complete!")

if __name__ == "__main__":
    pipeline = BronzeIngestionPipeline()
    pipeline.run()