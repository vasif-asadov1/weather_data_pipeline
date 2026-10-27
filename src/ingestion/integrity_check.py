import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

# Connect to an in-memory DuckDB instance
con = duckdb.connect(':memory:')

# Load the required extensions to talk to S3
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Configure DuckDB to talk to our local S3Mock container
# Note: DuckDB uses slightly different variable names than boto3
con.execute(f"""
    SET s3_endpoint='127.0.0.1:9090';
    SET s3_access_key_id='test';
    SET s3_secret_access_key='test';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

# Let's query the Rome file directly from S3!
print("Checking Data Integrity for Rome...")
result = con.execute("""
    SELECT 
        city,
        COUNT(*) as total_days,
        MIN(time) as start_date,
        MAX(time) as end_date,
        COUNT(temperature_2m_max) as temp_max_records,
        COUNT(precipitation_sum) as precipitation_records
    FROM read_csv_auto('s3://weather-bronze/weather/Rome_Italy_historical.csv')
    GROUP BY city
""").df()

print(result.to_markdown(index=False))