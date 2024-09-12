# historical_data_details.py
from google.cloud import storage
import pandas as pd
import io
import yfinance as yf
from datetime import datetime
import time
import numpy as np

class HistoricalDataDetails:
    def __init__(self, bucket_name, source_blob_name, destination_bucket_name, destination_blob_name):
        self.bucket_name = bucket_name
        self.source_blob_name = source_blob_name
        self.destination_bucket_name = destination_bucket_name
        self.destination_blob_name = destination_blob_name  # Added parameter
        self.scraping_url = "https://finance.yahoo.com/quote/{ticker}/history/"  # Fixed URL pattern
        self.scraping_timestamp = datetime.now().isoformat()  # Current timestamp
    
    def download_from_gcs(self):
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.source_blob_name)
        file_content = blob.download_as_bytes()
        return file_content

    def fetch_historical_data(self, ticker, exchange_suffix):
        try:
            ticker_with_suffix = ticker + exchange_suffix  # Add dynamic suffix to the ticker
            data = yf.download(ticker_with_suffix, period="max")
            data = data.reset_index()  # Reset index to move date from index to column
            return data
        except Exception as e:
            print(f"Error downloading data for {ticker}: {e}")
            return pd.DataFrame()

    def upload_to_gcs(self, df, destination_blob_name):
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.destination_bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Convert DataFrame to a BytesIO buffer
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Upload the buffer to GCS
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        print(f"Data uploaded to {destination_blob_name} in bucket {self.destination_bucket_name}.")

    def process_data(self):
        # Download the file content from GCS
        file_content = self.download_from_gcs()
        
        # Load the content into a pandas DataFrame
        all_companies = pd.read_parquet(io.BytesIO(file_content))
        
        # Loop through the 'Ticker' column in all_companies DataFrame
        for idx, row in all_companies.iterrows():
            ticker = row['Ticker']
            isin = row['ISIN']
            exchange_suffix = row.get('Exchange_Suffix', '.MC')  # Get exchange suffix from the DataFrame or use default

            # Fetch historical data for the current ticker
            df = self.fetch_historical_data(ticker, exchange_suffix)

            # If data is retrieved, add additional columns
            if not df.empty:
                # Add ISIN, exchange, and ticker columns
                df['ISIN'] = isin
                df['Exchange'] = exchange_suffix.strip('.')  # Remove leading dot if present
                df['Ticker'] = ticker  # Save the ticker without the suffix

                # Add URL and timestamp columns
                df['scraping_url'] = self.scraping_url.format(ticker=ticker)
                df['scraping_timestamp'] = self.scraping_timestamp

                # Reorder columns to place 'Exchange', 'Ticker', 'ISIN' at the beginning
                columns_order = ['Ticker', 'Exchange', 'ISIN'] + [col for col in df.columns if col not in ['Ticker', 'Exchange', 'ISIN', 'scraping_url', 'scraping_timestamp']] + ['scraping_url', 'scraping_timestamp']
                df = df[columns_order]

                # Define GCS destination path without the suffix
                destination_blob_name = f"data/history/{ticker}.parquet"

                # Upload DataFrame to GCS
                self.upload_to_gcs(df, destination_blob_name)

                print(f"Uploaded historical data for {ticker} to {destination_blob_name}")

            # Introduce random delay between API calls (1 to 3 seconds)
            random_int = np.random.choice([1, 2, 3])
            time.sleep(random_int)

    def run(self):
        self.process_data()