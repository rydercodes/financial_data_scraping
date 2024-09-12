# marketcap_details.py
from google.cloud import storage
import pandas as pd
import io
import yfinance as yf
from datetime import datetime
import time
import numpy as np

class MarketCapDetails:
    def __init__(self, source_bucket_name, source_blob_name, destination_bucket_name, destination_blob_name):
        self.source_bucket_name = source_bucket_name
        self.source_blob_name = source_blob_name
        self.destination_bucket_name = destination_bucket_name
        self.destination_blob_name = destination_blob_name

    def download_from_gcs(self):
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.source_bucket_name)
        blob = bucket.blob(self.source_blob_name)
        file_content = blob.download_as_bytes()
        return file_content

    def fetch_market_cap(self, ticker, isin, exchange_suffix='.MC'):
        try:
            ticker_with_suffix = ticker + exchange_suffix
            ticker_data = yf.Ticker(ticker_with_suffix)
            info = ticker_data.info

            return {
                'ISIN': isin,
                'Ticker': ticker,
                'Exchange': exchange_suffix.strip('.'),  # Clean the suffix
                'MarketCap': info.get('marketCap'),
                'scraping_url_analysis': f"https://finance.yahoo.com/quote/{ticker_with_suffix}/analysis/",
                'scraping_timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            return None

    def upload_to_gcs(self, df):
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.destination_bucket_name)
        blob = bucket.blob(self.destination_blob_name)

        # Convert DataFrame to a BytesIO buffer
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Upload the buffer to GCS
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        print(f"Data uploaded to {self.destination_blob_name} in bucket {self.destination_bucket_name}.")

    def run(self):
        # Download the file content from GCS
        file_content = self.download_from_gcs()

        # Load the content into a pandas DataFrame
        all_companies = pd.read_parquet(io.BytesIO(file_content))

        # List of tickers from the DataFrame
        tickers = all_companies[['Ticker', 'ISIN']].values.tolist()

        # Fetch market cap for each ticker with a delay
        marketcap_details = []
        for ticker, isin in tickers:
            data = self.fetch_market_cap(ticker, isin)
            if data:
                marketcap_details.append(data)

            # Introduce random delay between API calls
            random_int = np.random.choice([1, 2, 3])
            time.sleep(random_int)

        # Convert the results into a DataFrame
        marketcap_details_df = pd.DataFrame(marketcap_details)

        # Upload to GCS
        self.upload_to_gcs(marketcap_details_df)