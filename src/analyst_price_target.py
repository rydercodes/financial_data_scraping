# analyst_price_target_details.py
from google.cloud import storage
import pandas as pd
import io
import yfinance as yf
from datetime import datetime
import time
import numpy as np

class AnalystPriceTargetDetails:
    def __init__(self, bucket_name, source_blob_name, destination_bucket_name, destination_blob_name):
        self.bucket_name = bucket_name
        self.source_blob_name = source_blob_name
        self.destination_bucket_name = destination_bucket_name
        self.destination_blob_name = destination_blob_name
    
    def download_from_gcs(self):
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.source_blob_name)
        file_content = blob.download_as_bytes()
        return file_content

    def fetch_price_targets(self, ticker, isin):
        try:
            ticker_data = yf.Ticker(ticker)
            info = ticker_data.info

            return {
                'ISIN': isin,
                'Ticker': ticker,
                'Current': info.get('currentPrice'),
                'Average': info.get('targetMeanPrice'),
                'High': info.get('targetHighPrice'),
                'Low': info.get('targetLowPrice'),
                'scraping_url_analysis': f"https://finance.yahoo.com/quote/{ticker}/analysis/",
                'scraping_timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            return None

    def process_data(self):
        # Download the file content from GCS
        file_content = self.download_from_gcs()
        
        # Load the content into a pandas DataFrame
        all_companies = pd.read_parquet(io.BytesIO(file_content))
        
        # List of tickers from the DataFrame
        tickers = all_companies[['Ticker', 'ISIN']].values.tolist()
        
        # Fetch price targets for each ticker with a delay
        analyst_price_target = []
        for ticker, isin in tickers:
            price_data = self.fetch_price_targets(f"{ticker}.MC", isin)
            if price_data:
                analyst_price_target.append(price_data)
            
            # Introduce random delay between API calls (1 to 3 seconds)
            random_int = np.random.choice([1, 2, 3])
            time.sleep(random_int)
        
        # Convert the results into a DataFrame
        analyst_price_target_df = pd.DataFrame(analyst_price_target)
        return analyst_price_target_df

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
        # Process the data and upload to GCS
        analyst_price_target_df = self.process_data()
        self.upload_to_gcs(analyst_price_target_df)