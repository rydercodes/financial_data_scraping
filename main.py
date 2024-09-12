import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'config')))


import os

# main.py
from companies_details import CompaniesDetails
from marketcap_details import MarketCapDetails
from historical_data_details import HistoricalDataDetails
from analyst_price_target import AnalystPriceTargetDetails
from load_data import BigQueryLoader
from google.cloud import storage
from google.oauth2 import service_account


# Set the environment variable for the Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/financial_data_scraping/jaber-financial-b20f23e23588.json'

# Verify that it has been set correctly
print("GOOGLE_APPLICATION_CREDENTIALS:", os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))



def main():

    # Processing Companies Details
    gcs_credentials_path = '/workspaces/financial_data_scraping/jaber-financial-b20f23e23588.json'
    project_id = 'jaber-financial'
    dataset_id = 'financial_data'
    bucket_name = 'companies_details'

    urls = [
        "https://www.bolsasymercados.es/bme-exchange/en/Prices-and-Markets/Shares/Main-Market/Listed-Companies",
        "https://www.bolsasymercados.es/bme-exchange/en/Prices-and-Markets/Shares/Main-Market/Listed-Companies?page=2"
    ]
    local_file_path = '/workspaces/financial_data_scraping/data/companies_details.parquet'

    companies_details = CompaniesDetails(
        gcs_credentials_path=gcs_credentials_path,
        project_id=project_id,
        dataset_id=dataset_id,
        bucket_name=bucket_name
    )
    companies_details.process(urls, local_file_path)

    # Processing Historical Data Details
    source_bucket_name_hist = 'companies_details'
    source_blob_name_hist = 'data/companies_details.parquet'
    destination_bucket_name_hist = 'historical_data_details'
    destination_blob_name_hist = 'data/historical_data_details.parquet'

    historical_data = HistoricalDataDetails(
        bucket_name=source_bucket_name_hist,
        source_blob_name=source_blob_name_hist,
        destination_bucket_name=destination_bucket_name_hist,
        destination_blob_name=destination_blob_name_hist  # Updated parameter
    )
    historical_data.run()

    # Processing Analyst Price Target Details
    source_bucket_name_analyst = 'companies_details'
    source_blob_name_analyst = 'data/companies_details.parquet'
    destination_bucket_name_analyst = 'analyst_price_target'
    destination_blob_name_analyst = 'data/analyst_price_target.parquet'

    analyst_price_target = AnalystPriceTargetDetails(
        bucket_name=source_bucket_name_analyst,
        source_blob_name=source_blob_name_analyst,
        destination_bucket_name=destination_bucket_name_analyst,
        destination_blob_name=destination_blob_name_analyst
    )
    analyst_price_target.run()

    # Processing Market Cap Details
    source_bucket_name_marketcap = 'companies_details'
    source_blob_name_marketcap = 'data/companies_details.parquet'
    destination_bucket_name_marketcap = 'marketcap_details'
    destination_blob_name_marketcap = 'data/marketcap_details.parquet'

    marketcap_details = MarketCapDetails(
        source_bucket_name=source_bucket_name_marketcap,
        source_blob_name=source_blob_name_marketcap,
        destination_bucket_name=destination_bucket_name_marketcap,
        destination_blob_name=destination_blob_name_marketcap
    )
    marketcap_details.run()

    # Loading Data into BigQuery
    project_id = 'jaber-financial'
    dataset_id = 'financial_data'

    # Load Companies Details
    companies_loader = BigQueryLoader(
        project_id=project_id,
        dataset_id=dataset_id,
        bucket_name='companies_details',
        dir_prefix='data/',
        table_name='companies_details'
    )
    companies_loader.load_data()

    # Load Historical Data Details
    historical_loader = BigQueryLoader(
        project_id=project_id,
        dataset_id=dataset_id,
        bucket_name='historical_data_details',
        dir_prefix='data/history/',
        table_name='historical_data_details'
    )
    historical_loader.load_data()

    # Load Analyst Price Target Details
    analyst_loader = BigQueryLoader(
        project_id=project_id,
        dataset_id=dataset_id,
        bucket_name='analyst_price_target',
        dir_prefix='data/',
        table_name='analyst_price_target'
    )
    analyst_loader.load_data()

    # Load Market Cap Details
    marketcap_loader = BigQueryLoader(
        project_id=project_id,
        dataset_id=dataset_id,
        bucket_name='marketcap_details',
        dir_prefix='data/',
        table_name='marketcap_details'
    )
    marketcap_loader.load_data()

if __name__ == "__main__":
    main()
