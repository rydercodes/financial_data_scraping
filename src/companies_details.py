import os
import pandas as pd
import numpy as np
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.cloud import storage
from google.oauth2 import service_account
from io import StringIO

class CompaniesDetails:
    def __init__(self, gcs_credentials_path, project_id, dataset_id, bucket_name):
        self.gcs_credentials_path = gcs_credentials_path
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.driver = self.setup_driver()

    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--remote-debugging-port=9222')
        # Set up WebDriver
        driver = webdriver.Chrome(options=chrome_options)
        return driver

    def fetch_table_data(self, url):
        self.driver.get(url)
        try:
            table = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'table[role="table"]'))
            )
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            tables = soup.find_all("table")

            if tables:
                html_content = str(tables[0])
                df = pd.read_html(StringIO(html_content))[0]
                return df
            else:
                print("No tables found.")
                return pd.DataFrame()
        except Exception as e:
            print("Error waiting for table:", e)
            return pd.DataFrame()

    def fetch_company_details(self, links):
        details = []
        for link in links:
            self.driver.get(link)
            try:
                isin = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'lbl-details-2-2'))
                ).text
                ticker = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'lbl-details-2-3'))
                ).text
                details.append({
                    'ISIN': isin,
                    'Ticker': ticker,
                    'scraping_url': link,
                    'scraping_timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                print(f"Error fetching details for {link}: {e}")

            random_int = np.random.choice([1, 2, 3])
            time.sleep(random_int)

        return pd.DataFrame(details)

    def upload_to_gcs(self, local_file_path, destination_blob_name):
        credentials = service_account.Credentials.from_service_account_file(self.gcs_credentials_path)
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file_path)
        print(f"File {local_file_path} uploaded to {destination_blob_name} in bucket {self.bucket_name}.")

    def process(self, urls, local_file_path):
        all_companies = pd.DataFrame()

        for url in urls:
            df = self.fetch_table_data(url)
            if not df.empty:
                data_links = [a_tag.get_attribute('href') for a_tag in self.driver.find_elements(By.CSS_SELECTOR, 'td[role="rowheader"] a')]
                links_df = pd.DataFrame({'links': data_links})
                companies_df = self.fetch_company_details(links_df['links'])
                combined_data = pd.concat([df, companies_df], axis=1)
                all_companies = pd.concat([all_companies, combined_data], ignore_index=True)

        all_companies.to_parquet(local_file_path, index=False)
        print(f"Data saved locally to {local_file_path}.")
        
        self.upload_to_gcs(local_file_path, 'data/companies_details.parquet')
        self.driver.quit()