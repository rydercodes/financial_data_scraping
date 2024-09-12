# bigquery_loader.py
from google.cloud import bigquery, storage

class BigQueryLoader:
    def __init__(self, project_id, dataset_id, bucket_name, dir_prefix, table_name):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.dir_prefix = dir_prefix
        self.table_name = table_name
        self.gcs_client = storage.Client()
        self.bq_client = bigquery.Client()

    def list_parquet_files(self):
        """List all parquet files in a specific GCS bucket and prefix."""
        bucket = self.gcs_client.bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=self.dir_prefix)
        return [blob.name for blob in blobs if blob.name.endswith('.parquet')]

    def load_data(self):
        """Load parquet files from GCS into BigQuery."""
        files = self.list_parquet_files()

        table_ref = self.bq_client.dataset(self.dataset_id).table(self.table_name)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True
        )

        # Start the load job
        load_job = self.bq_client.load_table_from_uri(
            [f"gs://{self.bucket_name}/{file}" for file in files],
            table_ref,
            job_config=job_config
        )

        # Wait for the job to complete
        load_job.result()

        # Check the result
        table = self.bq_client.get_table(table_ref)
        print(f"Loaded {table.num_rows} rows into {self.dataset_id}:{self.table_name}.")
