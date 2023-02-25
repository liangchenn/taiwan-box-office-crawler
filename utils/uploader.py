import os

from dotenv import load_dotenv
from google.cloud import bigquery

from utils.logger import Logger


class BigQueryUploader:
    def __init__(self, logger=None):
        # load environment variables
        load_dotenv()
        # prepare logger
        self.logger = Logger(__name__).get_logger() if logger is None else logger

        # handle environment variables for Bigquery
        env_var_list = ["GOOGLE_APPLICATION_CREDENTIALS"]
        if not all(var in list(os.environ.keys()) for var in env_var_list):
            raise Exception(f"Need GCP configs in environment variables.\n {env_var_list}")
        if not os.path.exists(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]):
            raise Exception(
                f"The config file {os.environ['GOOGLE_APPLICATION_CREDENTIALS']} does not exists"
            )

        # construct bigquery client
        self.client = bigquery.Client()
        self.logger.info(f"Connect to project={self.client.project}")

    def execute(self, sql):
        try:
            query_job = self.client.query(sql)
            query_job.result()

        except Exception as err:
            for e in query_job.errors:
                self.logger.error(f"Error: {e['message']}")
            raise err

    def query_to_dataframe(self, sql):

        query_job = None
        try:
            query_job = self.client.query(sql)
            df = query_job.result().to_dataframe(create_bqstorage_client=True)
            return df
        except Exception as err:
            for error in query_job.errors:
                self.logger.error("ERROR: {}".format(error["message"]))
            raise err

    def load_table_from_dataframe(self, df, dataset_id, table_id, job_config=None):

        job = None
        table_id = f"{dataset_id}.{table_id}"
        try:
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            return job.result()
        except Exception as err:
            self.logger.error(f"ERROR: {err}")
            raise err

    def create_dataset(self, dataset_id):

        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(f"{self.client.project}.{dataset_id}")
        dataset = self.client.create_dataset(dataset, timeout=30)  # Make an API request.
        self.logger.info(f"Created dataset {self.client.project}.{dataset.dataset_id}")

    def delete_dataset(self, dataset_id):

        dataset = bigquery.Dataset(f"{self.client.project}.{dataset_id}")
        self.client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)

    def create_table(self, table_id, dataset_id):

        # Bigquery will automatically create table if the table is not existed.
        # Therefore, you can use "write" directly, and this function is actually useless.
        table_id = f"{self.client.project}.{dataset_id}.{table_id}"
        schema = []
        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table)  # Make an API request.
        self.logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
