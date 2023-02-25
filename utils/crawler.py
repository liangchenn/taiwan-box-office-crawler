import logging

import arrow
import pandas as pd
import requests
from google.cloud.bigquery import LoadJobConfig
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_random
from tqdm.auto import tqdm

from utils.date import gen_date_list
from utils.logger import Logger
from utils.uploader import BigQueryUploader


logging.basicConfig(
    format="[%(levelname)s %(asctime)s %(name)s] %(message)s",
)


class BoxOfficeCrawler:

    DEFAULT_JOB_CONFIG = "WRITE_APPEND"

    def __init__(self):
        self.url = "https://boxoffice.tfi.org.tw/api/export?start={date}&end={date}"
        self.data = None
        self.logger = Logger(__name__).get_logger()
        self.uploader = BigQueryUploader()

    def run_crawler(
        self,
        start_date: str,
        end_date: str,
        dataset_id: str,
        table_id: str,
        batch_upload_days: int = 0,
    ):

        dates = gen_date_list(start_date, end_date)
        self.logger.info(f"Box office data range from {start_date} to {end_date}")
        data = []
        for i, date in enumerate(tqdm(dates)):
            try:
                _res = self.fetch_data_by_one_date(date)
                data.extend(_res)
                # batch upload
                if (i % batch_upload_days == 0) & (i > 0):
                    df = pd.DataFrame(data)
                    self.upload_dataframe(df, dataset_id, table_id)
                    # reset data list after upload
                    data = []
            except Exception as err:
                self.logger.error(f"[{date=}] {err=}")

        try:
            if len(data):
                df = pd.DataFrame(data)
                self.upload_dataframe(df, dataset_id, table_id)
        except Exception as err:
            self.logger.error(f"[{date=}] {err=}")

        return 1

    def upload_dataframe(self, df, dataset_id, table_id):

        try:
            # before loading
            table = self.uploader.client.get_table(f"{dataset_id}.{table_id}")
            self.logger.info(f"Before load job: ({table.num_rows}, {len(table.schema)})")

            # get data start, end dates
            start_date, end_date = df.date.min(), df.date.max()
            # delete data by date
            delete_sql = f"""
                DELETE FROM {dataset_id}.{table_id}
                WHERE date BETWEEN '{start_date}' AND '{end_date}'
            """
            self.logger.info(delete_sql)
            self.uploader.execute(delete_sql)

        except Exception:
            self.logger.info("Before load job, table not exists.")

        # load data from dataframe
        job_config = LoadJobConfig(write_disposition=self.DEFAULT_JOB_CONFIG)
        self.uploader.load_table_from_dataframe(df, dataset_id, table_id, job_config)

        # after loading
        table = self.uploader.client.get_table(f"{dataset_id}.{table_id}")
        self.logger.info(f"After load job: ({table.num_rows}, {len(table.schema)})")

        return

    @retry(stop=stop_after_attempt(3), wait=wait_random(min=1, max=10))
    def fetch_data_by_one_date(self, date: str):
        # format date
        fetch_date = arrow.get(date).format("YYYY/MM/DD")
        url = self.url.format(date=fetch_date)
        self.logger.info(f"getting box office data on date={fetch_date}")

        # fetch data
        resp = requests.get(url)
        data = resp.json().get("list")

        if len(data) == 0:
            return

        result = [dict(item, **{"date": arrow.get(date).format("YYYY-MM-DD")}) for item in data]

        return result


if __name__ == "__main__":

    DATASET_ID = "box_office"
    TABLE_ID = "box_office_test"
    TEST_DATE = "2023-02-14"

    logger = logging.getLogger(__name__)

    crawler = BoxOfficeCrawler()

    # # create table
    # crawler.uploader.create_table(f"{}.{}")

    # test fetching data
    logger.info("test feching data")
    data = crawler.fetch_data_by_one_date(TEST_DATE)
    df = pd.DataFrame(data)
    logger.info(df.head(5))

    # test uploading data
    logger.info("test uploading data")
    try:
        crawler.upload_dataframe(df, dataset_id=DATASET_ID, table_id=TABLE_ID)
        logger.info("upload successfully")
    except Exception as err:
        logger.info(f"{err=}")

    # test run_crawler function
    logger.info("test run crawler function")
    try:
        crawler.run_crawler(
            start_date="2023-02-14",
            end_date="2023-02-14",
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            batch_upload_days=1,
        )
        logger.info("done")
    except Exception as err:
        logger.error(f"{err=}")
