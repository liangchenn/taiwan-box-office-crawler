import logging

from utils.crawler import BoxOfficeCrawler

DATASET_ID = "box_office"
TABLE_ID = "taiwan_movie_box_office"

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s %(asctime)s %(name)s] %(message)s",
)


if __name__ == "__main__":

    start_date = "2023-02-15"
    end_date = "2023-02-20"

    logger = logging.getLogger(__name__)
    crawler = BoxOfficeCrawler()

    try:
        crawler.run_crawler(
            start_date=start_date,
            end_date=end_date,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            batch_upload_days=4,
        )
        logger.info("done")
    except Exception as err:
        logger.error(f"{err=}")
