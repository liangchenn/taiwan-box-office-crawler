import argparse
import logging

import arrow

from utils.constant import DATASET_ID
from utils.constant import TABLE_ID
from utils.crawler import BoxOfficeCrawler
from utils.date import validate_dateformat


logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s %(asctime)s %(name)s] %(message)s",
)


def make_parser():

    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "-s", "--start_date", help="Start date of data to parse.", type=validate_dateformat
    )
    parser.add_argument(
        "-e", "--end_date", help="Start date of data to parse.", type=validate_dateformat
    )
    parser.add_argument(
        "-d", "--run_date", help="Target date of data to parse.", type=validate_dateformat
    )

    return parser


if __name__ == "__main__":

    logger = logging.getLogger(__name__)
    crawler = BoxOfficeCrawler()

    # cli argments
    parser = make_parser()
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    run_date = args.run_date

    # validate date arguments
    if start_date and not end_date:
        parser.error("end_ate not provided.")

    if end_date and not start_date:
        parser.error("start_date not provided.")

    if run_date:
        start_date = run_date
        end_date = run_date

    # default to 10D, 3D before current date
    if not (start_date and end_date):
        start_date = arrow.now().shift(days=-10).format("YYYY-MM-DD")
        end_date = arrow.now().shift(days=-3).format("YYYY-MM-DD")

    try:
        crawler.run_crawler(
            start_date=start_date,
            end_date=end_date,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            batch_upload_days=7,
        )
        logger.info("done")
    except Exception as err:
        logger.error(f"{err=}")
