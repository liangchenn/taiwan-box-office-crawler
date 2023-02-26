from argparse import ArgumentTypeError

import arrow


def gen_date_list(start_date: str, end_date: str):

    s = arrow.get(start_date)
    e = arrow.get(end_date)

    return [d.format("YYYY-MM-DD") for d in arrow.Arrow.range("day", s, e)]


def validate_dateformat(date: str):

    try:
        return arrow.get(date).format("YYYY-MM-DD")
    except ValueError:
        msg = f"Not a valid date format: {date:!r}"
        raise ArgumentTypeError(msg)
