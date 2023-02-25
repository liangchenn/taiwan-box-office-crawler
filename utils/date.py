import arrow


def gen_date_list(start_date: str, end_date: str):

    s = arrow.get(start_date)
    e = arrow.get(end_date)

    return [d.format("YYYY-MM-DD") for d in arrow.Arrow.range("day", s, e)]
