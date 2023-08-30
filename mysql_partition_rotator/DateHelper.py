def to_days(date):
    return 719528 + int((date / 86400))


def from_days(day_stamp):
    return (day_stamp - 719528) * 86400


def date_range(start_date, delta, end_date):
    date_list = []

    while start_date < end_date:
        date_list.append(start_date)
        start_date += delta
    return date_list
