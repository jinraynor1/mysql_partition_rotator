from mysql_partition_rotator.DateHelper import to_days, from_days
from datetime import timedelta, datetime, timezone


class RotateMonthly:

    def __init__(self):
        self.format = '%Y%m'
        self.delta = 1

    def get_partition_name(self, date_obj):
        return date_obj.strftime(self.format)

    def get_partition_value(self, date_obj):
        try:
            next_month_date = date_obj.replace(month=date_obj.month + 1, day=1)
        except ValueError:
            if date_obj.month == 12:
                next_month_date = date_obj.replace(year=date_obj.year + 1, month=1, day=1)
            else:
                # next month is too short to have "same date"
                # pick your own heuristic, or re-raise the exception:
                raise

        return to_days(next_month_date.timestamp())

    def get_partition_date(self, partition):
        return partition.get_date()
