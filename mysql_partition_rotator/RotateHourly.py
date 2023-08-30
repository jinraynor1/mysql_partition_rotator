from mysql_partition_rotator.DateHelper import to_days, from_days
from datetime import timedelta, datetime, timezone


class RotateHourly:

    def __init__(self):
        self.format = '%Y%m%d%H'
        self.delta = 1

    def get_partition_name(self, date_obj):
        return date_obj.strftime(self.format)

    def get_partition_value(self, date_obj):
        date_obj = date_obj + timedelta(hours=self.delta)
        date_from = datetime(1970, 1, 1, 0, 0, 0)
        date_from = date_from.replace(tzinfo=timezone.utc)
        diff = date_obj - date_from
        secs = 62167219200
        secs += diff.days * 24 * 60 * 60

        secs += int(date_obj.strftime('%H')) * 60 * 60
        secs += int(date_obj.strftime('%M')) * 60
        secs += int(date_obj.strftime('%S'))

        return secs

    def get_partition_date(self, partition):
        partition_date_text = partition.name[4:]
        date_format = '%Y%m%d%H'
        my_date_time = datetime.strptime(partition_date_text, date_format)
        my_date_time = my_date_time.replace(tzinfo=timezone.utc)
        return my_date_time
