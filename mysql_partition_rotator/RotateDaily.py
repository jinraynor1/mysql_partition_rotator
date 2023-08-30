from mysql_partition_rotator.DateHelper import to_days, from_days
from datetime import timedelta

class RotateDaily:

    def __init__(self):
        self.format = '%Y%m%d'
        self.delta = 1

    def get_partition_name(self, date_obj):
        return date_obj.strftime(self.format)

    def get_partition_value(self, date_obj):
        date_obj = date_obj + timedelta(days=self.delta)
        return to_days(date_obj.timestamp())

    def get_partition_date(self, partition):
        return partition.get_date()

