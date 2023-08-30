import datetime

from db_partition_rotator.DateHelper import from_days


class Partition:
    def __init__(self, name, description):
        self.name = name
        self.description = int(description)

    def get_date(self):
        partition_timestamp = from_days(self.description)
        return datetime.datetime.fromtimestamp(partition_timestamp, datetime.timezone.utc)

    def get_name(self):
        return self.name
