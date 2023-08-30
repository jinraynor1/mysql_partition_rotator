# This is a sample Python script.
import datetime
import logging
import sys

import pymysql
import os

from pymysql.cursors import DictCursor

from db_partition_rotator.Partition import Partition
from db_partition_rotator.PartitionRotator import PartitionRotator
from db_partition_rotator.RotateDaily import RotateDaily
from db_partition_rotator.RotateHourly import RotateHourly


def main():

    r = RotateHourly()
    d = r.get_partition_date(Partition("from2020100322","7645567"))
    print(repr(d))
    sys.exit()

    start_date = datetime.datetime(2022, 3, 1)
    end_date = datetime.datetime(2022, 3, 10)
    date_list = []

    delta = datetime.timedelta(hours=12)
    while start_date <= end_date:
        date_list.append(start_date)
        start_date += delta
    print(date_list)


    sys.exit()
    # Connect to the database
    connection = pymysql.connect(host='localhost',
                                 user='jatauje',
                                 password='jatauje',
                                 database='tests',
                                 cursorclass=DictCursor)

    new_time = datetime.datetime.now()
    old_time = new_time - datetime.timedelta(days=2)
    rotate_mode = RotateDaily()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()


    pr = PartitionRotator(connection, "tests", "test_rotate_daily", old_time, new_time, rotate_mode, logger)
    pr.rotate()
    a = 1

if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
