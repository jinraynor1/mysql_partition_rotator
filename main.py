# This is a sample Python script.
import datetime
import logging
import sys

import pymysql
import os

from pymysql.cursors import DictCursor

from mysql_partition_rotator.Partition import Partition
from mysql_partition_rotator.PartitionRotator import PartitionRotator
from mysql_partition_rotator.RotateDaily import RotateDaily
from mysql_partition_rotator.RotateHourly import RotateHourly


def main():
    # Example code
    connection = pymysql.connect(host='localhost',
                                 user='johndoe',
                                 password='mypass',
                                 database='test',
                                 cursorclass=DictCursor)

    new_time = datetime.datetime.now()
    old_time = new_time - datetime.timedelta(days=2)
    rotate_mode = RotateDaily()
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    pr = PartitionRotator(database_instance=connection, database_name="tests", table_name="test_rotate_daily",
                          oldest_partition_time=old_time, newest_partition_time=new_time,
                          rotate_mode=rotate_mode, logger=logger)
    pr.rotate()


if __name__ == '__main__':
    main()


