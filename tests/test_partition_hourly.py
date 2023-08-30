import datetime

import pymysql
import unittest2
from pymysql.cursors import DictCursor

from mysql_partition_rotator.PartitionRotator import PartitionRotator
from mysql_partition_rotator.RotateHourly import RotateHourly

from tests.config import UNIT_TEST


class TestPartitionHourly(unittest2.TestCase):

    def tearDown(self):
        self.connection.close()

    def setUp(self):
        self.connection = pymysql.connect(host=UNIT_TEST['host'],
                                          user=UNIT_TEST['user'],
                                          password=UNIT_TEST['password'],
                                          database=UNIT_TEST['database'],
                                          cursorclass=DictCursor)

        old_time = datetime.datetime(2020, 10, 3, 23)
        new_time = datetime.datetime(2020, 10, 4, 4)
        self.rotate_mode = RotateHourly()
        logger = None
        self.partition = PartitionRotator(self.connection, "test", "test_rotate_hourly", old_time, new_time,
                                          self.rotate_mode, logger)
        self.init_table()

    def init_table(self):
        cursor = self.connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_rotate_hourly")
        cursor.execute('''CREATE TABLE `test_rotate_hourly` (
        `dt` datetime NOT NULL
        ) ENGINE=MyISAM DEFAULT CHARSET=latin1
        PARTITION BY RANGE (TO_SECONDS(dt))
        (PARTITION `start` VALUES LESS THAN (0) ,
        PARTITION from2020100322 VALUES LESS THAN (TO_SECONDS('2020-10-03 23:00:00')) ,
        PARTITION from2020100323 VALUES LESS THAN (TO_SECONDS('2020-10-04 00:00:00')) ,
        PARTITION from2020100400 VALUES LESS THAN (TO_SECONDS('2020-10-04 01:00:00')) ,
        PARTITION from2020100401 VALUES LESS THAN (TO_SECONDS('2020-10-04 02:00:00')) ,
        PARTITION from2020100402 VALUES LESS THAN (TO_SECONDS('2020-10-04 03:00:00')) ,
        PARTITION from2020100403 VALUES LESS THAN (TO_SECONDS('2020-10-04 04:00:00')) ,
        PARTITION future VALUES LESS THAN MAXVALUE ) ''')

    def test_get_partitions(self):
        partition_list = self.partition.get_partitions()

        self.assertGreater(len(partition_list), 0, "should  be not empty")
        self.assertEqual("from2020100322", partition_list[0].get_name())
        self.assertEqual("from2020100403", partition_list[len(partition_list) - 1].get_name())

    def test_remove_partition(self):
        self.partition.remove_old_partition()
        partition_list = self.partition.get_partitions()
        self.assertEqual("from2020100323", partition_list[0].get_name())
        self.assertEqual(5, len(partition_list))

    def test_add_partition(self):
        self.partition.add_new_partition(datetime.datetime(2020, 10, 4, 5))
        partition_list = self.partition.get_partitions()
        self.assertEqual("from2020100405", partition_list[len(partition_list) - 1].get_name())
        self.assertEqual(7, len(partition_list))

    def test_rotate_partition(self):
        self.partition.rotate()
        partition_list = self.partition.get_partitions()
        self.assertEqual("from2020100323", partition_list[0].get_name())
        self.assertEqual("from2020100404", partition_list[len(partition_list) - 1].get_name())

    def test_partition_pruning(self):
        cursor = self.connection.cursor()
        cursor.execute('''INSERT INTO test_rotate_hourly(dt) VALUES
        ('2020-10-03 21:29:00'),
        ('2020-10-03 22:10:00'),
        ('2020-10-03 23:30:00'),('2020-10-04 00:10:00'),('2020-10-04 01:48:00'),
        ('2020-10-04 02:33:00')
        ''')

        cursor.execute('''EXPLAIN PARTITIONS SELECT * FROM test_rotate_hourly 
            WHERE dt BETWEEN '2020-10-03 21:19:00' AND '2020-10-03 21:39:00'
            ''')
        data = cursor.fetchone()

        self.assertEqual("from2020100322", data["partitions"])

        cursor.execute('''EXPLAIN PARTITIONS SELECT * FROM test_rotate_hourly 
            WHERE dt BETWEEN '2020-10-03 23:00:00' AND '2020-10-04 01:00:00'
                   ''')
        data = cursor.fetchone()

        self.assertEqual("from2020100323,from2020100400,from2020100401", data["partitions"])

    def test_range_partitions(self):
        partition_list = self.partition.get_partitions()
        old_partitions = len(partition_list)

        from_time = datetime.datetime(2020, 10, 4, 4)
        delta = datetime.timedelta(hours=1)
        end_time = datetime.datetime(2020, 10, 4, 6)

        self.partition.add_range_partitions(from_time, delta, end_time)

        partition_list = self.partition.get_partitions()
        total_partitions = old_partitions + 2
        self.assertEqual(total_partitions, len(partition_list))
        self.assertEqual("from2020100405", partition_list[total_partitions - 1].get_name())
        self.assertEqual("from2020100404", partition_list[total_partitions - 2].get_name())
