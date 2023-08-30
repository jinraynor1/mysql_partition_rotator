import datetime

import pymysql
import unittest2
from pymysql.cursors import DictCursor

from db_partition_rotator.PartitionRotator import PartitionRotator
from db_partition_rotator.RotateDaily import RotateDaily

from tests.config import UNIT_TEST


class TestPartitionDaily(unittest2.TestCase):

    def tearDown(self):
        self.connection.close()

    def setUp(self):
        self.connection = pymysql.connect(host=UNIT_TEST['host'],
                                          user=UNIT_TEST['user'],
                                          password=UNIT_TEST['password'],
                                          database=UNIT_TEST['database'],
                                          cursorclass=DictCursor)

        old_time = datetime.datetime(2020, 10, 3)
        new_time = datetime.datetime(2020, 10, 7)
        self.rotate_mode = RotateDaily()
        logger = None
        self.partition = PartitionRotator(self.connection, "test", "test_rotate_daily", old_time, new_time,
                                          self.rotate_mode, logger)
        self.init_table()

    def init_table(self):
        cursor = self.connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_rotate_daily")
        cursor.execute('''CREATE TABLE `test_rotate_daily` (
          `dt` datetime NOT NULL
        ) ENGINE=MyISAM DEFAULT CHARSET=latin1
        PARTITION BY RANGE (TO_DAYS(dt))
        (PARTITION `start` VALUES LESS THAN (0) ,
        PARTITION from20201001 VALUES LESS THAN (TO_DAYS('2020-10-02')) ,
        PARTITION from20201002 VALUES LESS THAN (TO_DAYS('2020-10-03')) ,
        PARTITION from20201003 VALUES LESS THAN (TO_DAYS('2020-10-04')) ,
        PARTITION from20201004 VALUES LESS THAN (TO_DAYS('2020-10-05')) ,
        PARTITION from20201005 VALUES LESS THAN (TO_DAYS('2020-10-06')) ,
        PARTITION from20201006 VALUES LESS THAN (TO_DAYS('2020-10-07')) ,
        PARTITION future VALUES LESS THAN MAXVALUE )''')

    def test_get_partitions(self):
        partition_list = self.partition.get_partitions()

        self.assertGreater(len(partition_list), 0, "should  be not empty")
        self.assertEqual("2020-10-02", partition_list[0].get_date().strftime("%Y-%m-%d"))
        self.assertEqual("2020-10-07", partition_list[len(partition_list) - 1].get_date().strftime("%Y-%m-%d"))

    def test_remove_partition(self):
        self.partition.remove_old_partition()
        partition_list = self.partition.get_partitions()
        self.assertEqual("from20201002", partition_list[0].get_name())
        self.assertEqual(5, len(partition_list))

    def test_add_partition(self):
        self.partition.add_new_partition(datetime.datetime(2020, 10, 7))
        partition_list = self.partition.get_partitions()
        self.assertEqual("from20201007", partition_list[len(partition_list) - 1].get_name())
        self.assertEqual(7, len(partition_list))

    def test_rotate_partition(self):
        self.partition.rotate()
        partition_list = self.partition.get_partitions()
        self.assertEqual("from20201002", partition_list[0].get_name())
        self.assertEqual("from20201007", partition_list[len(partition_list) - 1].get_name())

    def test_partition_pruning(self):
        cursor = self.connection.cursor()
        cursor.execute('''INSERT INTO test_rotate_daily(dt) VALUES
        ('2020-09-30 23:25:00'),
        ('2020-10-01 21:29:00'),
        ('2020-10-02 22:10:00'),
        ('2020-10-03 23:30:00'),
        ('2020-10-04 00:10:00'),
        ('2020-10-05 01:48:00')
        ''')

        cursor.execute('''EXPLAIN PARTITIONS SELECT * FROM test_rotate_daily 
            WHERE dt BETWEEN '2020-10-03 23:09:00' AND '2020-10-03 23:39:00'
            ''')
        data = cursor.fetchone()

        self.assertEqual("from20201003", data["partitions"])

        cursor.execute('''EXPLAIN PARTITIONS SELECT * FROM test_rotate_daily 
                   WHERE dt BETWEEN '2020-10-03 23:00:00' AND '2020-10-04 01:00:00'
                   ''')
        data = cursor.fetchone()

        self.assertEqual("from20201003,from20201004", data["partitions"])

    def test_range_partitions(self):
        partition_list = self.partition.get_partitions()
        old_partitions = len(partition_list)

        from_time = datetime.datetime(2020, 10, 7)
        delta = datetime.timedelta(days=1)
        end_time = datetime.datetime(2020, 10, 9)

        self.partition.add_range_partitions(from_time, delta, end_time)

        partition_list = self.partition.get_partitions()
        total_partitions = old_partitions + 2
        self.assertEqual(total_partitions, len(partition_list))
        self.assertEqual("from20201008", partition_list[total_partitions - 1].get_name())
        self.assertEqual("from20201007", partition_list[total_partitions - 2].get_name())
