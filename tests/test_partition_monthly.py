import datetime

import pymysql
import unittest2
from pymysql.cursors import DictCursor

from mysql_partition_rotator.PartitionRotator import PartitionRotator
from mysql_partition_rotator.RotateMonthly import RotateMonthly

from tests.config import UNIT_TEST


class TestPartitionMontly(unittest2.TestCase):

    def tearDown(self):
        self.connection.close()

    def setUp(self):
        self.connection = pymysql.connect(host=UNIT_TEST['host'],
                                          user=UNIT_TEST['user'],
                                          password=UNIT_TEST['password'],
                                          database=UNIT_TEST['database'],
                                          cursorclass=DictCursor)

        old_time = datetime.datetime(2020, 11, 1)
        new_time = datetime.datetime(2021, 1, 1)
        self.rotate_mode = RotateMonthly()
        logger = None
        self.partition = PartitionRotator(self.connection, "test", "test_rotate_monthly", old_time, new_time,
                                          self.rotate_mode, logger)
        self.init_table()

    def init_table(self):
        cursor = self.connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_rotate_monthly")
        cursor.execute('''CREATE TABLE `test_rotate_monthly` (
        `dt` datetime NOT NULL
        ) ENGINE=MyISAM DEFAULT CHARSET=latin1
         PARTITION BY RANGE (TO_DAYS(dt))
        (PARTITION `start` VALUES LESS THAN (0) ,
         PARTITION from202009 VALUES LESS THAN (TO_DAYS('2020-10-01')),
         PARTITION from202010 VALUES LESS THAN (TO_DAYS('2020-11-01')),
         PARTITION from202011 VALUES LESS THAN (TO_DAYS('2020-12-01')),
         PARTITION from202012 VALUES LESS THAN (TO_DAYS('2021-01-01')),
         PARTITION future VALUES LESS THAN MAXVALUE ) ''')

    def test_get_partitions(self):
        partition_list = self.partition.get_partitions()

        self.assertGreater(len(partition_list), 0, "should  be not empty")
        self.assertEqual("from202009", partition_list[0].get_name())
        self.assertEqual("2020-10-01", partition_list[0].get_date().strftime("%Y-%m-%d"))

        self.assertEqual("from202012", partition_list[len(partition_list)-1].get_name())
        self.assertEqual("2021-01-01", partition_list[len(partition_list)-1].get_date().strftime("%Y-%m-%d"))


    def test_remove_partition(self):
        self.partition.remove_old_partition()
        partition_list = self.partition.get_partitions()
        self.assertEqual("from202010", partition_list[0].get_name())
        self.assertEqual(3, len(partition_list))

    def test_add_partition(self):
        self.partition.add_new_partition(datetime.datetime(2021, 2, 1))
        partition_list = self.partition.get_partitions()
        self.assertEqual("from202102", partition_list[len(partition_list) - 1].get_name())
        self.assertEqual(5, len(partition_list))

    def test_rotate_partition(self):
        self.partition.rotate()
        partition_list = self.partition.get_partitions()
        self.assertEqual("from202010", partition_list[0].get_name())
        self.assertEqual("from202101", partition_list[len(partition_list) - 1].get_name())
        self.assertEqual(4, len(partition_list))


    def test_partition_pruning(self):
        cursor = self.connection.cursor()
        cursor.execute('''INSERT INTO test_rotate_monthly(dt) VALUES
        
        ('2020-10-09 21:29:00'),
        ('2020-11-10 22:10:00'),
        ('2020-12-11 23:30:00')
        ''')

        cursor.execute('''EXPLAIN PARTITIONS SELECT * FROM test_rotate_monthly 
            WHERE dt BETWEEN '2020-10-09 00:00:00' AND '2020-10-09 23:59:00'
            ''')
        data = cursor.fetchone()

        self.assertEqual("from202010", data["partitions"])

        cursor.execute('''EXPLAIN PARTITIONS SELECT * FROM test_rotate_monthly 
            WHERE dt BETWEEN '2020-10-09 00:00:00' AND '2020-11-11 23:59:00'
                   ''')
        data = cursor.fetchone()

        #todo: siempre agregar la particion de inicio cuando se busca mas de un mes aunque no tenga filas
        self.assertEqual("start,from202010,from202011", data["partitions"])

