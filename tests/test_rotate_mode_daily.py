from datetime import datetime

import unittest2

from mysql_partition_rotator.RotateDaily import RotateDaily


class TestRotateModeDaily(unittest2.TestCase):
    def setUp(self):
        self.rotate = RotateDaily()
    def test_get_partition_name(self):
        self.assertEqual("20210201", self.rotate.get_partition_name(datetime(2021,2,1)))
        self.assertEqual("20210202", self.rotate.get_partition_name(datetime(2021, 2, 2)))

    def test_get_partition_value(self):
        self.assertEqual(738188, self.rotate.get_partition_value(datetime(2021, 2, 1)))
        self.assertEqual(738189, self.rotate.get_partition_value(datetime(2021, 2, 2)))