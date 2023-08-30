from datetime import datetime

import unittest2

from db_partition_rotator.RotateDaily import RotateDaily
from db_partition_rotator.RotateMonthly import RotateMonthly


class TestRotateMonthly(unittest2.TestCase):
    def setUp(self):
        self.rotate = RotateMonthly()

    def test_get_partition_name(self):
        self.assertEqual("202102", self.rotate.get_partition_name(datetime(2021, 2, 2)))
        self.assertEqual("202101", self.rotate.get_partition_name(datetime(2021, 1, 30)))

    def test_get_partition_value(self):
        self.assertEqual(738215, self.rotate.get_partition_value(datetime(2021, 2, 2)))
        self.assertEqual(738187, self.rotate.get_partition_value(datetime(2021, 1, 30)))
