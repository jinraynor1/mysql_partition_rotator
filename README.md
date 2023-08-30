# Mysql Partition Rotator


This module allows you to simply rotate your data avoiding slow deletes on big tables, this module is inspedired by  **Rick James** [go check his page for more details ](https://mysql.rjweb.org/doc.php/partitionmaint)

## Features

- Rotates table in daily periods
- Rotates table in hourly periods
- Rotates table in monthly periods

## Requires
Python 3
pip

## Installation

```sh
pip install mysql_partition_rotator
```

## Usage
First create a table with enabled partitioning for example:
```mysql
CREATE TABLE `test_rotate_daily` (
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
PARTITION future VALUES LESS THAN MAXVALUE )
```

Then create a partition rotator instance with all the necessary config in order to rotate the table

```python
import pymysql
from pymysql.cursors import DictCursor
import logging
import datetime
from mysql_partition_rotator import RotateDaily
from mysql_partition_rotator import PartitionRotator

connection = pymysql.connect(host='localhost', user='johndoe', password='mypass', database='test', cursorclass=DictCursor)
old_time = datetime.datetime(2020, 10, 3)
new_time = datetime.datetime(2020, 10, 7)
rotate_mode = RotateDaily()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

pr = PartitionRotator(database_instance=connection,
                    database_name="tests",
                    table_name="test_rotate_daily", 
                    oldest_partition_time=old_time,
                    newest_partition_time=new_time,
                    rotate_mode=rotate_mode,
                    logger=logger)

pr.rotate()
```
