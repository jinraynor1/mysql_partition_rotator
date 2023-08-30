import logging
from datetime import datetime, timezone
from db_partition_rotator.DateHelper import date_range
from db_partition_rotator.Partition import Partition


class PartitionRotator:
    def __init__(self, database_instance, database_name, table_name, oldest_partition_time, newest_partition_time,
                 rotate_mode,
                 logger):
        self.oldest_partition_time = oldest_partition_time.replace(tzinfo=timezone.utc)
        self.newest_partition_time = newest_partition_time.replace(tzinfo=timezone.utc)
        self.rotate_mode = rotate_mode
        self.logger = logger
        self.table_name = table_name
        self.database_name = database_name
        self.database_instance = database_instance
        if self.logger is None:
            self.logger = logging.getLogger()

    def set_logger(self, logger):
        self.logger = logger

    def remove_old_partition(self):
        partition_list = self.get_partitions()
        if partition_list is None:
            self.logger.info("no partitions found for removal")
            return True

        for partition in partition_list:
            if self.rotate_mode.get_partition_date(partition) < self.oldest_partition_time:
                self.logger.info(f"attempting to remove partition {partition.get_name()} ")
                sql = f"ALTER TABLE `{self.database_name}`.`{self.table_name}` DROP PARTITION {partition.get_name()}"
                cursor = self.database_instance.cursor()

                try:
                    cursor.execute(sql)
                    self.logger.info(f"partition {partition.get_name()} successfully removed ")
                except:
                    self.logger.error(f"partition {partition.get_name()} was not removed ")
                    pass

        return True

    def get_partitions(self):
        sql = f'''SELECT partition_name, partition_description FROM INFORMATION_SCHEMA.PARTITIONS 
                WHERE table_schema='{self.database_name}'
                AND table_name='{self.table_name}'
                AND partition_name NOT IN ('start','future')'''
        cursor = self.database_instance.cursor()
        cursor.execute(sql)
        partition_list = cursor.fetchall()
        result_list = []
        for partition in partition_list:
            result_list.append(Partition(partition['partition_name'], partition['partition_description']))

        return result_list

    def partition_exists(self, partition_time):
        sql = f'''SELECT partition_name,partition_description FROM INFORMATION_SCHEMA.PARTITIONS 
                WHERE table_schema='{self.database_name}'
                AND table_name='{self.table_name}'
                AND partition_name='from{self.rotate_mode.get_partition_name(partition_time)}'
                '''
        cursor = self.database_instance.cursor()
        cursor.execute(sql)
        return cursor.fetchone()

    def add_range_partitions(self, from_time, delta, end_time):
        from_time = from_time.replace(tzinfo=timezone.utc)
        end_time = end_time.replace(tzinfo=timezone.utc)

        date_list = date_range(from_time, delta, end_time)

        for single_date in date_list:
            date_format = '%Y-%m-%d %H:%M:%S'
            my_date_time = datetime.strptime(single_date.strftime("%Y-%m-%d %H:00:00"), date_format)
            my_date_time = my_date_time.replace(tzinfo=timezone.utc)
            try:
                if self.partition_exists(my_date_time) is None:
                    self.add_new_partition(my_date_time)
            except Exception as e:
                partition_date_text = my_date_time.strftime("%Y-%m-%d %H:%M:%S")
                error_message = repr(e)
                self.logger.error(f"Error adding partition {partition_date_text}, error: {error_message}")
                pass

    def add_new_partition(self, partition_date):
        partition_date = partition_date.replace(tzinfo=timezone.utc)

        partition_date_text = partition_date.strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info(f"attempting to add new partition for date {partition_date_text}")

        partition_exists = self.partition_exists(partition_date)
        if partition_exists:
            self.logger.info(f"partition {partition_date_text} already exists")
            return True

        partition_name = self.rotate_mode.get_partition_name(partition_date)
        partition_value = self.rotate_mode.get_partition_value(partition_date)
        sql = f'''ALTER TABLE  `{self.database_name}`.`{self.table_name}`  REORGANIZE PARTITION future INTO(
                PARTITION from{partition_name} VALUES LESS THAN ({partition_value}),
                 PARTITION future VALUES LESS THAN MAXVALUE ) 
                '''
        cursor = self.database_instance.cursor()

        try:
            cursor.execute(sql)
            self.logger.info(f"partition {partition_name} was successfully created")
        except Exception as e:
            error_message = repr(e)
            self.logger.error(f"partition {partition_name} was not created, {error_message}")

    def rotate(self):
        self.remove_old_partition()
        self.add_new_partition(self.newest_partition_time)
