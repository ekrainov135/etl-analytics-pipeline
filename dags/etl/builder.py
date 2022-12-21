from abc import ABC, abstractmethod
from datetime import datetime as dt

import pandas as pd
from tqdm import tqdm

from settings import DATE_FORMAT, DATETIME_FORMAT, BATCH_SIZE


pd.options.mode.chained_assignment = None


def get_table_metadata(db_driver, obj_name, date_col=None, data_filter=None):
    metadata = {'total': 0}

    for batch in db_driver.read(obj_name, [date_col], batch_size=BATCH_SIZE):
        metadata['total'] += batch.shape[0]

        if data_filter is not None:
            metadata['filtered'] = 0 if metadata.get('filtered') is None else  metadata['filtered']
            metadata['filtered'] += batch[data_filter(batch)].shape[0]

        if date_col is not None:
            metadata['min_date'] = batch[date_col].min() if metadata.get('min_date') is None else min(batch[date_col].min(), metadata['min_date'])
            metadata['max_date'] = batch[date_col].max() if metadata.get('max_date') is None else max(batch[date_col].max(), metadata['max_date'])

    return metadata


class BaseDagBuilder(ABC):
    """ 
    """

    @abstractmethod
    def run(self, *args, **kwargs):
        pass

    def add_method(self, name, func, is_static=False):
        method = func if is_static else partial(func, self)
        setattr(self, name, method)
        return self


class ETLDagBuilder(BaseDagBuilder):
    """ 
    """

    def __init__(self, logger, source_driver, target_driver, db_reset_sql=[]):
        self.logger = logger
        self.db_reset_sql = db_reset_sql
        self.source_driver = source_driver
        self.target_driver = target_driver

    def load(self, target, data):
        self.target_driver.write(target, data)

    def extract(self, source, date_col, start_date, end_date):

        self.logger.info("Getting '{}' metadata...".format(source))

        # Calculating the size of the loading dataset
        data_filter = lambda df: (df[date_col] >= start_date) & (df[date_col] < end_date)
        target_meta = get_table_metadata(self.source_driver, source, date_col, data_filter)

        dataset_size = target_meta['total']
        slice_size = target_meta['filtered']
        num_iterations = dataset_size // BATCH_SIZE + (dataset_size % BATCH_SIZE > 0)

        self.logger.info("Data loading shape: {} ({} in total)".format(slice_size, dataset_size))
        self.logger.info("Scanning {} batches...".format(num_iterations))

        extracted_iterable = self.source_driver.read(source, batch_size=BATCH_SIZE)

        # Extract data by batches
        for batch in extracted_iterable:
            data = batch[(batch[date_col] >= start_date) & (batch[date_col] < end_date)]

            if data.shape[0] > 0:
                yield data

    def run(self, metadata, date_col='time', start_date=None, end_date=None):
        """
        """

        target = metadata[0]['table']
        source = [row['value'] for row in metadata if row['param']=='source'][0]
        etl_first_run = [row['value'] for row in metadata if row['param']=='first_run'][0] or None
        etl_last_run = [row['value'] for row in metadata if row['param']=='last_run'][0] or None
        cycle_start = [row['value'] for row in metadata if row['param']=='cycle_start'][0]
        cycle_end = [row['value'] for row in metadata if row['param']=='cycle_end'][0]

        today = dt.strptime(metadata[0]['timestamp'], DATETIME_FORMAT)

        self.logger.info("Execution datetime: {}".format(today.strftime(DATETIME_FORMAT)))
        self.logger.info("Start loading table '{}'".format(target))
        self.logger.info("Extracting from: '{}'".format(source))

        period_delta = dt.strptime(cycle_end, DATE_FORMAT) - dt.strptime(cycle_start, DATE_FORMAT)

        # Recreate the tables if this is the first load or if all cycle data is loaded
        if etl_first_run is None or etl_last_run is None or today - dt.strptime(etl_first_run, DATETIME_FORMAT) > period_delta:
            self.logger.info("ETL dates period is missing")

            self.logger.info("Resetting target tables...")
            self.target_driver.exec(*self.db_reset_sql)
            
            etl_first_run = today
            etl_last_run = today

            self.logger.info("Getting '{}' metadata...".format(source))

            time_shift = etl_first_run.date() - dt.strptime(cycle_start, DATE_FORMAT).date()
            source_meta = get_table_metadata(self.source_driver, source, date_col)
            source_min_date = source_meta['min_date']
            start_date = start_date or source_min_date + time_shift
        else:
            etl_first_run = dt.strptime(etl_first_run, DATETIME_FORMAT)
            etl_last_run = dt.strptime(etl_last_run, DATETIME_FORMAT)

            time_shift = etl_first_run.date() - dt.strptime(cycle_start, DATE_FORMAT).date()
            start_date = start_date or etl_last_run
        end_date = end_date or today

        self.logger.info("Data loading period: [{} - {}]".format(start_date.strftime(DATETIME_FORMAT), end_date.strftime(DATETIME_FORMAT)))

        extracted_iterable = self.extract(source, date_col, start_date-time_shift, end_date-time_shift)

        for data in tqdm(extracted_iterable):
            data[date_col] = data[date_col] + time_shift
            data[date_col] = data[date_col].dt.strftime(DATETIME_FORMAT)
            self.load(target, data)

        self.logger.info("End loading table '{}'".format(target))

        result = [
            {'table': target, 'param': 'first_run', 'value': etl_first_run.strftime(DATETIME_FORMAT), 'timestamp': today.strftime(DATETIME_FORMAT)}, 
            {'table': target, 'param': 'last_run', 'value': today.strftime(DATETIME_FORMAT), 'timestamp': today.strftime(DATETIME_FORMAT)}, 
            {'table': target, 'param': 'timedelta', 'value': str(time_shift.days), 'timestamp': today.strftime(DATETIME_FORMAT)}
        ]

        return result
