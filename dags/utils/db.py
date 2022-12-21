from abc import ABC, abstractmethod
import hashlib
import os
from random import random
#import pyarrow as pa
#import pyarrow.parquet as pq

import pandas as pd
import pyarrow.parquet as pq
import pandahouse as ph

from utils.sql import TEMP_VIEW_CREATE, TEMP_VIEW_DROP, TEMP_VIEW_SELECT
from settings import BATCH_SIZE, CTL_SCHEMA


class BaseReader(ABC):
    @abstractmethod
    def read(self, *args, **kwargs):
        pass


class BaseWriter(ABC):
    @abstractmethod
    def write(self, *args, **kwargs):
        pass


class BaseDriver(BaseReader, BaseWriter):
	pass


class DBDriver(BaseDriver):
    @abstractmethod
    def __init__(self, connection, *args, **kwargs):
        pass

    @abstractmethod
    def exec(self, *args, **kwargs):
        pass


class ParquetDriver(BaseDriver):
    def __init__(self, workdir='./'):
        self.workdir = workdir

    def read(self, path, columns=None, batch_size=65536):
        parquet_file = pq.ParquetFile(os.path.join(self.workdir, path))

        for batch in parquet_file.iter_batches(columns=columns, batch_size=batch_size):
            yield batch.to_pandas()

    def write(self):
    	# TODO: complete the method
        return None

        pqwriter = None
        flag = True
        for i, df in enumerate(generate_df_by_slice(date_range_list)):
            table = pa.Table.from_pandas(df)
            if df.shape[0] == 0:
                continue
            if flag:
                pqwriter = pq.ParquetWriter('ma.parquet', table.schema)
                flag = False
            pqwriter.write_table(table)

        # close the parquet writer
        if pqwriter:
            pqwriter.close()


class ClickhouseDriver(DBDriver):
    def __init__(self, connection):
        self.connection = connection

    def read(self, table, columns=None, condition=None, batch_size=20000):
        table_hash = hashlib.md5((table + str(random())).encode()).hexdigest()
        view_name = 'v_{}_{}_temp'.format(table.split('.')[-1], table_hash)

        ph.execute(TEMP_VIEW_DROP.format(view=view_name), connection=self.connection)
        ph.execute(TEMP_VIEW_CREATE.format(view=view_name, table=table), connection=self.connection)

        condition = condition or '1=1'
        select_str = ', '.join(columns) if columns else '*'
        query = TEMP_VIEW_SELECT.format(columns=select_str, table=table, cond=condition, limit=batch_size, offset=0)

        batch = ph.read_clickhouse(query, connection=self.connection)
        yield batch

        i = 0
        while batch.shape[0] == batch_size:
            i += 1
            query = TEMP_VIEW_SELECT.format(columns=', '.join(columns), table=table, cond=condition, limit=batch_size, offset=i*batch_size)
            batch = ph.read_clickhouse(query, connection=self.connection)
            yield batch

        ph.execute(TEMP_VIEW_DROP.format(view=view_name), connection=self.connection)

    def write(self, table, data, overwrite=False):
        ph.to_clickhouse(df=data, table=table.split('.')[-1], index=False, connection=self.connection)

    def exec(self, *queries):
    	for query in queries:
    		ph.execute(query, connection=self.connection)


def Driver(driver_type, *args, **kwargs):
    drivers = {
    	'parquet': ParquetDriver,
        'clickhouse': ClickhouseDriver
    }
    return drivers[driver_type](*args, **kwargs)
