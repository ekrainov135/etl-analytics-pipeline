from airflow.operators.python import get_current_context
import pandas as pd

from utils.sql import METADATA_DELETE
from settings import DATETIME_FORMAT, TIMEZONE, CTL_SCHEMA, CTL_TABLE_LOAD_INFO


def ctl_metadata_get(db_driver, table, params=[]):
    condition = "table='{}'".format(table)
    if params:
        condition += " and param in({})".format(', '.join(["'{}'".format(x) for x in params]))
    data = next(db_driver.read('{db}.{table}'.format(db=CTL_SCHEMA, table=CTL_TABLE_LOAD_INFO), condition=condition, batch_size=1000))

    return data.to_dict(orient='records')


def ctl_metadata_update(db_driver, table, data=None):
    params = ["'{}'".format(x['param']) for x in data]
    condition = "table='{}' and param in({})".format(table, ', '.join(params))

    db_driver.exec(METADATA_DELETE.format(db=CTL_SCHEMA, cond=condition))
    db_driver.write('{db}.{table}'.format(db=CTL_SCHEMA, table=CTL_TABLE_LOAD_INFO), pd.DataFrame(data))

    return data


def context_init():
    result = {}
    context = get_current_context()
    result['exec_date'] = context['execution_date'].in_timezone(TIMEZONE).strftime(DATETIME_FORMAT)
    return result


