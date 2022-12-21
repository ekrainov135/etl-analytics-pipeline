import os


# Directory params
WORKDIR = os.path.dirname(os.path.abspath(__file__))
LOGDIR = os.path.join(WORKDIR, 'logs')

# Common data warehouse params
BATCH_SIZE = 50000

# Clickhouse params
CLICKHOUSE_HOST = 'http://172.18.0.1:8123'
DATABASE = 'test'
CTL_SCHEMA = 'test'
CTL_TABLE_LOAD_INFO = 'ctl_metadata'

# Date and time params
TIMEZONE = 'Europe/Moscow'
DATETIME_FORMAT = '%Y-%m-%d %X'
DATE_FORMAT = '%Y-%m-%d'

