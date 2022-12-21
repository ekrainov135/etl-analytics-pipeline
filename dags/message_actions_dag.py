from datetime import timedelta as td
import os

from airflow.decorators import dag, task
import pendulum

from etl import ctl_metadata_get, ctl_metadata_update, context_init as context_init_method
from etl.builder import ETLDagBuilder
from utils import get_ch_config
from utils.logger import get_logger
from utils.sql import MESSAGE_ACTIONS_CREATE, DROP_TABLE
from utils.db import Driver
from settings import WORKDIR, LOGDIR, TIMEZONE


SCHEDULE_INTERVAL = '*/5 * * * *'

default_args = {
    'owner': 'airflow',
    'wait_for_downstream': True,
    'depends_on_past': True,
    'retries': 5,
    'retry_delay': td(minutes=2),
    'start_date': pendulum.datetime(year=2022, month=11, day=13, tz=TIMEZONE),
    'task_concurrency': 1
}


@dag(default_args=default_args, schedule_interval=SCHEDULE_INTERVAL, catchup=False, tags=['from_parquet', 'to_clickhouse'])
def message_actions_dag():

    @task()
    def context_init():
        return self.context_init()

    @task()
    def ctl_init(context, table, params, defaults):
        # Init metadata from CTL

        data = self.ctl_metadata_get(self.target_driver, table, params)

        if len(data) == 0:
            self.target_driver.exec(*self.db_reset_sql)
            for key, val in zip(params, defaults):
                data.append({'table': table, 'param': key, 'value': val, 'timestamp': context['exec_date']})

            data = self.ctl_metadata_update(self.target_driver, table, data=data)
        else:
            for row in data:
                row['timestamp'] = context['exec_date']

        return data

    @task()
    def run_message_actions(metadata):
        result = self.run(metadata, date_col='time')
        return result

    @task()
    def ctl_update(metadata):
        result = self.ctl_metadata_update(self.target_driver, table, metadata)
        return result

    table = 'test.message_actions'
    db_reset_sql = [DROP_TABLE.format(table=table), MESSAGE_ACTIONS_CREATE]

    dag_logger = get_logger(os.path.join(LOGDIR, '{}.log'.format(table)))

    connection = get_ch_config()
    self = ETLDagBuilder(logger=dag_logger, 
                         source_driver=Driver('parquet', os.path.join(WORKDIR, 'data', 'parquet')),
                         target_driver=Driver('clickhouse', connection),
                         db_reset_sql=db_reset_sql)
    self.add_method('context_init', context_init_method, True) \
        .add_method('ctl_metadata_get', ctl_metadata_get, True) \
        .add_method('ctl_metadata_update', ctl_metadata_update, True)

    params_default = {'source': 'message_actions.parquet', 
                      'cycle_start': '2022-11-01', 
                      'cycle_end': '2022-12-01', 
                      'first_run': None, 
                      'last_run': None
    }

    # Taskflow pipeline
    context_init_task = context_init()
    ctl_init_task = ctl_init(context=context_init_task, 
                             table=table, 
                             params=params_default.keys(),
                             defaults=params_default.values())
    run_message_actions_task = run_message_actions(ctl_init_task)
    run_message_actions_task.set_upstream(context_init_task)
    ctl_update_task = ctl_update(run_message_actions_task)


message_actions_dag = message_actions_dag()
