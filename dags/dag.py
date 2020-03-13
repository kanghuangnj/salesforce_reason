from __future__ import print_function

import datetime
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

import os,sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__))+'/../')
from lib import hot_location_longterm



args = {
    'owner': 'kang',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 3, 12),
}

dag_id = 'salesforce_recommendation_reason'

"""
Create a DAG to execute tasks
"""
dag = DAG(
    dag_id=dag_id,
    default_args=args,
    schedule_interval=None,
)

main_op = DummyOperator(
    task_id = 'Main_entrance',
    dag= dag,
)

end_op = DummyOperator(
    task_id = 'End',
    trigger_rule = 'none_failed',
    dag = dag,
)

task_get_reason = 'hot_location_longterm'
op_get_reason = PythonOperator(
    task_id=task_get_reason,
    python_callable=hot_location_longterm,
    dag=dag,
)


main_op >> op_get_reason >> end_op
