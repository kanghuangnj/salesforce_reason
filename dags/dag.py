from __future__ import print_function

import datetime
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

import os,sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__))+'/../')
from lib import *

args = {
    'owner': 'kang',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 3, 12),
}

dag_id = 'salesforce_recommendation_reason'

# independent_reasons = {
#     'hot_location_longterm': hot_location_longterm,
#     'hot_location_occupancy': hot_location_occupancy,
#     'hot_location_shortterm': hot_location_shortterm,
# }


"""
Create a DAG to execute tasks
"""
dag = DAG(
    dag_id=dag_id,
    default_args=args,
    schedule_interval=None,
)

main_op = DummyOperator(
    task_id = 'main_entrance',
    dag= dag,
)

generate_pair_op = PythonOperator(
    task_id='generate_pairs',
    python_callable=generate_pairs,
    dag=dag,
)

# end_op = DummyOperator(
#     task_id = 'End',
#     trigger_rule = 'none_failed',
#     dag = dag,
# )

merging_op = PythonOperator(
    task_id='merging_all_reasons',
    provide_context=True,
    python_callable=merge_reasons,
    op_kwargs={'reason_names': list(reason_function.keys())},
    trigger_rule = 'all_done',
    dag=dag,
)

reason_ops = {}
for name in reason_function:
    reason_ops[name] = PythonOperator(
                                task_id=name,
                                python_callable=reason_function[name],
                                dag=dag,
                            )
    main_op >> reason_ops[name] >> merging_op
