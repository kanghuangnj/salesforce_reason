from __future__ import print_function

import datetime
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

import os,sys
pj = os.path.join
LIBPATH = pj(os.path.dirname(os.path.abspath(__file__)), '../../salesforce_reason')
sys.path.insert(0, LIBPATH)
from reason_lib import *

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

context_op = PythonOperator(
    task_id='generate_context',
    python_callable=generate_context,
    dag=dag,
)

merging_op = PythonOperator(
    task_id='merge_all_reasons',
    provide_context=True,
    python_callable=merge_reasons,
    op_kwargs={'reason_names': list(reason_function.keys())},
    trigger_rule = 'all_done',
    dag=dag,
)

end_op = PythonOperator(
    task_id = 'end',
    provide_context=True,
    trigger_rule = 'all_success',
    python_callable=post_processing,
    dag = dag,
)

# reason_ops = {}
# main_op >> context_op
# for name in reason_function:
#     reason_ops[name] = PythonOperator(
#                                 task_id=name,
#                                 provide_context=True,
#                                 python_callable=reason_function[name],
#                                 dag=dag,
#                             )
#     context_op >> reason_ops[name] >> merging_op
# merging_op >> end_op