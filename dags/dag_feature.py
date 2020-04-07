from __future__ import print_function

import datetime
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

import os,sys
FEATLIB_PATH = '/Users/kanghuang/Documents/work/location_recommendation/salesforce_model'
sys.path.insert(0, FEATLIB_PATH)
from feature_lib.function import feat_function, merge_feat, rating_gen

args = {
    'owner': 'kang',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 3, 12),
}

dag_id = 'salesforce_feature_generation'
tables = ['account', 'location_scorecard', 'building']

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


rating_op = PythonOperator(
    task_id='rating',
    python_callable=rating_gen,
    dag=dag,
)

merging_op = PythonOperator(
    task_id='merge_all_features',
    provide_context=True,
    python_callable=merge_feat,
    op_kwargs={'names': tables},
    trigger_rule = 'all_done',
    dag=dag,
)

# save_op = PythonOperator(
#     task_id='save',
#     provide_context=True,
#     python_callable=save_feat,
#     trigger_rule = 'success',
#     dag=dag,
# )
main_op >> rating_op >> merging_op
feat_ops = {}
for name in feat_function:
    feat_ops[name] = PythonOperator(
                                task_id=name,
                                python_callable=feat_function[name],
                                dag=dag,
                            )
    main_op >> feat_ops[name] >> merging_op
# merging_op >> save_op