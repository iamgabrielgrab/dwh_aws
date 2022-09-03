from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from generate_fake_data import GenerateFakeDataOperator
from airflow.utils.dates import days_ago

log_json_file = "log_json_path.json"
pg_conn_id = "dwh"
number_of_users = 5
number_of_lists=10
number_of_todos = 20

default_args = {
    'owner': 'Erez',
    #'depends_on_past': True,
    'start_date': '2022-09-01',
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    #'catchup': True
}

dag_name = 'fake_data_generator' 
dag = DAG(dag_name,
          default_args=default_args,
          description='Populate Todo list app with fake data'      
        )
print("Test")
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

print("Test")
generate_fake_data_to_pg = GenerateFakeDataOperator(
    task_id='generate_fake_data',
    pg_conn_id=pg_conn_id,
    number_of_users = number_of_users,
    number_of_lists=number_of_lists,
    number_of_todos = number_of_todos,
    log_json_file = "",
    dag=dag
    #*args, **kwargs
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> generate_fake_data_to_pg >> end_operator
