import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

args={'owner': 'airflow'}

default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_dwh_agg = DAG(
    dag_id = "dag_dwh_agg",
    default_args=args,
    schedule_interval='0 0 * * *',
    #schedule_interval='@once',	
    dagrun_timeout=timedelta(minutes=60),
    description='Run DWH aggregations',
    start_date = airflow.utils.dates.days_ago(1)
)

create_dwh_schema_query = """ 
                            create schema if not exists dwh;
                        """
create_agg_daily_user_todo_query = """ 
                                    CREATE table if not exists dwh.agg_daily_user_todo (
                                        user_id int NULL,
                                        registration_dt timestamp null,
                                        "date" date NULL,
                                        todo_count int NULL,
                                        list_count int NULL,
                                        finished_count int NULL
                                    );


                        """

insert_agg_daily_user_todo_query = """
                                    truncate table dwh.agg_daily_user_todo;
                                    insert into dwh.agg_daily_user_todo (user_id,registration_dt,date,todo_count,list_count,finished_count)
                                    select 
                                        coalesce(users.id,todo.creator_id) as user_id,
                                        users.registration_dt ,
                                        date(coalesce(todo.created_at,users.registration_dt)) as date, 
                                        count(1) as todo_count,
                                        count(distinct list_id) as list_count,
                                        count(case when is_finished then 1 else 0 end) finished_count
                                    from public."todo" as todo
                                    left join public.user as users
                                        on todo.creator_id = users.id 
                                    group by 1,2,3;
                        """

create_dim_user_query = """ 
                                    CREATE TABLE if not exists dwh.dim_user (
                                        user_id int NULL,
                                        registration_dt timestamp NULL,
                                        last_login_date date NULL,
                                        total_todo int NULL,
                                        total_list int NULL,
                                        total_finished_todo int NULL
                                    );


                        """

insert_dim_user_query = """
                                    truncate table dwh.dim_user;
                                    insert into dwh.dim_user (user_id,registration_dt,last_login_date,total_todo,total_list,total_finished_todo)
                                    select 
                                        user_id,
                                        registration_dt,
                                        max(date) as last_login_date, 
                                        sum(todo_count) as total_todo,
                                        sum(list_count) as total_list,
                                        sum(finished_count) as total_finished_todo
                                    from 
                                        dwh.agg_daily_user_todo
                                    group by 1,2;
                        """


create_dwh_schema_opt = PostgresOperator(
                                sql = create_dwh_schema_query,
                                task_id = "create_dwh_schema",
                                postgres_conn_id = "dwh",
                                dag = dag_dwh_agg
                                )

create_agg_daily_user_todo_table_opt = PostgresOperator(
                                sql = create_agg_daily_user_todo_query,
                                task_id = "create_agg_daily_user_todo",
                                postgres_conn_id = "dwh",
                                dag = dag_dwh_agg
                                )

insert_agg_daily_user_todo_opt = PostgresOperator(
                                sql = insert_agg_daily_user_todo_query,
                                task_id = "insert_agg_daily_user_todo",
                                postgres_conn_id = "dwh",
                                dag = dag_dwh_agg
                                )

create_dim_user_table_opt = PostgresOperator(
                                sql = create_dim_user_query,
                                task_id = "create_dim_user_query",
                                postgres_conn_id = "dwh",
                                dag = dag_dwh_agg
                                )

insert_dim_user_opt = PostgresOperator(
                                sql = insert_dim_user_query,
                                task_id = "insert_dim_user_query",
                                postgres_conn_id = "dwh",
                                dag = dag_dwh_agg
                                )
create_dwh_schema_opt >> create_agg_daily_user_todo_table_opt >> insert_agg_daily_user_todo_opt >> create_dim_user_table_opt >> insert_dim_user_opt