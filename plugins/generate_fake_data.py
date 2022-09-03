from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import reflection
from faker import Factory
from random import randrange
from datetime import date

class GenerateFakeDataOperator(BaseOperator):

    ui_color = '#358140'    
    @apply_defaults
    def __init__(
                 self,
                 pg_conn_id="",
                 number_of_users = "",
                 number_of_lists="",
                 number_of_todos = "",
                 log_json_file = "",
                 *args, **kwargs):

        super(GenerateFakeDataOperator, self).__init__(*args, **kwargs)
        self.pg_conn_id = pg_conn_id
        self.number_of_users = number_of_users
        self.number_of_lists = number_of_lists
        self.number_of_todos = number_of_todos
        self.log_json_file = log_json_file

        self.today = date.today()
        self.fake = Factory.create()
        self.pg_connection = PostgresHook(postgres_conn_id = self.pg_conn_id).get_uri()
        


    
    def generate_last_login_date(row,start_range,end_range):
        """ 
            Generating last_login must be bigger then the registration date and but not bigger then today
        """

        last_login_generated = row['registration_dt'] + pd.Timedelta(days=randrange(start_range,end_range))
        return self.today if last_login_generated > self.today else last_login_generated

    def generate_creation_date(self,row,users,lists=None,object_type=None,field=None):
        """ 
            Generation of Creation Date can be different for multiple scenarios and datasets
        """
        id =  row["creator_id"]
        user = users.loc[id]
        if object_type=="todo":
            todo_list = lists.loc[row["list_id"]]
            return self.fake.date_time_between(start_date = todo_list["created_at"], end_date = user["last_login_dt"]) if field=="created_at" \
                else self.fake.date_time_between(start_date = row["created_at"], end_date = user["last_login_dt"])
        else:
            return self.fake.date_time_between(start_date = user["registration_dt"], end_date = user["last_login_dt"]) 

    def store(self,dataframe, table_name):
        
        dataframe.to_sql(table_name, con=self.pg_connection, if_exists="append", chunksize=100) 

    def execute(self, context):
        
        self.log.info(f"Generating {self.number_of_users} users, using {self.number_of_lists} lists and {self.number_of_todos} todos ")
        
        postgres_hook = PostgresHook(postgres_conn_id = self.pg_conn_id)
        

        users = pd.DataFrame(index = range(1,self.number_of_users))
        users.index.name="id"
        users['user_name'] = users.index.map(lambda x : self.fake.user_name())
        users['email'] = users.index.map(lambda x : self.fake.email())
        users['password_hash'] = users.index.map(lambda x : self.fake.password())
        users['registration_dt'] = users.index.map(lambda x : self.fake.date_time_between(start_date = "-3y", end_date = "now"))
        self.log.info(f"Generating {self.today}s ")

        users['last_login_dt'] = users.apply (lambda row: self.fake.date_time_between(start_date = row["registration_dt"], end_date = self.today), axis=1)
        users['is_admin'] = users.index.map(lambda x : self.fake.boolean(chance_of_getting_true=1))
        self.log.info("Users generation")

        lists = pd.DataFrame(index=range (1,self.number_of_lists))
        lists.index.name = "id" 
        lists['title'] = lists.index.map(lambda x: self.fake.word(ext_word_list=['Feed the dog', 'Water the flowers', 'Learn', 'Meditate','Grocery','Bake Cake','Write Blog']))
        lists['creator_id'] = lists.index.map(lambda x: self.fake.random_int(min=1,max=self.number_of_users-1))
        lists['created_at'] = lists.apply(lambda row: self.generate_creation_date(row,users),axis=1)
        self.log.info("lists generation")
        
        todo = pd.DataFrame(index=range (1,self.number_of_todos))
        todo.index.name = "id"
        #todo['description'] = todo.index.map(lambda x: self.fake.words(nb=5))
        todo['description'] = todo.index.map(lambda x: self.fake.word(ext_word_list=['Planning', 'Start', 'Check Status', 'Postpone','Call']))

        todo['is_finished'] = todo.index.map(lambda x: self.fake.boolean(chance_of_getting_true=25))
        todo['creator_id'] = todo.index.map(lambda x: self.fake.random_int(min=1,max=self.number_of_users-1))
        todo['list_id'] = todo.index.map(lambda x: self.fake.random_int(min=1,max=self.number_of_lists-1))
        """ 
            List's creation is prior to the todo creation so we must generating todo after the 
            list creation and before the last_login in the user table
        """
        todo['created_at'] = todo.apply(lambda row: self.generate_creation_date(row,users,lists ,"todo","created_at"),axis=1)
        todo['finished_at'] = todo.apply(lambda row: self.generate_creation_date(row,users,lists,"todo","finished_at"),axis=1)

        self.log.info("todos generation")

        self.store(users,'user')
        self.store(lists, 'list')
        self.store(todo, 'todo')

        self.log.info("todos generation")