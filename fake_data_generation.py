import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import reflection
from faker import Factory
from random import randrange
from datetime import date 

engine   = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1:8432', pool_recycle=3600)
inspector = inspect(engine)
fake = Factory.create()
today = date.today()

number_of_users = 5
number_of_lists = 20
number_of_todos = 50


def generate_last_login_date(row,start_range,end_range):
    """ 
        Generating last_login must be bigger then the registration date and but not bigger then today
    """

    last_login_generated = row['registration_dt'] + pd.Timedelta(days=randrange(start_range,end_range))
    return today if last_login_generated > today else last_login_generated

def generate_creation_date(row,users,lists=None,object_type=None,field=None):
    """ 
        Generation of Creation Date can be different for multiple scenarios and datasets
    """
    id = row["creator_id"]
    user = users.loc[id]
    if object_type=="todo":
        todo_list = lists.loc[row["list_id"]]
        return fake.date_time_between(start_date = todo_list["created_at"], end_date = user["last_login_dt"]) if field=="created_at" \
            else fake.date_time_between(start_date = row["created_at"], end_date = user["last_login_dt"])
    else:
        return fake.date_time_between(start_date = user["registration_dt"], end_date = user["last_login_dt"]) 

def store(dataframe, table_name):
    dataframe.to_sql(table_name, con=engine, if_exists="append", chunksize=100)


users = pd.DataFrame(index = range(1,number_of_users))
users.index.name="id"
users['user_name'] = users.index.map(lambda x : fake.user_name())
users['email'] = users.index.map(lambda x : fake.email())
users['password_hash'] = users.index.map(lambda x : fake.password())
users['registration_dt'] = users.index.map(lambda x : fake.date_time_between(start_date = "-3y", end_date = "now"))
users['last_login_dt'] = users.apply (lambda row: fake.date_time_between(start_date = row["registration_dt"], end_date = today), axis=1)
users['is_admin'] = users.index.map(lambda x : fake.boolean(chance_of_getting_true=1))

print(users.head())


""" 
    Creating the lists, we need to generate the title, creator_id and 
    the creation time (which must be between registration and last login)
"""
lists = pd.DataFrame(index=range (1,number_of_lists))
lists.index.name = "id" 
lists['title'] = lists.index.map(lambda x: fake.word(ext_word_list=['Feed the dog', 'Water the flowers', 'Learn', 'Meditate','Grocery','Bake Cake','Write Blog']))
lists['creator_id'] = lists.index.map(lambda x: fake.random_int(min=1,max=number_of_users-1))
lists['created_at'] = lists.apply(lambda row: generate_creation_date(row,users),axis=1)
print (lists.head(20))

todo = pd.DataFrame(index=range (1,number_of_todos))
todo.index.name = "id"
#todo['description'] = todo.index.map(lambda x: fake.words(nb=5))
todo['description'] = todo.index.map(lambda x: fake.word(ext_word_list=['Planning', 'Start', 'Check Status', 'Postpone','Call']))

todo['is_finished'] = todo.index.map(lambda x: fake.boolean(chance_of_getting_true=25))
todo['creator_id'] = todo.index.map(lambda x: fake.random_int(min=1,max=number_of_users-1))
todo['list_id'] = todo.index.map(lambda x: fake.random_int(min=1,max=number_of_lists-1))

""" 
    List's creation is prior to the todo creation so we must generating todo after the 
    list creation and before the last_login in the user table
"""
todo['created_at'] = todo.apply(lambda row: generate_creation_date(row,users,lists ,"todo","created_at"),axis=1)
todo['finished_at'] = todo.apply(lambda row: generate_creation_date(row,users,lists,"todo","finished_at"),axis=1)

print(todo.head(50))
#print(todo.head(50).where(todo["created_at"]>todo["finished_at"]).dropna())

store(users,'user')
store(lists, 'list')
store(todo, 'todo')