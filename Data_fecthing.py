# importing required third party modules.
import requests
import pandas as pd
from sqlalchemy import create_engine
import logging

# logging configuration
logging.basicConfig(filename='app.log', filemode='w',format='%(asctime)s - %(message)s', level=logging.DEBUG)
logging.info('Connection made successfully')

# fecthing API data using request module.
Data = requests.get("https://jsonplaceholder.typicode.com/posts")
logging.info(Data.status_code)

# Converting API into Json
json_data = Data.json()

# pandas as database connection using sqlalchemy create_engine
conn_string = 'postgresql://postgres:Anex08@localhost/DATA FETECHING ASSIGNMENT'
db = create_engine(conn_string)
connection = db.connect()
logging.info('pandas as database connection made successfully')

# pandas dataframe
data = pd.DataFrame(json_data)

# pandas dataframe into CSV
csv_data = data.to_csv()

try:
# creating an table using pandas module
    sql_create_table = pd.read_sql_query("""create table data(
                userId int,
                Id int,
                title varchar(255),
                body text,
                created date,
                primary key (Id)
                )""", connection)

# inserting an data into connected table in database.
    data.to_sql("data", connection, if_exists="replace", index=False)

except:
    logging.warning('table is already created')

