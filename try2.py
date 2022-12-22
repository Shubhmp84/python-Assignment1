import csv
import datetime
import logging
import psycopg2
import requests

API_URL = "https://jsonplaceholder.typicode.com/posts"
CREATED_ON = datetime.date.today().strftime("%Y-%m-%d")
CSV_FILENAME = "dataincsv2.csv"
USER_ID = 2

logging.basicConfig(filename="log2.log", level=logging.INFO)

response = requests.get(url=API_URL, )
data = response.json()
KEYS = [x for x in data[0].keys()]
KEYS.append("created")
print(KEYS)
print(data)


def write_csv(json_data, name: str):
    with open(f"{name}", mode="w", newline="", ) as file:
        writer = csv.writer(file)
        header = KEYS
        writer.writerow(header)
        for item in json_data:
            new_row = [x for x in item.values()]
            new_row.append(CREATED_ON)
            if new_row[0] == USER_ID:
                writer.writerow(new_row)
    logging.info("CSV file created")


def create_connection():
    connection = psycopg2.connect(user="postgres",
                                  password="Shubham@123",
                                  host="localhost",
                                  port="5432",
                                  database="assignment1")
    cursor = connection.cursor()
    return connection, cursor


def create_table(keys):
    sql_create = f"""
    create table user_data (
    {keys[0]} int,
    {keys[1]} int not null,
    {keys[2]} varchar(255),
    {keys[3]} text,
    {keys[4]} date
    );
    """
    try:
        connection1, cursor1 = create_connection()
        cursor1.execute(sql_create)
        connection1.commit()
        print("New table created")
        logging.info("New table created")
    except psycopg2.errors.DuplicateTable:
        logging.info("Table already Exists")
        if connection1:
            cursor1.close()
            connection1.close()
        return None
    except Exception as e:
        logging.error("Error while creating table ", e)
        print("Error while creating table ", e)
    finally:
        if connection1:
            cursor1.close()
            connection1.close()


def insert_data():
    try:
        connection2, cursor2 = create_connection()
        for item in data:
            if item[KEYS[0]] == USER_ID:
                values = [x for x in item.values()]
                values.append(CREATED_ON)
                insert_query = f"insert into user_data values {tuple(values)}"
                cursor2.execute(insert_query)
        connection2.commit()
        print("data inserted in table")
        logging.info("data inserted in table")

    except Exception as e:
        print("Error while inserting data ", e)
        logging.error("Error while inserting data ", e)
    finally:
        if connection2:
            cursor2.close()
            connection2.close()


write_csv(data, CSV_FILENAME)

create_table(KEYS)

insert_data()
