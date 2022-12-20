import csv
import datetime
import logging
import psycopg2
import requests


API_URL = "https://jsonplaceholder.typicode.com/posts"
logging.basicConfig(filename= "log.log", level= logging.INFO)


def get_data(url):

    response = requests.get(url)
    all_data = response.json()
    return all_data


data = get_data(API_URL)
logging.info(data)
# print(data)
date = datetime.date.today()
created_on = date.strftime("%Y-%m-%d")


def database_update(data):
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="Shubham@123",
                                      host="localhost",
                                      port="5432",
                                      database="assignment1"
                                      )
        cursor = connection.cursor()
        query = """create table data(
                userId int,
                Id int,
                title varchar(255),
                body text,
                created date,
                primary key (Id)
                )"""
        cursor.execute(query)
        connection.commit()
        logging.info("table created")

    except Exception as e:
        logging.error(e)
        print(e)
    else:
        for item in data:
            values = [x for x in item.values()]
            if values[0] == 2:
                insert_data = f"insert into data values {tuple(values)} "
                cursor.execute(insert_data)
        connection.commit()
    finally:
        if connection:
            cursor.close()
            connection.close()




def write_to_csv(data):
    with open("dataincsv.csv", mode='w', newline="") as file:
        header = [f"{x}" for x in data[0].keys()]
        header.append("Created")
        write = csv.writer(file)
        write.writerow(header)
        for item in data:
            values = [item['userId'], item['id'], item['title'], f"{item['body']}", created_on]
            if values[0] ==2:
                write.writerow(values)


write_to_csv(data)

database_update(data)


