
# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("banks_project_log.txt","a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    df = pd.DataFrame(columns=table_attribs)
    count = 0
    for row in rows:
        if count < 10:
            col = row.find_all('td')
            if len(col) != 0:
                data_dict = {"Country": col[1].find_all('a')[1].get('title'),
                             "MC_USD_Billion": float(col[2].contents[0])}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
                count += 1
        else:
            break
    return df




def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.read_csv(csv_path)

    exchange_rate = exchange_rate.set_index("Currency").to_dict()["Rate"]

    df["MC_GBP_Billion"] = [np.round(x * exchange_rate["GBP"], 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [np.round(x * exchange_rate["EUR"], 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(x * exchange_rate["INR"], 2) for x in df["MC_USD_Billion"]]

    return df



def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(csv_output_path)




def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index = False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)






''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''



url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs=["Country", "MC_USD_Billion"]
table_name = 'Banks'
csv_path = '/home/project/exchange_rate.csv'
csv_output_path = './banks_project.csv'
log_progress('Preliminaries complete. Initiating ETL process')




df = extract(url)
log_progress('Data extraction complete. Initiating Transformation process')




df = transform(df, csv_path)
log_progress('Data transformation complete. Initiating Loading process')




load_to_csv(df, csv_output_path)

log_progress('Data saved to CSV file')




sql_connection = sqlite3.connect('banks_project.db')

log_progress('SQL Connection initiated')

load_to_db(df,sql_connection,table_name)

log_progress('Data loaded to Database as a table, Executing queries')





query_statement1 = f"SELECT * FROM Banks"

run_query(query_statement1, sql_connection)

query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM Banks"

run_query(query_statement2, sql_connection)

query_statement3 = f"SELECT country from Banks LIMIT 5"

run_query(query_statement3, sql_connection)

log_progress('Process Complete')

sql.connection.close()

log_progress('Server Connection closed')