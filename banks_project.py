from datetime import datetime

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3


def log_progress(message):
    timeformat = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timeformat)
    log = f'{timestamp} : {message}'
    with open('code_log.txt', 'a') as file:
        file.write(f'{log}\n')


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
output_path = './Largest_banks_data.csv'
table_name = 'Largest_banks'
db_name = 'Banks.db'

log_progress('Preliminaries complete. Initiating ETL process')


def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)
    page = requests.get(url).text
    parsed_page = BeautifulSoup(page, 'html.parser')
    table = parsed_page.find_all('tbody')
    rows = table[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            country = col[1].find('a')['title'].strip()
            cap = float(col[2].contents[0][:-1])
            dict_data = {
                table_attribs[0]: country,
                table_attribs[1]: cap
            }
            df1 = pd.DataFrame(dict_data, index=[0])
            df1 = df1.dropna(axis=1, how='all')
            if not df1.empty:
                df = pd.concat([df, df1], ignore_index=True)
    log_progress('Data extraction complete. Initiating Transformation process')
    return df


def transform(df, csv_path):
    file = pd.read_csv(csv_path)
    exchange_rate = file.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 0) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 0) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 0) for x in df['MC_USD_Billion']]
    print(df['MC_EUR_Billion'][4])

    log_progress('Data transformation complete. Initiating Loading process')
    return df


def load_to_csv(df, output_path):
    df.to_csv(output_path)
    log_progress('Data saved to CSV file')


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress('Data loaded to Database as a table, Executing queries')
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''


def run_query(query_statement, sql_connection):
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    log_progress('Process Complete')

# print(extract(url, table_attribs))
# (extract(url, table_attribs))
# df = (extract(url, table_attribs))
# # print(transform(df, 'exchange_rate.csv'))
# df = transform(df, 'exchange_rate.csv')
# load_to_csv(df, output_path)


# load_to_db(df, sql_connection, table_name)
# q1 = 'SELECT * FROM Largest_banks'
# q2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
# q3 = 'SELECT Name from Largest_banks LIMIT 5'
# run_query(q1, sql_connection)
# run_query(q2, sql_connection)
# run_query(q3, sql_connection)

def executing_pipeline():
    df = (extract(url, table_attribs))
    df = transform(df, 'exchange_rate.csv')
    load_to_csv(df, output_path)
    sql_connection = sqlite3.connect(db_name)
    log_progress('SQL Connection initiated')
    load_to_db(df, sql_connection, table_name)
    q1 = 'SELECT * FROM Largest_banks'
    q2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
    q3 = 'SELECT Name from Largest_banks LIMIT 5'
    run_query(q1, sql_connection)
    run_query(q2, sql_connection)
    run_query(q3, sql_connection)
    sql_connection.close()
    log_progress('Server Connection closed')


executing_pipeline()
