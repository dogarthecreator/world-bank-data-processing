# Code for ETL operations on Country-GDP data

# Importing the required libraries

from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3

table_attribs = ["Name","MC_USD_Billion"]
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_csv = 'exchange_rate.csv'
output_csv_path = './output.csv'
table_name = 'Largest_banks'
db_name = 'Banks.db'
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"{time_stamp} : {message}\n"
    with open('log.txt','a') as f:
        f.write(log_message)


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')

    df = pd.DataFrame(columns = table_attribs)


    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        
        if len(cols) != 0:
            anchors = cols[1].find_all('a')
            if anchors:
                data_dict = { 
                    "Name": anchors[1].text,
                    "MC_USD_Billion": cols[2].contents[0]
                }
                new_df = pd.DataFrame(data_dict,index =[0])
                df = pd.concat([df,new_df],ignore_index=True)
        
    return df



def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_df = pd.read_csv(csv_path)
    exchange_df.set_index('Currency',inplace=True)

    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace('\n','').astype(float)

    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion']*exchange_df.loc['GBP']['Rate'],2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion']*exchange_df.loc['EUR']['Rate'],2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion']*exchange_df.loc['INR']['Rate'],2)

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path,index = False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name,sql_connection,if_exists = 'replace', index = False)



def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement,sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('ETL process started')

log_progress('Starting data extraction')
df = extract(url, table_attribs)
log_progress('Data extraction completed')

log_progress('Starting data transformation')
df = transform(df, exchange_csv)
log_progress('Data transformation completed')

log_progress('Starting CSV load')
load_to_csv(df, output_csv_path)
log_progress('CSV load completed')

log_progress('Starting database load')
sql_connection = sqlite3.connect(db_name)
load_to_db(df, sql_connection, table_name)
log_progress('Database load completed')

# Hardcoded Queries
query1 = 'SELECT * FROM Largest_banks'
query2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query3 = 'SELECT Name FROM Largest_banks LIMIT 5'

log_progress('Running queries')
run_query(query1, sql_connection)
run_query(query2, sql_connection)
run_query(query3, sql_connection)

log_progress('ETL process completed')
sql_connection.close()