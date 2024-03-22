# wget -o exchange_rate.csv https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv
#python3.11 -m pip install requests bs4 pandas numpy

# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_path,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    #print(data)
    with open('parsed_html.html', 'w', encoding='utf-8') as file:
        file.write(data.prettify())
        
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('table')
    target_table = tables[0]

    if target_table:
        df = pd.DataFrame(columns=["Name", "MC_USD_Billion"])
        rows = target_table.find_all('tr')[1:]  # Exclude header row
        for row in rows:
            cols = row.find_all(['th', 'td'])
            if len(cols) >= 3:
                bank_name = cols[1].text.strip()
                market_cap = cols[2].text.strip()
                df = pd.concat([df, pd.DataFrame({"Name": [bank_name], "MC_USD_Billion": [market_cap]})], ignore_index=True)
        print(df)
        return df
    else:
        print("Target table not found.")
        return None


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rates = pd.read_csv(csv_path)
    
    eur_rate = exchange_rates.loc[exchange_rates['Currency'] == 'EUR', 'Rate'].values[0]
    gbp_rate = exchange_rates.loc[exchange_rates['Currency'] == 'GBP', 'Rate'].values[0]
    inr_rate = exchange_rates.loc[exchange_rates['Currency'] == 'INR', 'Rate'].values[0]

    #print("EUR Rate:", eur_rate)
    #print("GBP Rate:", gbp_rate)
    #print("INR Rate:", inr_rate)
    
    # Convert 'MC_USD_Billion' column to numeric type
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'])
    # Convert market capitalization to GBP, EUR, and INR
    df['MC_GBP_Billion'] = df['MC_USD_Billion'] * gbp_rate
    df['MC_EUR_Billion'] = df['MC_USD_Billion'] * eur_rate
    df['MC_INR_Billion'] = df['MC_USD_Billion'] * inr_rate

    # Round the values to 2 decimal places
    df['MC_GBP_Billion'] = df['MC_GBP_Billion'].round(2)
    df['MC_EUR_Billion'] = df['MC_EUR_Billion'].round(2)
    df['MC_INR_Billion'] = df['MC_INR_Billion'].round(2)

    print(df)
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    

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
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
log_path = './code_log.txt'
csv_conversion_path = 'C:/Users/KIIT/Desktop/Python Project for Data Engineering Course/Project/exchange_rate.csv'

	
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_conversion_path)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as a table, Executing queries')

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) from {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)


log_progress('Process Complete.')

sql_connection.close()

log_progress('Server Connection closed.')
