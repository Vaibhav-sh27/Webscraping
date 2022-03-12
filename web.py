from logging import root
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import sqlite3
import pandas as pd
from sqlalchemy import create_engine



while(True):
    now = datetime.now()

    # this is just to get the time at the time of
    # web scraping
    current_time = now.strftime("%H:%M:%S")
    print(f'At time : {current_time} IST')

    response = requests.get('https://finance.yahoo.com/cryptocurrencies/')
    text = response.text
    html_data = BeautifulSoup(text, 'html.parser')
    headings = html_data.find_all('tr')[0]
    headings_list = []
    for x in headings:
        headings_list.append(x.text)
    headings_list = headings_list[:9]

    data = []

    for x in range(1, 10):
        row = html_data.find_all('tr')[x]
        column_value = row.find_all('td')
        dict = {}

        for i in range(9):
            dict[headings_list[i]] = column_value[i].text
        data.append(dict)

    df = pd.DataFrame.from_dict(data)
    
    tableName   = "status"
    sqlEngine       = create_engine('mysql+pymysql://root:2310@localhost/coins', pool_recycle=3600)
    dbConnection    = sqlEngine.connect()

    try:

        frame           = df.to_sql(tableName, dbConnection, if_exists='replace');

    except ValueError as vx:

        print(vx)

    except Exception as ex:   

        print(ex)

    else:

        print("Table %s created successfully."%tableName);   

    finally:

        dbConnection.close()
    time.sleep(120)