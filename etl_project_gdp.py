import pandas as pd 
import numpy as np
import requests
from datetime import datetime
import sqlite3
from bs4 import BeautifulSoup

URL='https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

table_attributes=['Country','GDP_USD_millions']
db_name='World_Economies.db'
conn=sqlite3.connect(db_name)
table_name='Countries_by_GDP'
csv_path='Countries_by_GDP.csv'

##EXTRACTION

def extract(url, table_attributes):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attributes)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df
    
    
##TRANSFORMATION

#Convert the contents of the 'GDP_USD_millions' column of df dataframe from currency format to floating numbers.
#Divide all these values by 1000 and round it to 2 decimal places.
#Modify the name of the column from 'GDP_USD_millions' to 'GDP_USD_billions'.
def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df

##LOADING
def load_to_csv(df,csv_path):
    df.to_csv(csv_path)
    
def  load_to_db(df,conn,table_name):
    
    df.to_sql(table_name,conn, if_exists='replace', index=False)
    
    
 ##QUERYING THE DATABASE TABLE
def run_query(conn,table_name):
    query=f"SELECT * FROM {table_name}"
    query_output=pd.read_sql(query, conn)
    print(query_output)
    
    
 ##LOGGING
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
        
        
# EXTRACTION
log_progress("ETL Job Started")
log_progress("Extract phase Started")
df = extract(URL, table_attributes)
print("Extracted Data:")
print(df)
log_progress("Extract phase Ended")

# TRANSFORMATION
log_progress("Transform phase Started")
df = transform(df)
print("\nTransformed Data:")
print(df)
log_progress("Transform phase Ended")

# LOADING
log_progress("Load phase Started")
load_to_csv(df, csv_path)
print("\nData saved to CSV")
load_to_db(df, conn, table_name)
print("Data saved to Database")
log_progress("Load phase Ended")

# QUERYING
print("\nQuery Output:")
run_query(conn, table_name)

log_progress("ETL Job Ended")        
        

        
    
     
    
    
    
      


