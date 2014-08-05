#------------------------------------------------
# modules
#------------------------------------------------

import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pymysql as mdb

# user written module for specific task
from umls_to_mysql_queries import createTable

#------------------------------------------------
# parameters
#------------------------------------------------
to_load = 100 # how many rows to load (for prototyping)

# path names
location_code = '/Users/Friederike/Dropbox/Insight/Project/FDA/code'
location_data = '/Users/Friederike/Dropbox/Insight/Project/FDA/data/RxNorm_full_08042014/rrf'

#------------------------------------------------
# loading, processing, and inserting data into database
#------------------------------------------------

# get list of all data files (.RRF files) 
os.chdir(location_data)
files = [f for f in os.listdir(location_data) if 'RRF' in f]
os.chdir(location_code)

for file in files:
  
  # load
  os.chdir(location_data)
  data = pd.read_table(file,sep='|',nrows = to_load, header = None)
  os.chdir(location_code)
  print data.head()

  # process
  data = data.drop(data.columns[-1],axis=1) # drop last, empty column (parsing error)

  # create connection to db
  con = mdb.connect('localhost', 'root', '', 'RxNorm'); #host, user, password, #database
  tableName = file.split('.')[0]

  # insert data
  with con:
    cur = con.cursor()

    # drop table is it exists
    query = """DROP TABLE IF EXISTS %s;""" %tableName
    cur.execute(query)

    # create table - rethink the values for each column in database - use datetime and integers
    query = createTable(tableName)
    cur.execute(query)

    # insert values for every row in the file
    for i in range(0,len(data)):
      values = tuple(data.iloc[i])
      query = insertTable(tableName,values) # function definition to be written
      cur.execute(query)









