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
to_load = 1000 # how many rows to load (for prototyping)

# path names
location_code = '/Users/Friederike/Dropbox/Insight/Project/FDA/code'
location_data = '/Users/Friederike/Dropbox/Insight/Project/FDA/data/RxNorm_full_08042014/rrf'

#------------------------------------------------
# functions
#------------------------------------------------

# determine "length" of VARCHAR
def MySqlTable_DataTypes(entries, name):
  maxLen = 1
  for i in entries:
    if len(str(i))> maxLen:
      maxLen = len(str(i))
  maxLen += 0.2*maxLen
  maxLen = int(maxLen)
  return name + ' varchar('+str(maxLen)+')'

#------------------------------------------------
# loading, processing, and inserting data into database
#------------------------------------------------

# list of all data files (.RRF files) to be included (add more later) 
files = ['RXNDOC.RRF', 'RXNCONSO.RRF', 'RXNREL.RRF']

for file in files[1:2]:

  # headers taken from: http://www.nlm.nih.gov/research/umls/rxnorm/docs/2012/rxnorm_doco_full_2012-3.html
  # single letter for columns known to be empty
  if file == 'RXNDOC.RRF':
    headers = ['key','value','type','explanation','empty']
  elif file == 'RXNCONSO.RRF':
    headers = ['rxcui','language','a','b','c','d','e','rxaui','source_atom','source_concept','f','source_abbr','source_term_type','most_useful_id','string','g','suppress_view','view_flag','empty']
  elif file == 'RXNREL.RRF':
    headers = ['rxcui1','rxaui1','cui_aui1','rel1','rxcui2','rxaui2','cui_aui2','rel2','rui','source_rui','source_abbr','source_label','dir','rg','view_flag','empty']
  else:
    print "No appropriate headers found ..."
  
  # load
  os.chdir(location_data)
  data = pd.read_table(file,sep='|', nrows = to_load, header = None)
  os.chdir(location_code)

  # pre-process
  data.columns = headers
  data = data.dropna(axis=1)
  headers = list(data.columns) # update (without empty columns)

  # database
  tableName = file.split('.')[0].lower()    

  # create connection to db
  con = mdb.connect('localhost', 'root', '', 'RxNorm'); #host, user, password, #database

  # insert data
  with con:
    cur = con.cursor()

    # drop table is it exists
    query = """DROP TABLE IF EXISTS %s;""" %tableName
    cur.execute(query)

    # qTypes = '"""CREATE TABLE %s (' %tableName
    # for h in headers:
    #   valueType = MySqlTable_DataTypes(data[h], h)
    #   qTypes = qTypes + valueType
    #   if h != headers[-1]: 
    #     qTypes = qTypes+', '
    #   else:
    #     qTypes = qTypes+');"""'
    
    query = """CREATE TABLE rxnconso
    (rxcui varchar(8) NOT NULL, language varchar(3) NOT NULL, rxaui varchar(8) NOT NULL, source_abbr varchar(20) NOT NULL, source_term_type varchar(20) NOT NULL, most_useful_id varchar(50), string varchar(3000), suppress_view varchar(1));"""
    cur.execute(query)
    
    # insert values for every row in the file - not necessary to specify column names, so just use row by row value ...
    for i in range(0,len(data)):
      values = tuple(data.iloc[i])
      query = """INSERT INTO rxnconso VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');""" %values
      cur.execute(query)









