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
  data = pd.read_table(file,sep='|',nrows = to_load, header = None)
  os.chdir(location_code)

  # pre-process
  data.columns = headers
  data = data.dropna(axis=1)
  headers = list(headers) # update (without empty columns)

  # create connection to db
  con = mdb.connect('localhost', 'root', '', 'RxNorm'); #host, user, password, #database
  tableName = file.split('.')[0].lower()

  # insert data
  with con:
    cur = con.cursor()

    # drop table is it exists
    query = """DROP TABLE IF EXISTS %s;""" %tableName
    cur.execute(query)

    # use headers, determine value type from file or manually ...
    # one could write a script that checks for value type per column starting from smaller storage to larger
    # this script I could reuse when later building other databases ...
    # then construct the query string ...
    if file == 'RXNDOC.RRF':
      value_types = ['INT']
    elif file == 'RXNCONSO.RRF':
      value_types = []
    elif file == 'RXNREL.RRF':
      value_types = []
    else:
      print "No appropriate value types found ..."

    # create table - rethink the values for each column in database - use datetime and integers
    query = createTable(tableName)
    cur.execute(query)
    
    # insert values for every row in the file - not necessary to specify column names, so just use row by row value ...
    for i in range(0,len(data)):
      values = tuple(data.iloc[i])
      query = insertTable(tableName,values) # function definition to be written
      cur.execute(query)









