#------------------------------------------------
# Loads RxNorm .rrf files and inserts them into MySQL RxNorm DB
#------------------------------------------------

#------------------------------------------------
# modules
#------------------------------------------------

import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pymysql as mdb


#------------------------------------------------
# parameters
#------------------------------------------------
# to_load = 1000 # how many rows to load (for prototyping)

# path names
location_code = '/Users/Friederike/Dropbox/Insight/Project/FDA/code'
location_data = '/Users/Friederike/Dropbox/Insight/Project/FDA/data/RxNorm_full_08042014/rrf'
location_db = '/Users/Friederike/Dropbox/Insight/Project/FDA/data'

# user written module for specific task
os.chdir(location_code)
from umls_to_mysql_queries import createTable

#------------------------------------------------
# Change to where the db is
#------------------------------------------------
os.chdir(location_db)

#------------------------------------------------
# functions
#------------------------------------------------

# # determine "length" of VARCHAR - currently unused in script
# def MySqlTable_DataTypes(entries, name):
#   maxLen = 1
#   for i in entries:
#     if len(str(i))> maxLen:
#       maxLen = len(str(i))
#   maxLen += 0.2*maxLen
#   maxLen = int(maxLen)
#   return name + ' varchar('+str(maxLen)+')'

def getHeaders(filename):
  # headers taken from: http://www.nlm.nih.gov/research/umls/rxnorm/docs/2012/rxnorm_doco_full_2012-3.html
  if filename == 'RXNDOC.RRF':
    headers = ['key','value','type','expl','empty']
  elif filename == 'RXNCONSO.RRF':
    headers = ['rxcui','lat','ts','lui','stt','sui','ispref','rxaui','saui','scui','sdui','sab','tty','code','str','srl','suppress','cvf','empty']
  elif filename == 'RXNSAT.RRF':
    headers = ['rxcui','lui','sui','rxaui','stype','code','atui','satui','atn','sab','atv','suppress','cvf','empty']
  elif filename == 'RXNSTY.RRF':
    headers = ['rxcui','tui','stn','sty','atui','cvf','empty']
  elif filename == 'RXNREL.RRF':
    headers = ['rxcui1','rxaui1','stype1','rel','rxcui2','rxaui2','stype2','rela','rui','srui','sab','sl','dir','rg','suppress','cvf','empty']
  elif filename == 'RXNSAB.RRF':
    headers = ['vcui','rcui','vsab','rsab','son','sf','sver','vstart','vend','imeta','rmeta','slc','scc','srl','tfr','cfr','cxty','ttyl','atnl','lat','cenc','curver','sabin','ssn','scit','empty']
  elif filename == 'RXNCUI.RRF':
    headers = ['cui1','vsab_start','vsas_end','Cardinality','cui2','empty']
  else:
    headers = None

  return headers


def insertValues(filename, values,default):

   if filename == 'RXNCONSO.RRF':
    query = """INSERT INTO rxnconso VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "%s", '%s', '%s');""" %values
   elif filename == 'RXNREL.RRF':
      query = """INSERT INTO rxnrel VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');""" %values
   elif filename == "RXNSAB.RRF":
      query = """INSERT INTO rxnsab VALUES ('%s', '%s', '%s', '%s', '%s', '%s', "%s", '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');""" %values
   elif filename == "RXNSAT.RRF":
      query = """INSERT INTO rxnsat VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');""" %values
   elif filename == "RXNSTY.RRF":
      query = """INSERT INTO rxnsty VALUES ('%s', '%s', '%s', '%s', '%s');""" %values
   elif filename == "RXNDOC.RRF":
      if default:
        query = """INSERT INTO rxndoc VALUES ('%s', '%s', '%s');""" %values
      else:
        query = """INSERT INTO rxndoc VALUES ('%s', '%s', '%s', "%s");""" %values
   elif filename == "RXNCUI.RRF":
      query = """INSERT INTO rxncui VALUES ('%s', '%s', '%s', '%s', '%s');""" %values
   else:
      query = None

   return query

#------------------------------------------------
# loading, processing, and inserting data into database
#------------------------------------------------

if __name__ == "__main__":

  # list of all data files (.RRF files) to be included (add more later) 
  #files = ['RXNCONSO.RRF', 'RXNREL.RRF', 'RXNSAB.RRF', 'RXNSAT.RRF', 'RXNSTY.RRF', 'RXNDOC.RRF', 'RXNCUI.RRF']
  files = ['RXNREL.RRF', 'RXNSAB.RRF', 'RXNSAT.RRF', 'RXNSTY.RRF', 'RXNDOC.RRF', 'RXNCUI.RRF']

  for file in files:

    print "Processing file: " + file + "\n"

    # load files
    os.chdir(location_data)
    data = pd.read_table(file,sep='|', header = None, low_memory=False)
    os.chdir(location_db)

    # pre-process
    headers = getHeaders(file)
    data.columns = headers
    data = data.dropna(axis=1, how='all')
    headers = list(data.columns) # update (without empty columns)
    # run string processing replacing either " ' by the other to avoid problems with sql query

    # database
    tableName = file.split('.')[0].lower()    

    # create connection to db
    con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'RxNorm'); #host, user, password, #database

    # insert data
    with con:
      cur = con.cursor()

      # drop table is it exists
      query = """DROP TABLE IF EXISTS %s;""" %tableName
      cur.execute(query)

      # creat table
      query = createTable(file) # edit to take into account zero columns
      cur.execute(query)
      
      # insert the data
      for i in range(0,len(data)):
        values = tuple(data.iloc[i])
        print "Inserting entry: " + str(i) + " out of: " + str(len(data))+"\n"    
        try:
          query = insertValues(file, values, 1)
          cur.execute(query)
        except mdb.err.ProgrammingError:
          try:
            query = insertValues(file, values, -1)
            cur.execute(query)
          except mdb.err.ProgrammingError:
            print "Error with inserting values: " + str(values)
        con.commit()

  print "DONE \n\n"


# Processing file: RXNREL.RRF

# Inserting entry: 0 out of: 5156552

# Traceback (most recent call last):
#   File "umls_to_mysql.py", line 142, in <module>
#     query = insertValues(file, values, 1)
#   File "umls_to_mysql.py", line 76, in insertValues

#     query = """INSERT INTO rxnrel VALUES ('%s', '%s', '%s', '%s', '%s', '%s');""" %values




