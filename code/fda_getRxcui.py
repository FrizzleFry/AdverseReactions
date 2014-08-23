import pandas as pd
from pandas import DataFrame
import os
import sys
import pymysql as mdb
import re
from numpy import isnan

#-------------------------------
# functions
#-------------------------------

def checkValidRxCui(drugID, cur):
  query = """SELECT rxcui, tty FROM rxnconso WHERE rxcui = "%s" AND tty = 'IN' AND sab = 'RXNORM';""" %drugID 
  cur.execute(query)
  rxcui = 'NULL'
  valid = None
  for e in cur:
    if e:
      rxcui = e[0]
  if rxcui != 'NULL':
    valid = 1
  return valid

# function to get rxcui in case provided name is an ingredient name - returns 'NULL' if not
def getRxCui(drugName, cur):
  query = """SELECT rxcui FROM rxnconso WHERE str = "%s" AND tty = 'IN' AND sab = 'RXNORM';""" %drugName
  cur.execute(query)
  rxcui = 'NULL'
  for e in cur:
    rxcui=e[0]
  return rxcui

# function to get rxcui in case provided name is an ingredient name - returns 'NULL' if not
def getRxCuiName(drugID, cur):
  query = """SELECT str FROM rxnconso WHERE rxcui = "%s" AND tty = 'IN' AND sab = 'RXNORM';""" %drugID
  cur.execute(query)
  name = []
  for e in cur:
    name.append(e[0])
  return name

# function to get rxcui if provided name is precise ingredient, brand name, or includes info on dosage - returns null if not
def getRxCuiRel1(drugName, cur):
  query = """SELECT rxcui, tty FROM rxnconso WHERE str = "%s" AND sab = 'RXNORM';""" %drugName 
  cur.execute(query)
  entries = {}
  for entry in cur:
    entries[entry[1]]=entry[0]

    if 'PIN' in entries:
      query = """SELECT rxcui2 FROM rxnrel WHERE rxcui1 = "%s" and rela = "has_form";""" %entries['PIN']
    elif 'SCDC' in entries:
      query = """SELECT rxcui2 FROM rxnrel WHERE rxcui1 = "%s" and rela = "ingredient_of";""" %entries['SCDC']
    elif 'BN' in entries:
      query = """SELECT rxcui2 FROM rxnrel WHERE rxcui1 = "%s" and rela = "has_tradename";""" %entries['BN']
    elif 'SCDF' in entries:
      query = """SELECT rxcui2 FROM rxnrel WHERE rxcui1 = "%s" and rela = "ingredient_of";""" %entries['BN']
    else:
      query = None

    if query:
      cur.execute(query)
      entries = []
      for entry in cur:
        entries.append(entry[0])
      for entry in entries:
        query = """SELECT rxcui FROM rxnconso WHERE rxcui = "%s" AND tty = 'IN' AND sab = 'RXNORM';""" %entry 
        cur.execute(query)
        rxcui = 'NULL'
        for e in cur:
          if e:
            rxcui = e[0]
            return rxcui
    else:
      return 'NULL'

if __name__ == '__main__':


  #--------------------------------------------
  # parameters
  #--------------------------------------------

  restart = 1
  loadDataFromFile = 1

  #--------------------------------------------
  # extract and all medicinal products
  #--------------------------------------------

  if restart == 0:

    con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'fda_data'); #host, user, password, #database
    with con:
      cur = con.cursor()
      query = """SELECT DISTINCT medicinalproduct FROM drug_report;"""
      cur.execute(query)

      drugs = []
      for drug in cur:
        drugs.append(drug[0])

    #--------------------------------------------
    # construct dataframe
    #--------------------------------------------

    d = {}
    d['medicinalproduct'] = drugs
    df = DataFrame(d)
    df['medprod_corrected'] = None
    df['rxcui'] = None
    df2 = DataFrame(df['medicinalproduct'])

    #--------------------------------------------
    # pre-process (clean)
    #--------------------------------------------

    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.upper())
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split('(')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.replace('HCL','HYDROCHLORIDE'))
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.replace("CHILDREN'S", '').strip())
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.replace("JUNIOR'S", '').strip())
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(" INJ")[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(" INJECTION")[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: re.split('-\s?', x)[0].strip())
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.replace("*", '').strip())
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: re.split('\s\w\w(?!\w)', x)[0].strip()) # removes 2 letter add ons
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' TAB')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' ORAL')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' PRESERVATIVE')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(" '")[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' GRANULES')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' CAP')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' WITHOUT')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' EYE DROPS')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(', ')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' EXTENDED')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' UNKNOWN')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' INSULIN')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' /')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split('/')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' -')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' +')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' AND')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' WITH')[0])
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: x.split(' I.V.')[0])
    df2.medicinalproduct[df2.medicinalproduct == 'GLUCOBAY'] = 'ACARBOSE'
    df2.medicinalproduct[df2.medicinalproduct == 'ILETIN'] = 'HUMULIN'
    df2.medicinalproduct[df2.medicinalproduct == 'NPH INSULIN'] = 'HUMULIN N'
    df2.medicinalproduct[df2.medicinalproduct == 'LIPIDIL'] = 'FENOFIBRATE'
    df2.medicinalproduct[df2.medicinalproduct == 'AMLODIPINE MALEATE'] = 'AMLODIPINE'
    df2.medicinalproduct = df2.medicinalproduct.apply(lambda x: re.split('\d+',x)[0].strip()) # removes any numbers
    df['medprod_corrected'] = df2['medicinalproduct']

    #--------------------------------------------
    # Look up rxcui
    #--------------------------------------------

    drugNames = list(df.medprod_corrected.unique())

  else:
    print "Restarting ... \n\n"
    data = pd.read_csv('drug_rxcui.csv')
    df = data[isnan(data.rxcui)]
    drugNames = list(df.medprod_corrected.unique())
    df = data
  
  for drugName in drugNames:
    con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'rx_norm'); #host, user, password, #database
    with con:
      cur = con.cursor()
      try:
        rxcui = getRxCui(drugName, cur)
        if rxcui == 'NULL':
          rxcui = getRxCuiRel1(drugName, cur)
          valid = checkValidRxCui(rxcui, cur)
          if not valid:
            rxcui = "NULL"
          if not rxcui:
            rxcui = "NULL"
      except KeyboardInterrupt:
        raise
      except:
        try:
          print "Failed to look up rxcui for: " + drugName + "\n"
        except TypeError:
          print "No entry"
        rxcui = 'NULL'
      
      try:
        print "Entry: " + drugName + " rxcui: " + rxcui
      except TypeError:
        print "No entry"
      df.rxcui[df.medprod_corrected == drugName] = rxcui
      df.to_csv('drug_rxcui.csv') # save file in case of crash 
  
  #--------------------------------------------
  # Insert into DB
  #--------------------------------------------

  if loadDataFromFile == 1:
      data = pd.read_csv('drug_rxcui.csv')
      data[data['rxcui'] == -99] = 'NULL' 
      df = data

  con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'fda_data'); #host, user, password, #database
  with con:
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS drug_rxcui;")
    query = """CREATE TABLE drug_rxcui 
    (medicinalproduct VARCHAR(500),
     medprod_corrected VARCHAR (500),
     rxcui VARCHAR(50))"""
    cur.execute(query)

    for i in range(0,len(df)):
      values = tuple(df.iloc[i])
      try:
        print "Inserting entry " + str(i) + " out of " + str(len(df)) + " " + str(values) + "\n"
        query = """INSERT INTO drug_rxcui(medicinalproduct, medprod_corrected, rxcui)
        VALUES ("%s", "%s", "%s");""" %values
        cur.execute(query)
      except KeyboardInterrupt:
        raise
      except:
        try:
          print 'Failed to include in drug_rxcui table: ' + str(values)
        except TypeError:
          print "No entry"

      con.commit()
  con.close()

  print "SUCCESS \n\n"

if loadDataFromFile == 1:
    data = pd.read_csv('drug_rxcui.csv')
    data[data['rxcui'] == -99] = 'NULL' 
    df = data

con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'fda_data'); #host, user, password, #database
with con:
  cur = con.cursor()
  # query = """ALTER TABLE drug_report ADD COLUMN rxcui INT DEFAULT NULL;"""
  # cur.execute(query)
  for i in range(21733,len(df)):
    d = df.iloc[i]
    if d.rxcui == 'NULL' or isnan(d.rxcui):
      d.rxcui = 0
    rxcui = int(d.rxcui)
    try:
      print "Inserting: " + d.medicinalproduct + " as " + str(rxcui)
      query = """UPDATE drug_report AS dr 
      SET dr.rxcui = '%s'
      WHERE medicinalproduct = "%s";""" %(rxcui, d.medicinalproduct)
      cur.execute(query)
    except KeyboardInterrupt:
      raise
    except:
      print "Failed to insert " + str(d.medicinalproduct) + " as " + str(rxcui)
    con.commit()
























