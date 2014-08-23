#---------------------------
# modules
#---------------------------

import pandas as pd
from pandas import DataFrame
import os
import sys
import pymysql as mdb
import re

if __name__ == '__main__':

  #---------------------------
  # paths and data
  #---------------------------

  location_data = '/Users/Friederike/Dropbox/Insight/Project/FDA/data/aers'
  location_code = '/Users/Friederike/Dropbox/Insight/Project/FDA/code'

  os.chdir(location_data)
  aers = pd.read_csv('aers.csv', header = None)
  aers.columns = ['name','effect']
  aers['use'] = 0

  #---------------------------
  # pre-process (hack)
  #---------------------------

  aers.name = aers.name.apply(lambda x: x.replace('HCL','HYDROCHLORIDE'))
  aers.name = aers.name.apply(lambda x: x.replace("CHILDREN'S", '').strip())
  aers.name = aers.name.apply(lambda x: x.replace("JUNIOR STRENGTH", '').strip())
  aers.name = aers.name.apply(lambda x: re.split('\s\w\w(?!\w)', x)[0].strip()) # removes 2 letter add ons
  aers.name = aers.name.apply(lambda x: x.split(' TAB')[0])
  aers.name = aers.name.apply(lambda x: x.split(' (')[0])
  aers.name = aers.name.apply(lambda x: x.split(' PRESERVATIVE')[0])
  aers.name = aers.name.apply(lambda x: x.split(' EXTENDED')[0])
  aers.name = aers.name.apply(lambda x: x.split(' /')[0])
  aers.name = aers.name.apply(lambda x: x.split('/')[0])
  aers.name = aers.name.apply(lambda x: x.split(' - ')[0])
  aers.name = aers.name.apply(lambda x: x.split(' I.V.')[0])
  aers.name[aers.name == 'DOXAZOSIN MESLYLATE'] = 'DOXAZOSIN MESYLATE'
  aers.name[aers.name == 'GLUCOBAY'] = 'ACARBOSE'
  aers.name[aers.name == 'ILETIN'] = 'HUMULIN'
  aers.name[aers.name == 'NPH INSULIN'] = 'HUMULIN N'
  aers.name[aers.name == 'LIPIDIL'] = 'FENOFIBRATE'
  aers.name[aers.name == 'AMLODIPINE MALEATE'] = 'AMLODIPINE'
  aers.name = aers.name.apply(lambda x: re.split('\d+',x)[0].strip()) # removes any numbers

  def excludeEntries(entry):
    if ' AND ' in entry:
      exclude = 1
    elif ' W ' in entry:
      exclude = 1
    elif ' + ' in entry:
      exclude = 1
    elif ' W/' in entry:
      exclude = 1
    else:
      exclude = 0
    return exclude

  for name in aers.name:
    exclude = excludeEntries(name)
    aers.use[aers.name == name] = exclude

  aers = aers[aers.use == 0]

  #---------------------------
  # create tables
  #---------------------------

  con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'aers'); #host, user, password, #database

  with con:
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS names;")
    query = """CREATE TABLE names
    (
      _id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
      drug varchar(100),
      sideeffect varchar(100)
      );"""
    cur.execute(query)

    cur.execute("DROP TABLE IF EXISTS rxcuis;")
    query = """CREATE TABLE rxcuis
    (
      _id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
      rxcui varchar(100),
      sideeffect varchar(100)
      );"""
    cur.execute(query)

    cur.execute("DROP TABLE IF EXISTS link;")
    query = """CREATE TABLE link
    (
      _id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
      rxcui varchar(100),
      drug varchar(100)
      );"""
    cur.execute(query)
    con.commit()

  #---------------------------
  # populate table with drug names
  #---------------------------

  con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'aers'); #host, user, password, #database
  with con:
    cur = con.cursor()
    for name, effect in zip(aers.name.values, aers.effect.values):
      query = """INSERT INTO names(drug, sideeffect) VALUES("%s","%s");""" %(name,effect)
      cur.execute(query)
    con.commit()

  #---------------------------
  # populate table with rxcuis instead of names
  #---------------------------

  noRxNormEntry = []
  for name, effect in zip(aers.name.values, aers.effect.values):
    con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'rx_norm'); #host, user, password, #database
    with con:
      cur = con.cursor()
      query = """SELECT rxcui FROM rxnconso WHERE str = "%s" AND tty = 'IN' AND sab = 'RXNORM';""" %name 
      cur.execute(query)

      entries = []
      for entry in cur:
        entries.append(entry[0])

      # name classified as RxNorm ingredient and corresponding rxcui found
      if entries:
        con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'aers'); #host, user, password, #database
        with con:
          cur = con.cursor()
          for entry in unique(entries):
            print name + ": " + entry + ' for ' + effect
            query = """INSERT INTO rxcuis(rxcui,sideeffect) VALUES("%s","%s");""" %(entry,effect)
            cur.execute(query)
            query = """INSERT INTO link(rxcui,drug) VALUES("%s","%s");""" %(entry,name)
            cur.execute(query)

      #---------------------------
      # entry not a rxnorm ingredient
      #---------------------------

      else:

        con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'rx_norm'); #host, user, password, #database
        with con:
          cur = con.cursor()
          query = """SELECT rxcui, tty FROM rxnconso WHERE str = "%s" AND sab = 'RXNORM';""" %name 
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
              query = """SELECT rxcui, tty FROM rxnconso WHERE rxcui = "%s" AND tty = 'IN' AND sab = 'RXNORM';""" %entry[0] 
              cur.execute(query)
              rxcui = 'NULL'
              for e in cur:
                if e:
                  rxcui = e[0]
                break
              break
            
            con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'aers'); #host, user, password, #database
            with con:
              cur = con.cursor()
              print name + ": " + rxcui + ' for ' + effect
              query = """INSERT INTO rxcuis(rxcui,sideeffect) VALUES("%s","%s");""" %(rxcui,effect)
              cur.execute(query)
              query = """INSERT INTO link(rxcui,drug) VALUES("%s","%s");""" %(rxcui,name)
              cur.execute(query)

          else:
            noRxNormEntry.append(name)
            print name + ": no result"


              

