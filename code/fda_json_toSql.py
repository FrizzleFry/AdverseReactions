#--------------------------------------------
# README: Parses the JSON files obtained from FDA API
# https://open.fda.gov/drug/event/reference/
#--------------------------------------------

#--------------------------------------------
# modules
#--------------------------------------------
import json
import pandas as pd
import os
import sys
import argparse
import pymysql as mdb
from pandas.io import sql
from pandas import DataFrame
import unicodedata

#--------------------------------------------
# parameters
#--------------------------------------------

# path names
location_code = '/Users/Friederike/Dropbox/Insight/Project/FDA/code'
location_data = "/Volumes/Friede\'s storage/Insight/Project/data/fda/openFDAfull"

#--------------------------------------------
# functions
#--------------------------------------------

def determineAgeInYears(dp):
  try:
    age_unit = dp['patientonsetageunit']
    if age_unit == '801':
      return dp['patientonsetage']
    elif age_unit =='800':
      return str(float(dp['patientonsetage'])*10)
    elif age_unit =='802':
      return str(float(dp['patientonsetage'])/12)
    elif age_unit =='803':
      return str(float(dp['patientonsetage'])/52)
    elif age_unit =='804':
      return str(float(dp['patientonsetage'])/365)
    elif age_unit =='805':
      return str(float(dp['patientonsetage'])/8766)
    else:
      return 'NULL'
  except KeyError:
    return 'NULL'

def checkExistEntry(data,fieldname):
  try:
    result = data[fieldname]
  except KeyError:
    result = 'NULL'
  return result

def transformDate(entry):
  entry = list(entry)
  entry.insert(4,'-')
  entry.insert(7,'-')
  return ''.join(entry)

def insertDate(data,fieldname,table):
  result = checkExistEntry(data,fieldname)
  if result:
    result = transformDate(result)
  table[fieldname].append(result)
  return table

def replaceDoubleQuotesDataframe(df):
  for h in list(df.columns):
    try:
      df[h] = df[h].apply(lambda x: x.replace('"',"'"))
    except AttributeError:
      df[h] = df[h]
  return df

def cleanMedicinalProductDataframe(df):
  if 'medicinalproduct' in list(df.columns):
    try:
      df['medicinalproduct'] = df['medicinalproduct'] .apply(lambda x: x.replace('\\', ' '))
      return df
    except AttributeError:
      return df
  else:
    return df

def ensureUnicode(entry):
  entry = unicodedata.normalize('NFKD', entry).encode('ascii','ignore')
  return entry

def ensureUnicodeDataFrame(df):
  for h in list(df.columns):
    try:
      df[h] = df[h].apply(ensureUnicode) 
    except TypeError:
      pass
    return df

#--------------------------------------------
# main script
#--------------------------------------------

if __name__ == "__main__":

  os.chdir(location_code)
  f = open('dataEntry_log.log','r')
  status = f.read()
  f.close()

  if status == '':
    # create tables
    con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'fda_data'); #host, user, password, #database

    with con:
      cur = con.cursor()

      cur.execute("DROP TABLE IF EXISTS main;")
      query = """CREATE TABLE main
      ( 
        safetyreportid varchar(20),
        receivedate date,
        transmissiondate date,
        receiptdate date,
        serious varchar(5),
        qualification varchar(5),
        reportercountry varchar(1000),
        sendertype varchar(5) ); """
      cur.execute(query)

      cur.execute("DROP TABLE IF EXISTS serious;")
      query = """CREATE TABLE serious
      ( 
        safetyreportid varchar(20),
        congenitalanomali varchar(5),
        death varchar(5),
        disabling varchar(5),
        hospitalization varchar(5),
        lifethreatening varchar(5),
        other varchar(5) ); """
      cur.execute(query)

      cur.execute("DROP TABLE IF EXISTS patient;")
      query = """CREATE TABLE patient
      ( 
        safetyreportid varchar(20),
        age varchar(100),
        sex varchar(5),
        weight varchar(100),
        number_drugs varchar(100),
        death varchar(5)
        );"""
      cur.execute(query)

      cur.execute("DROP TABLE IF EXISTS drug_patient;")
      query = """ CREATE TABLE drug_patient
      (
        safetyreportid varchar(20),
        actiondrug varchar(5),
        drugdosageform varchar(100),
        drugadministrationroute varchar(100),
        drugbatchnumb varchar(100),
        drugcharacterization varchar(100),
        drugindication varchar(100),
        medicinalproduct varchar(500)
        );"""
      cur.execute(query)

      cur.execute("DROP TABLE IF EXISTS adverse_effect;")
      query = """ CREATE TABLE adverse_effect
      (
        safetyreportid varchar(20),
        reaction varchar(500),
        outcome varchar(5)
        );"""
      cur.execute(query)

      cur.execute("DROP TABLE IF EXISTS drug_report;")
      query = """ CREATE TABLE drug_report
      (
        safetyreportid varchar(20),
        medicinalproduct varchar(500)
        );"""
      cur.execute(query)
    con.close()
  else:
    status = status.split(',')
    status = [s.strip() for s in status]

  #--------------------------------------------
  # load json file
  #--------------------------------------------
  
  os.chdir(location_data)
  allFiles = [f for f in os.listdir(location_data) if 'json' in f]
  files = []
  for file in allFiles:
    if file in status:
      pass
    else:
      files.append(file)

  for file in files:

    print 'Processing file: ' + file

    os.chdir(location_data)
    f = open(file, mode = 'r')
    data = json.loads(f.read())
    f.close()
    os.chdir(location_code)
    data = data['results']

    # parameters to keep track of discarded, etc. reports
    included    = 0
    discarded   = 0   # error during report handling
    excluded    = 0   # does not contain essential fields or more than 2 drugs
    notindb     = 0   # counts records failed to insert into DB (happens for unicode encoding errors)

    #--------------------------------------------
    # make all dics for data
    #--------------------------------------------

    main_table      = {} 
    main_table['safetyreportid'] = []
    main_table['receivedate'] = []
    main_table['receiptdate'] = []
    main_table['serious'] = []
    main_table['transmissiondate'] = []
    main_table['qualification'] = []
    main_table['reportercountry'] = []
    main_table['sendertype'] = []

    serious       = {}
    serious['safetyreportid'] = []
    serious['congenitalanomali'] = []
    serious['death'] = []
    serious['disabling'] = []
    serious['hospitalization'] = []
    serious['lifethreatening'] = []
    serious['other'] = []

    patient = {}
    patient['safetyreportid'] = []
    patient['age'] = []
    patient['sex'] = []
    patient['weight'] = []
    patient['number_drugs'] = []
    patient['death'] = []

    drug_patient = {}
    drug_patient['safetyreportid']  = []
    drug_patient['actiondrug']      = []
    drug_patient['drugdosageform']  = []
    drug_patient['drugadministrationroute'] = []
    drug_patient['drugbatchnumb']   = []
    drug_patient['drugcharacterization'] = []
    drug_patient['drugindication']  = []
    drug_patient['medicinalproduct'] = []

    adverse_effect = {}
    adverse_effect['safetyreportid'] = []
    adverse_effect['reaction'] = []
    adverse_effect['outcome'] = []

    drug_report = {}
    drug_report['safetyreportid'] = []
    drug_report['medicinalproduct'] = []
  
    #--------------------------------------------
    # processing report by report
    #--------------------------------------------
    for i, d in enumerate(data):

      # exclude reports that have no safetyreportid, list no drugs, list no adverse reaction
      try: 
        safetyreportid = d['safetyreportid']
        drugs = d['patient']['drug']
        drug = d['patient']['drug'][0]['medicinalproduct']
        reaction = d['patient']['reaction'][0]['reactionmeddrapt']
        include = 1
      except KeyError:
        include = 0

      # exclude reports that have more than 2 drugs
      if len(d['patient']['drug']) > 2:
        include = 0

      if include == 1:

        try:
          #--------------------------------------------
          # main table
          #--------------------------------------------

          main_table['safetyreportid'].append(d['safetyreportid'])
          main_table = insertDate(d,'receivedate',main_table)
          main_table = insertDate(d,'transmissiondate',main_table)
          main_table = insertDate(d,'receiptdate',main_table)
          main_table['serious'].append(checkExistEntry(d,'serious'))
          try:
            main_table['qualification'].append(checkExistEntry(d['primarysource'],'qualification'))
            main_table['reportercountry'].append(checkExistEntry(d['primarysource'],'reportercountry'))
          except TypeError:
            main_table['qualification'].append('NULL')
            main_table['reportercountry'].append('NULL')
          try:      
            main_table['sendertype'].append(checkExistEntry(d['sender'],'sendertype'))
          except TypeError:
            main_table['sendertype'].append('NULL')

          #--------------------------------------------
          # serious table
          #--------------------------------------------
          
          if d['serious'] == '1':
            serious['safetyreportid'].append(d['safetyreportid'])
            serious['congenitalanomali'].append(checkExistEntry(d,'seriousnesscongenitalanomali'))
            serious['death'].append(checkExistEntry(d,'seriousnessdeath'))
            serious['disabling'].append(checkExistEntry(d,'seriousnessdisabling'))
            serious['hospitalization'].append(checkExistEntry(d,'seriousnesshospitalization'))
            serious['lifethreatening'].append(checkExistEntry(d,'seriousnesslifethreatening'))
            serious['other'].append(checkExistEntry(d,'seriousnessother'))

          #--------------------------------------------
          # patient table
          #--------------------------------------------

          patient['safetyreportid'].append(d['safetyreportid'])
          age = checkExistEntry(d['patient'],'onsetage')
          if age:
            age = determineAgeInYears(d['patient'])
          else:
            age = 'NULL'
          patient['age'].append(age)
          patient['sex'].append(checkExistEntry(d['patient'],'patientsex'))
          patient['weight'].append(checkExistEntry(d['patient'],'patientweight'))
          patient['number_drugs'].append(len(d['patient']['drug']))
          try:
            if d['patient']['patientdeath']:
              patient['death'].append('1')
            else:
              patient['death'].append('0')
          except KeyError:
            patient['death'].append('0')

          #--------------------------------------------
          # drug patient table
          #--------------------------------------------

          for drug in d['patient']['drug']:
            drug_patient['safetyreportid'].append(d['safetyreportid'])
            drug_patient['actiondrug'].append(checkExistEntry(drug, 'actiondrug'))
            drug_patient['drugdosageform'].append(checkExistEntry(drug, 'drugdosageform'))        
            drug_patient['drugadministrationroute'].append(checkExistEntry(drug, 'drugadministrationroute'))
            drug_patient['drugbatchnumb'].append(checkExistEntry(drug, 'drugbatchnumb'))
            drug_patient['drugcharacterization'].append(checkExistEntry(drug, 'drugcharacterization'))
            drug_patient['drugindication'].append(checkExistEntry(drug, 'drugindication'))
            drug_patient['medicinalproduct'].append(checkExistEntry(drug, 'medicinalproduct'))

          #--------------------------------------------
          # adverse_effect table
          #--------------------------------------------

          for r in d['patient']['reaction']:
            adverse_effect['safetyreportid'].append(d['safetyreportid'])
            adverse_effect['reaction'].append(checkExistEntry(r,'reactionmeddrapt'))
            adverse_effect['outcome'].append(checkExistEntry(r,'reactionoutcome'))

          #--------------------------------------------
          # drug report table
          #--------------------------------------------

          for drug in d['patient']['drug']:
            drug_report['safetyreportid'].append(d['safetyreportid'])
            drug_report['medicinalproduct'].append(checkExistEntry(drug,'medicinalproduct'))

          included += 1

        # catch all errors and continue except for KeyboardInterrupt 
        except KeyboardInterrupt:
            raise
        except:
          discarded += 1
      else:
        excluded += 1

    # write to log file - keep track of processes JSONS
    os.chdir(location_code)
    f = open('dataEntry_log.log','a')
    f.write(file+', ')
    f.close()

    # keep track of how many reports included / excluded per report
    os.chdir(location_code)
    f = open('dataLog.log','a')
    f.write(file+','+str(included)+','+str(discarded)+','+str(excluded)+','+ str(len(data))+'\n')
    f.close()

    #--------------------------------------------
    # convert to dataframe
    #--------------------------------------------

    main = DataFrame.from_dict(main_table)
    serious = DataFrame.from_dict(serious)
    patient  = DataFrame.from_dict(patient)
    drug_patient = DataFrame.from_dict(drug_patient)
    adverse_effect = DataFrame.from_dict(adverse_effect)
    drug_report = DataFrame.from_dict(drug_report)

    #--------------------------------------------
    # pre-process before entry into DB
    #--------------------------------------------

    main = replaceDoubleQuotesDataframe(main)
    serious = replaceDoubleQuotesDataframe(serious)
    patient = replaceDoubleQuotesDataframe(patient)
    drug_patient = replaceDoubleQuotesDataframe(drug_patient)
    adverse_effect = replaceDoubleQuotesDataframe(adverse_effect)
    drug_report = replaceDoubleQuotesDataframe(drug_report)

    main = cleanMedicinalProductDataframe(main)
    serious = cleanMedicinalProductDataframe(serious)
    patient = cleanMedicinalProductDataframe(patient)
    drug_patient = cleanMedicinalProductDataframe(drug_patient)
    adverse_effect = cleanMedicinalProductDataframe(adverse_effect)
    drug_report = cleanMedicinalProductDataframe(drug_report)

    main = ensureUnicodeDataFrame(main)
    serious = ensureUnicodeDataFrame(serious)
    patient = ensureUnicodeDataFrame(patient)
    drug_patient = ensureUnicodeDataFrame(drug_patient)
    adverse_effect = ensureUnicodeDataFrame(adverse_effect)
    drug_report = ensureUnicodeDataFrame(drug_report)

    #--------------------------------------------
    # insert into DB
    #--------------------------------------------

    con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'fda_data'); #host, user, password, #database
    with con:
      cur = con.cursor()

      for i in range(0,len(main)):
        values = tuple(main.iloc[i])
        try:
          query = """INSERT INTO main(qualification, receiptdate, receivedate, reportercountry, safetyreportid, sendertype, serious, transmissiondate)
          VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s");""" %values
          cur.execute(query)
        except KeyboardInterrupt:
          raise
        except:
            print 'Failed to include in main table: ' + str(values)
      
      for i in range(0,len(serious)):
        values = tuple(serious.iloc[i])
        try:
          query = """INSERT INTO serious(congenitalanomali, death, disabling, hospitalization, lifethreatening, other, safetyreportid)
          VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s");""" %values
          cur.execute(query)
        except KeyboardInterrupt:
          raise
        except:
            print 'Failed to include in serious table: ' + str(values)

      for i in range(0,len(patient)):
        values = tuple(patient.iloc[i])
        try:
          query = """INSERT INTO patient(age, death, number_drugs, safetyreportid, sex, weight)
          VALUES ("%s", "%s", "%s", "%s", "%s", "%s");""" %values
          cur.execute(query)
        except KeyboardInterrupt:
          raise
        except:
            print 'Failed to include in patient table: ' + str(values)

      for i in range(0,len(drug_patient)):
        values = tuple(drug_patient.iloc[i])
        try:
          query = """INSERT INTO drug_patient(actiondrug, drugadministrationroute, drugbatchnumb, drugcharacterization, drugdosageform, drugindication, medicinalproduct, safetyreportid)
          VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s");""" %values
          cur.execute(query)
        except KeyboardInterrupt:
          raise
        except:
            print 'Failed to include in drug_patient table: ' + str(values)

      for i in range(0,len(adverse_effect)):
        values = tuple(adverse_effect.iloc[i])
        try:
          query = """INSERT INTO adverse_effect(outcome, reaction, safetyreportid)
          VALUES ("%s", "%s", "%s");""" %values
          cur.execute(query)
        except KeyboardInterrupt:
          raise
        except:
            print 'Failed to include in adverse_effect table: ' + str(values)

      for i in range(0,len(drug_report)):
        values = tuple(drug_report.iloc[i])
        try:
          query = """INSERT INTO drug_report(medicinalproduct, safetyreportid)
          VALUES ("%s", "%s");""" %values
          cur.execute(query)
        except KeyboardInterrupt:
          raise
        except:
            print 'Failed to include in drug_report table: ' + str(values)
      
      con.commit()
    con.close()









    