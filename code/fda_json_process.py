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

#--------------------------------------------
# parameters
#--------------------------------------------

# path names
location_code = '/Users/Friederike/Dropbox/Insight/Project/FDA/code'
location_data = '/Users/Friederike/Dropbox/Insight/Project/FDA/data/FDA_jsons'

#--------------------------------------------
# functions
#--------------------------------------------

def determineAgeInYears(dp):
  try:
    age_unit = dp['patientonsetageunit']
    if age_unit == '801':
      return dp['patientonsetage']
    elif age_unit =='800':
      return str(int(dp['patientonsetage'])*10)
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

def jsonToDict(data,fieldname,table,tablename):
  try:
    table[tablename] = data[fieldname]
  except KeyError:
    table[tablename] = 'NULL'
  return table

#--------------------------------------------
# main
#--------------------------------------------

if __name__ == "__main__":


  # create connection to db
  con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'fda'); #host, user, password, #database

  with con:
    cur = con.cursor()

    # drop all tables if exist
    cur.execute("""DROP TABLE IF EXISTS main;""")
    cur.execute("""DROP TABLE IF EXISTS patient;""")
    cur.execute("""DROP TABLE IF EXISTS sources;""")
    cur.execute("""DROP TABLE IF EXISTS drugs;""")
    cur.execute("""DROP TABLE IF EXISTS serious;""")
    cur.execute("""DROP TABLE IF EXISTS drugs_report;""")
    cur.execute("""DROP TABLE IF EXISTS drug_patient;""")

    # create all tables
    query = """CREATE TABLE main 
    (id int(10),
    reportid int(10),
    duplicate int(10),
    receivedate varchar(10),
    receiptdate varchar(10),
    serious int,
    transmissiondate varchar(10),
    occurcountry varchar(10),
    primarysourcecountry varchar(10),
    qualification varchar(5),
    reportercountry varchar(10),
    sendertype varchar(5));"""
    cur.execute(query)

    #--------------------------------------------
    # load json file
    #--------------------------------------------
    os.chdir(location_data)
    files = [f for f in os.listdir(location_data) if 'json' in f]

    for file in files:

      # read data into memory one file at a time
      os.chdir(location_data)
      f = open(file, mode = 'r')
      data = json.loads(f.read())
      f.close()
      os.chdir(location_code)

      main_table      = {} # done
      patient_table   = {} # done
      sources_table   = {} # done
      drugs_table     = {} # to do
      serious_table   = {} # done
      drugs_report_link_table = {} # all report id and drug combinations TO DO
      drug_patient_table = {} # done
      discarded       = 0
      included        = 0

      # include primary key with autoincrement to prevent overwriting ...
      
      # follow entries in https://open.fda.gov/drug/event/reference/
      for i, d in enumerate(data):

        # open FDA only returns the latest report of duplicates: no check necessary on d['safetyreportversion'] even if d['duplicate'] == 1
        try:
          main_table['safetyreportid'] = d['safetyreportid']                                          # only retain unique reports - address later
          
          # insert values if exist in data
          main_table = jsonToDict(d,'duplicate',main_table,'duplicate') # int
          main_table['duplicate'] = int(main_table['duplicate'])
          main_table = jsonToDict(d,'receivedate',main_table,'receivedate') # yyyymmdd
          main_table = jsonToDict(d,'receiptdate',main_table,'receiptdate') # yyyymmdd
          main_table = jsonToDict(d,'serious',main_table,'serious')
          main_table['serious'] = int(main_table['serious'])
          main_table = jsonToDict(d,'transmissiondate',main_table,'transmissiondate') #yyyymmdd
          main_table = jsonToDict(d,'occurcountry',main_table,'occurcountry')
          main_table = jsonToDict(d,'primarysourcecountry',main_table,'primarysourcecountry')
          main_table = jsonToDict(d['primarysource'],'qualification',main_table,'qualification')      # fails if no primarysource key - address later
          main_table = jsonToDict(d['primarysource'],'reportercountry',main_table,'reportercountry')  # fails if no primarysource key - address later
          main_table = jsonToDict(d['sender'],'sendertype',main_table,'sendertype')                   # fails if no sender key - address later
          values = tuple(main_table.values())
          query = """INSERT INTO main VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');""" %values
          cur.execute(query)

          # source table
          sources_table['safetyreportid'] = d['safetyreportid']
          sources_table = jsonToDict(d,'companynum',sources_table,'companynum')
          if d['reportduplicate']:
            sources_table = jsonToDict(d['reportduplicate'],'duplicatenumb',sources_table,'duplicatenumb')
            sources_table = jsonToDict(d['reportduplicate'],'duplicatesource',sources_table,'duplicatesource')
          else:
            sources_table['duplicatenumb'] = 'NULL'
            sources_table['duplicatesource'] = 'NULL'
          values = tuple(sources_table.values())
          query_insert = """INSERT INTO sources_table VALUES('%s', '%s', '%s', '%s');""" %values

          # serious table
          try:
            if d['serious'] == '1':
              serious_table['safetyreportid'] = d['safetyreportid'] 
              serious_table = jsonToDict(d,'seriousnesscongenitalanomali',serious_table,'congenitalanomali')
              serious_table = jsonToDict(d,'seriousnessdeath',serious_table,'death')
              serious_table = jsonToDict(d,'seriousnessdisabling',serious_table,'disabling')
              serious_table = jsonToDict(d,'seriousnesshospitalization',serious_table,'hospitalization')
              serious_table = jsonToDict(d,'seriousnesslifethreatening',serious_table,'lifethreatening')
              serious_table = jsonToDict(d,'seriousnessother',serious_table,'other')
              values = tuple(serious_table.values())
              query_insert = """INSERT INTO serious_table VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s');""" %values
            else:
              pass
          except KeyError:
            pass
              
          # patient table
          try:
            dp = d['patient']
            patient_table['safetyreportid'] = d['safetyreportid']
            patient_table['onsetage'] = determineAgeInYears(dp)
            patient_table = jsonToDict(dp,'patientsex',serious_table,'sex')
            patient_table = jsonToDict(dp,'patientweight',serious_table,'weight')
            patient_table['numberdrugstaken'] = len(d['patient']['drug'])
            try:
              if dp['patientdeath']:
                patient_table['death'] = '1'
              else:
                patient_table['death'] = '0'
            except KeyError:
                patient_table['death'] = '0'

            query_insert = """INSERT INTO patient_table VALUES('%s', '%s', '%s', '%s', '%s', '%s');""" %values
          except:
            print "No patient information for report: " + d['safetyreportid'] + "\n" # do not include in db

          # drugs report links table # insert into SQL on row at a time in inner loop with distinct, autoincrement primary key
          for i2, drug in enumerate(d['patient']['drug']):  
            drug_patient_table['safetyreportid'] = d['safetyreportid']        
            drug_patient_table = jsonToDict(drug,'actiondrug',drug_patient_table,'actionsdrug')
            drug_patient_table = jsonToDict(drug,'drugadditional',drug_patient_table,'drugadditional')
            # drug_patient_table = jsonToDict(drug,'actiondrug',drug_patient_table,'actionsdrug') # dosage - build additional function
            drug_patient_table = jsonToDict(drug,'drugdosageform',drug_patient_table,'dosageform')
            # drug_patient_table = jsonToDict(drug,'drugintervaldosageunitnumb',drug_patient_table,'actionsdrug') # - build helper function
            drug_patient_table = jsonToDict(drug,'drugrecurrentadminitration',drug_patient_table,'recurrentadministration')
            # drug_patient_table = jsonToDict(drug,'drugseparatedosagenumb',drug_patient_table,'separatedosageumb') # - build helper
            drug_patient_table = jsonToDict(drug,'drugadministrationroute',drug_patient_table,'administrationroute') # make table that maps this number to explanation - later
            drug_patient_table = jsonToDict(drug,'drugbatchnumb',drug_patient_table,'batchnumb')
            drug_patient_table = jsonToDict(drug,'drugcharacterization',drug_patient_table,'drugcharacterization')
            drug_patient_table = jsonToDict(drug,'drugdosagetext',drug_patient_table,'drugdosagetext')
            drug_patient_table = jsonToDict(drug,'drugenddate',drug_patient_table,'drugenddate')
            drug_patient_table = jsonToDict(drug,'drugindication',drug_patient_table,'drugindication')
            drug_patient_table = jsonToDict(drug,'drugstartdate',drug_patient_table,'drugstartdate')
            # drug_patient_table = jsonToDict(drug,'drugtreatmentduration',drug_patient_table,'drugtreatmentduration') # - build helper function
            drug_patient_table = jsonToDict(drug,'medicinalproduct',drug_patient_table,'medicinalproduct')


          # drugs table - this will include all identifiers, what drug typically treats - insert one at a time
          for i2, drug in enumerate(d['patient']['drug']):          
            drugs_table

          # report id, rxcui, openfda - populated by data & lateron RxNorm and drugs_table
          for i2, drug in enumerate(d['patient']['drug']):          
            drugs_report_link_table



          included += 1
        except KeyError:
          discarded += 1
          print 'Reports lacks ID: discard \n'
          # in current sample 4.6% of all data - not too bad. Low chance of inducing bias.














      # in report duplicate, it lists the source of the duplicate, which one may want to take into account in certain analyses (what's the source)

      # one table for header, make separate for all reports that have serious == 1 with reason for this evaluation

      # make one table for duplicates where report id shows up multiple times

      # sender and receiver field can be removed

      # one table for patient data linked to report id
      # patient age: check format age before entering

      # one that links report to drug

      # table for drugs: all details that are not patient dependend (e.g. cumulative dose should be w patient info)

      # 

    
