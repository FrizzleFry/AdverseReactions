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
import scipy  
import scikits.bootstrap as bootstrap 
import random
from ggplot import *

def computeConfidenceInterval(nrSamples,data):
  dsData = []
  for i in range(0,nrSamples):
    include=random.randint(0,len(data))
    dsData.append(data[include])
  CIs = bootstrap.ci(dsData, statfunction=scipy.mean)
  return CIs

def iterateCursor(cur):
  d = []
  for e in cur:
    d.append(int(e[0]))
  return d

def iterateCursorCounts(cur):
  d = {}
  for entry in cur:
    if entry[0] =='1':
      d['male'] = int(entry[1])
    elif entry[0] == '2':
      d['female'] = int(entry[1])
  d['propFem'] = float(d['female']) / float(d['female'] + d['male'])
  return d


#------------------------------------------------
# Exploring Gender
#------------------------------------------------
Gender = []
con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'fda_data'); #host, user, password, #database
with con:
  cur = con.cursor()

  #------------------------------------------------
  # for all
  #------------------------------------------------
  print "Look at all data."
  cur.execute("select sex, count(sex) from patient where sex in (1,2) group by sex")
  d = iterateCursorCounts(cur)

  data = []
  cur.execute("select sex from patient where sex in (1,2);")
  data = iterateCursor(cur)
  CIs = computeConfidenceInterval(10000,data)
  d['CI'] = CIs-1
  d['name'] = 'all'
  Gender.append(d)
  
  #------------------------------------------------
  # Excluding birth control
  #------------------------------------------------
  print 'Excluding birth control'
  query = """SELECT sex, count(sex) from patient as p
  WHERE sex IN (1,2)
  AND p.safetyreportid IN (
    SELECT DISTINCT dp.safetyreportid FROM drug_patient as dp where dp.drugindication NOT IN ('contraception', 'post coital contraception', 'birth control')
    )
  GROUP BY sex;"""
  cur.execute(query)
  d = iterateCursorCounts(cur)

  data = []
  query = """SELECT sex from patient as p
  WHERE sex IN (1,2)
  AND p.safetyreportid IN (
    SELECT DISTINCT dp.safetyreportid FROM drug_patient as dp where dp.drugindication NOT IN ('contraception', 'post coital contraception', 'birth control')
    );"""
  cur.execute(query)
  data = iterateCursor(cur)
  CIs = computeConfidenceInterval(10000,data)
  d['CI'] = CIs-1
  d['name'] = 'exclBC'
  Gender.append(d)

  #------------------------------------------------
  # Only Serious excluding birth control
  #------------------------------------------------

  print "Only serious side effects."
  query = """SELECT sex, count(sex) FROM patient AS p
  WHERE sex IN (1,2)
  AND p.safetyreportid IN (
    SELECT DISTINCT dp.safetyreportid FROM drug_patient as dp 
    INNER JOIN main as m on dp.safetyreportid=m.safetyreportid 
    WHERE m.serious = 1
    AND dp.drugindication NOT IN ('contraception', 'post coital contraception', 'birth control') 
    )
  GROUP BY sex;"""
  cur.execute(query)
  d = iterateCursorCounts(cur)

  data = []
  query = """SELECT sex FROM patient AS p
  WHERE sex IN (1,2)
  AND p.safetyreportid IN (
    SELECT DISTINCT dp.safetyreportid FROM drug_patient as dp 
    INNER JOIN main as m on dp.safetyreportid=m.safetyreportid 
    WHERE m.serious = 1
    AND dp.drugindication NOT IN ('contraception', 'post coital contraception', 'birth control') 
    );"""
  cur.execute(query)
  data = iterateCursor(cur)
  CIs = computeConfidenceInterval(10000,data)
  d['CI'] = CIs-1
  d['name'] = 'exclBCoSer'
  Gender.append(d)

  #------------------------------------------------
  # Only Health Prof excluding birth control
  #------------------------------------------------

  print "Only Health Prof"
  query = """SELECT sex, count(sex) FROM patient AS p
  WHERE sex IN (1,2)
  AND p.safetyreportid IN (
    SELECT DISTINCT dp.safetyreportid FROM drug_patient as dp 
    INNER JOIN main as m on dp.safetyreportid=m.safetyreportid 
    WHERE m.qualification IN (1,2,3)
    AND dp.drugindication NOT IN ('contraception', 'post coital contraception', 'birth control') 
    )
  GROUP BY sex;"""
  cur.execute(query)
  d = iterateCursorCounts(cur)

  data = []
  query = """SELECT sex FROM patient AS p
  WHERE sex IN (1,2)
  AND p.safetyreportid IN (
    SELECT DISTINCT dp.safetyreportid FROM drug_patient as dp 
    INNER JOIN main as m on dp.safetyreportid=m.safetyreportid 
    WHERE m.qualification IN (1,2,3)
    AND dp.drugindication NOT IN ('contraception', 'post coital contraception', 'birth control') 
    );"""
  cur.execute(query)
  data = iterateCursor(cur)
  CIs = computeConfidenceInterval(10000,data)
  d['CI'] = CIs-1
  d['name'] = 'exclBCoHP'
  Gender.append(d)

  #------------------------------------------------
  # Only Health Prof only Serious
  #------------------------------------------------

  print "Only Health Prof and Serious Cases."
  query = """SELECT sex, count(sex) FROM patient AS p
  WHERE sex IN (1,2)
  AND p.safetyreportid IN (
    SELECT DISTINCT dp.safetyreportid FROM drug_patient as dp 
    INNER JOIN main as m on dp.safetyreportid=m.safetyreportid 
    WHERE m.qualification IN (1,2,3)
    AND dp.drugindication NOT IN ('contraception', 'post coital contraception', 'birth control') 
    AND m.serious = 1
    )
  GROUP BY sex;"""
  cur.execute(query)
  d = iterateCursorCounts(cur)

  data = []
  query = """SELECT sex FROM patient AS p
  WHERE sex IN (1,2)
  AND p.safetyreportid IN (
    SELECT DISTINCT dp.safetyreportid FROM drug_patient as dp 
    INNER JOIN main as m on dp.safetyreportid=m.safetyreportid 
    WHERE m.qualification IN (1,2,3)
    AND dp.drugindication NOT IN ('contraception', 'post coital contraception', 'birth control') 
    AND m.serious = 1
    );"""
  cur.execute(query)
  data = iterateCursor(cur)
  CIs = computeConfidenceInterval(10000,data)
  d['CI'] = CIs-1
  d['name'] = 'exclBCoHPoS'
  Gender.append(d)

  #------------------------------------------------
  # Only Customer and not serious
  #------------------------------------------------

  print "Only consumers and not serious cases."
  query = """SELECT sex, count(sex) FROM patient AS p
  WHERE sex IN (1,2)
  AND p.safetyreportid IN (
    SELECT DISTINCT dp.safetyreportid FROM drug_patient as dp 
    INNER JOIN main as m on dp.safetyreportid=m.safetyreportid 
    WHERE m.qualification = 5
    AND dp.drugindication NOT IN ('contraception', 'post coital contraception', 'birth control') 
    AND m.serious = 2
    )
  GROUP BY sex;"""
  cur.execute(query)
  d = iterateCursorCounts(cur)

  data = []
  query = """SELECT sex FROM patient AS p
  WHERE sex IN (1,2)
  AND p.safetyreportid IN (
    SELECT DISTINCT dp.safetyreportid FROM drug_patient as dp 
    INNER JOIN main as m on dp.safetyreportid=m.safetyreportid 
    WHERE m.qualification = 5
    AND dp.drugindication NOT IN ('contraception', 'post coital contraception', 'birth control') 
    AND m.serious = 2
    );"""
  cur.execute(query)
  data = iterateCursor(cur)
  CIs = computeConfidenceInterval(10000,data)
  d['CI'] = CIs-1
  d['name'] = 'exclBCoCnS'
  Gender.append(d)

DATA = {}
DATA['GENDER'] = Gender
import pickle
pickle.dump( DATA, open( "gender.p", "wb" ) )

#------------------------------------------------
# Qualification (lawyer, health professional, consumer)
#------------------------------------------------

def iterateCursor(cur):
  d = {}
  names = []
  counts = []
  for e in cur:
    names.append(e[0])
    counts.append(int(e[1]))
  d['names'] = names
  d['counts'] = counts
  return d

def iterateCurCounts(cur):
  for e in cur:
    total = e[0]
  return total

Qualification = []
con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'fda_data'); #host, user, password, #database
with con:
  cur = con.cursor()

  # what side effects do lawyers report
  print "Get data for lawyers ..."
  query = """SELECT reaction, count(reaction) from adverse_effect as ae
  WHERE ae.safetyreportid IN (
    SELECT DISTINCT m.safetyreportid FROM main AS m WHERE m.qualification = 4)
  group by reaction 
  order by count(reaction) desc
  limit 5;"""
  cur.execute(query)
  d = iterateCursor(cur)
  d['qualification'] = 'lawyer'

  query = """SELECT count(safetyreportid) FROM main WHERE qualification = 4;"""
  cur.execute(query)
  total = iterateCurCounts(cur)
  proportions = []
  for entry in d['counts']:
    proportions.append(float(entry)/float(total))
  d['proportion'] = proportions
  Qualification.append(d)

  # what side effects do health profs report
  print "Get data for health professionals ..."
  query = """SELECT reaction, count(reaction) from adverse_effect as ae
  WHERE ae.safetyreportid IN (
    SELECT DISTINCT m.safetyreportid FROM main AS m WHERE m.qualification IN (1,2,3))
  group by reaction 
  order by count(reaction) desc
  limit 5;"""
  cur.execute(query)
  d = iterateCursor(cur)
  d['qualification'] = 'healthProf'

  query = """SELECT count(safetyreportid) FROM main WHERE qualification IN (1,2,3);"""
  cur.execute(query)
  total = iterateCurCounts(cur)
  proportions = []
  for entry in d['counts']:
    proportions.append(float(entry)/float(total))
  d['proportion'] = proportions  
  Qualification.append(d)

  # what side effects do consumers report
  print "Get data for consumers ..."
  query = """SELECT reaction, count(reaction) from adverse_effect as ae
  WHERE ae.safetyreportid IN (
    SELECT DISTINCT m.safetyreportid FROM main AS m WHERE m.qualification = 5)
  group by reaction 
  order by count(reaction) desc
  limit 5;"""
  cur.execute(query)
  d = iterateCursor(cur)
  d['qualification'] = 'consumer'

  query = """SELECT count(safetyreportid) FROM main WHERE qualification = 5;"""
  cur.execute(query)
  total = iterateCurCounts(cur)
  proportions = []
  for entry in d['counts']:
    proportions.append(float(entry)/float(total))
  d['proportion'] = proportions
  Qualification.append(d)


for i in range(0,len(Qualification)):
  filename = Qualification[i]['qualification'] + ".eps"
  df = pd.DataFrame({"x":['A','B','C','D','E'], "y":Qualification[i]['proportion']})
  # plot3 = ggplot(df, aes(x='x', y='y')) + geom_bar(aes(weight='y')) + theme(axis.text.x=element_text(angle=90, size=12), axis.title.x=element_text(size=16)) + ylab('') + xlab('') + ylim(0.2)

  plot1 = ggplot(df, aes(x='x', y='y')) + geom_bar(aes(weight='y')) + ylab('adverse effect') + xlab('proportion') + ylim(0,0.2) + theme(axis_text_x=element_text(angle=45,hjust=1,size=12))
  ggsave(plot1, filename, units = 'in', width = 4, height = 2.5)










