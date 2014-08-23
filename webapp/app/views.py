from flask import render_template, Flask, jsonify, request
from app import app
import pymysql as mdb
import os

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

# function to get rxcui in case provided name is an ingredient name
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

# function returns the brand name: "form_of" / "tradename_of"
def getAlternateName(drugID, relation, typeEntity, cur):
  query = """SELECT str 
    FROM rxnconso AS a
    INNER JOIN rxnrel as b
    ON a.rxcui = b.rxcui2
    WHERE b.rxcui1 = '%s' AND b.rela = '%s'
    AND a.tty = '%s' AND a.sab = 'rxnorm';""" %(drugID,relation,typeEntity)      
  cur.execute(query)
  names = []
  for e in cur:
    names.append(e[0])
  return names
  
def getNames(drugName, cur):
  rxcui = getRxCui(drugName, cur)
  if rxcui == 'NULL':
    rxcui = getRxCuiRel1(drugName, cur)
    valid = checkValidRxCui(rxcui, cur)
    if not valid:
      return "no entry"
  d = {}
  d['brandnames'] = getAlternateName(rxcui, 'tradename_of', 'BN', cur) # modify to also return generic name
  d['ingredients'] = getAlternateName(rxcui, 'form_of', 'PIN', cur)
  d['names'] = getRxCuiName(rxcui, cur)
  return d

def compareDrugs(drugName1, drugName2, cur, d):
  rxcui1 = getRxCui(drugName1, cur)
  if rxcui1 == 'NULL':
    rxcui1 = getRxCuiRel1(drugName1, cur)
    valid = checkValidRxCui(rxcui1, cur)
    if not valid:
      return "no entry"
  rxcui2 = getRxCui(drugName2, cur)
  if rxcui2 == 'NULL':
    rxcui2 = getRxCuiRel1(drugName2, cur)
    valid = checkValidRxCui(rxcui2, cur)
    if not valid:
      return "no entry"
  message = None
  if rxcui1 == rxcui2:
    d['name'] = getRxCuiName(rxcui1, cur)
    d['message'] = "YES! Be careful of accidental overdose!"
  else:
    d['name'] = 'no overlap'
    d['message']  = "Nope! You're fine."
  return d

def getTopFiveReactions(drugID, cur):
  
  query = """SELECT count(dr.safetyreportid) FROM drug_report AS dr 
  JOIN main AS m ON dr.safetyreportid = m.safetyreportid
  WHERE dr.rxcui = '%s'
  AND m.qualification IN (1,2,3);""" %drugID
  cur.execute(query)
  for entry in cur:
    total = entry[0]

  data = []
  query = """SELECT ae.reaction, count(ae.reaction) FROM adverse_effect AS ae
  JOIN drug_report AS dr ON dr.safetyreportid = ae.safetyreportid
  JOIN main AS m ON m.safetyreportid = dr.safetyreportid
  WHERE dr.rxcui = '%s'
  AND m.qualification IN (1,2,3)
  GROUP BY ae.reaction
  ORDER BY count(ae.reaction) DESC LIMIT 10;""" %drugID
  cur.execute(query)
  for entry in cur:
    print entry[1]
    d = {}
    d['reaction'] = entry[0].lower()
    d['proportion'] = float(entry[1]) / float(total)
    data.append(d)
  return data

#-------------------------------
# web code
#-------------------------------


@app.route('/')

@app.route('/index')
def index():
    return render_template("index.html")

@app.route("/_drug3")
def _drug3():
    drug = request.args.get('drug3', 0, type=str)
    d = {}
    d['entry'] = drug
    con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'rx_norm'); #host, user, password, #database
    with con:
      cur = con.cursor()
      result = getNames(drug, cur)
    d['names'] = []
    d['ingredients'] = []
    d['brandnames'] = []
    for i in result['names']:
      d['names'].append(dict(name=i))
    for i in result['ingredients']:
      d['ingredients'].append(dict(ingredient=i))
    for i in result['brandnames']:
      d['brandnames'].append(dict(brandname=i))
    return jsonify(dict(d=d))

@app.route("/_drug1")
def _drug_combined():
    drug1 = request.args.get('drug1', 0, type=str)
    drug2 = request.args.get('drug2', 0, type=str)
    d = {}
    d['entry1'] = drug1
    d['entry2'] = drug2
    con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'rx_norm'); #host, user, password, #database
    with con:
      cur = con.cursor()
      message = compareDrugs(drug1, drug2, cur, d)
    return jsonify(dict(d=d))

@app.route("/_drug_reactions")
def _drug_reactions():
    drugSE = request.args.get('drugSE', 0, type=str)
    d = {}
    d['entry'] = drugSE
    print "searching for " + str(drugSE) 

    con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'rx_norm'); #host, user, password, #database
    with con:
      cur = con.cursor()
      print "Looking up rxcui"
      rxcui = getRxCui(drugSE, cur)
      print rxcui
      if rxcui == 'NULL':
        print "Looking up rxcui via relation"
        rxcui = getRxCuiRel1(drugSE, cur)
        print rxcui

    con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'fda_data'); #host, user, password, #database
    with con:
      print "Looking up sideeffets ..."
      cur = con.cursor()
      data = getTopFiveReactions(rxcui, cur)
    d['sideeffects'] = data
    return jsonify(dict(d=d))


