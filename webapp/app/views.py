
#-------------------------------
# Backend for Drugged-up.me organized as minimal app (http://flask.pocoo.org/docs/0.10/quickstart/)
#-------------------------------

from flask import Flask
app = Flask(__name__)

from flask import render_template, jsonify, request
import pymysql as mdb
import os
import pickle

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
    name.append(e[0].lower())
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
    names.append(e[0].lower())
  return names
  
def getNames(rxcui, cur):
  d = {}
  d['brandnames'] = getAlternateName(rxcui, 'tradename_of', 'BN', cur) # modify to also return generic name
  d['ingredients'] = getAlternateName(rxcui, 'form_of', 'PIN', cur)
  d['names'] = getRxCuiName(rxcui, cur)
  return d

def compareDrugs(drugName1, drugName2, cur, d):
  flag = 0
  rxcui1 = getRxCui(drugName1, cur)
  if rxcui1 == 'NULL':
    rxcui1 = getRxCuiRel1(drugName1, cur)
    valid = checkValidRxCui(rxcui1, cur)
    if not valid:
      flag = 1
  rxcui2 = getRxCui(drugName2, cur)
  if rxcui2 == 'NULL':
    rxcui2 = getRxCuiRel1(drugName2, cur)
    valid = checkValidRxCui(rxcui2, cur)
    if not valid:
      flag = 1
  if rxcui1 == rxcui2 and flag == 0:
    d['name'] = getRxCuiName(rxcui1, cur)[0]
    d['message'] = 1
  elif flag == 1:
    d['name'] = None
    d['message'] = -1
  else:
    d['name'] = None
    d['message']  = 0
  return d

def getTopFiveReactions(drugID, serious, cur):
  
  query = """SELECT count(dr.safetyreportid) FROM drug_report AS dr 
  JOIN main AS m ON dr.safetyreportid = m.safetyreportid
  JOIN adverse_effect AS ae ON dr.safetyreportid = ae.safetyreportid
  WHERE dr.rxcui = '%s'
  AND m.qualification IN (1,2,3,5)
  AND m.serious = '%s'
  AND ae.reaction NOT IN ('overdose', 'suicide', 'intentional overdose', 'completed suicide', 'drug misuse', 'intentional drug misuse');""" %(drugID,serious)
  cur.execute(query)
  for entry in cur:
    total = entry[0]

  data = []
  query = """SELECT ae.reaction, count(ae.reaction) FROM adverse_effect AS ae
  JOIN drug_report AS dr ON dr.safetyreportid = ae.safetyreportid
  JOIN main AS m ON m.safetyreportid = dr.safetyreportid
  WHERE dr.rxcui = '%s'
  AND m.qualification IN (1,2,3,5)
  AND m.serious = '%s'
  AND ae.reaction NOT IN ('overdose', 'suicide', 'intentional overdose', 'completed suicide', 'drug misuse', 'intentional drug misuse')
  GROUP BY ae.reaction
  ORDER BY count(ae.reaction) DESC LIMIT 10;""" %(drugID,serious)
  cur.execute(query)
  for entry in cur:
    d = {}
    d['reaction'] = entry[0].lower()
    d['proportion'] = float(entry[1]) / float(total)
    data.append(d)
  return data, total

def getMaxDose(drugName,cur, d):
  query = """SELECT maxDD FROM maxDose WHERE name = '%s';""" %drugName
  cur.execute(query)
  for entry in cur:
    d['maxDose'] = entry[0]
  return d

#-------------------------------
# Render index page
#-------------------------------

@app.route('/')

@app.route('/index')
def index():
    return render_template("index.html")

#-------------------------------
# Compare two drugs
#-------------------------------

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
      d = compareDrugs(drug1, drug2, cur, d)

    if d['name']:
      con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'max_Dosages'); #host, user, password, #database
      with con:
        cur = con.cursor()
        getMaxDose(d['name'],cur, d)
      d['averageFemale'] = d['maxDose']*72
      d['averageMale'] = d['maxDose']*89
    print d
    return jsonify(dict(d=d))

#-------------------------------
# Get sideeffects
#-------------------------------

@app.route("/_drug_reactions")
def _drug_reactions():
    drugSE = request.args.get('drugSE', 0, type=str)
    d = {}
    d['entry'] = drugSE
    print "searching for " + str(drugSE) 

    con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'rx_norm'); #host, user, password, #database
    with con:
      cur = con.cursor()

      # Look up RxCui drug ID
      print "Looking up rxcui"
      rxcui = getRxCui(drugSE, cur)
      print rxcui
      if rxcui == 'NULL':
        print "Looking up rxcui via relation"
        rxcui = getRxCuiRel1(drugSE, cur)
        print rxcui

      # get ingredient and brand names
      if rxcui and rxcui != 'NULL':
        d['okay'] = 1
        result = getNames(rxcui, cur)
        d['names'] = []
        d['ingredients'] = []
        d['brandnames'] = []
        for i in result['names']:
          d['names'].append(dict(name=i))
        for i in result['ingredients']:
          d['ingredients'].append(dict(ingredient=i))
        for i in result['brandnames']:
          d['brandnames'].append(dict(brandname=i))

        # percentages of side effects for serious and not serious cases
        con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'fda_data'); #host, user, password, #database
        with con:
          print "Looking up sideeffets ..."
          cur = con.cursor()
          data, totalSerious = getTopFiveReactions(rxcui, 1, cur)
          d['sideeffects'] = {}
          d['sideeffects']['serious'] = data
          data, totalNotSerious = getTopFiveReactions(rxcui, 2, cur)
          d['sideeffects']['notSerious'] = data
          x =  (float(totalSerious) / float(totalSerious+totalNotSerious))*100
          d['propSerious'] = "{:10.2f}".format(x).strip()
          d['total'] = totalSerious+totalNotSerious

          # get the most distriminative features
          d['features'] = {}
          currentDirectory = os.getcwd()
          dataLocation = currentDirectory+'/data'
          allFiles = [f for f in os.listdir(dataLocation) if '.p' in f]
          processed = [] 
          for f in allFiles:
            processed.append(int(f.split('.')[0]))
          if int(rxcui) in processed:
            os.chdir(dataLocation)
            filename = str(rxcui)+'.p'
            f = open(filename, 'rb')
            discriminativeFeatures = pickle.load(f)
            f.close()
            os.chdir(currentDirectory)

            # include only if accuracy higher than 0.60
            if discriminativeFeatures['accuracy']>0.60:
              d['features']['okay'] = 1
              harmless = []
              harmful = []
              for feature in discriminativeFeatures['harmless']:
                if feature['odds'] > 2:
                  harmless.append(feature['name'])
              for feature in discriminativeFeatures['harmful']:
                if feature['odds'] > 2:
                  harmful.append(feature['name'])
              
              # show top 10 most discriminative features (or less if there are less)
              if len(harmless)>=10:
                harmless = harmless[:10]
              if len(harmful)>=10:
                harmful = harmful[:10]

              # include in d
              d['features']['harmless'] = harmless
              d['features']['harmfull'] = harmful

            else:
              d['features']['okay'] = 0
          else:
            d['features']['okay'] = 0
      else:
        d['okay'] = 0
    
    return jsonify(dict(d=d))

#-------------------------------
# run
#-------------------------------

if __name__ == "__main__":
  app.run(host='0.0.0.0',port=5000)


