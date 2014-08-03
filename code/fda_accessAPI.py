#--------------------------------------------
# https://open.fda.gov/drug/event/reference/#data-downloads
# http://www.fda.gov/Drugs/InformationOnDrugs/ucm079750.htm [list of FDA approved drugs and notes on approval process]
# http://www.fda.gov/drugs/informationondrugs/ucm129689.htm
# http://www.medilexicon.com/drugsearch.php?z=true
# http://www.centerwatch.com/drug-information/fda-approved-drugs/drug-names/A # can be used for known vs. novel side effects
# http://www.ncbi.nlm.nih.gov/pubmed/16136624
#--------------------------------------------

import requests
import json
import pandas as pd
import os
import sys

# parameters
save = None

# load list of all FDA approved drugs (flat file)
filename = 'fda_allApproved/Product.txt'
appDrugs = pd.read_table(filename,delimiter='\t')
print 'FDA approved a total of ' + str(len(appDrugs)) + ' drugs.\n'

# interacting with API
myKey = 'R4RFOXASGiQNvdPPD7XHSRZ0jGTg7xVrhjdtpa1N'
root = 'https://api.fda.gov'
apiDrugs = '/drug/event' # drug event endpoint
apiFormat = '.json?' # results returns in json format

# limit query to past 6 months
query = '&search=receivedate:[20130601+TO+20150101]'

a = 99
b = 0
results = []
while True:
  limits = '&limit='+str(a)+'&skip='+str(b)
  url = root+apiDrugs+apiFormat+'api_key='+myKey+query+limits

  print 'Accessing: ' + query+limits + '\n'
  page = requests.get(url)
  page = page.json()
  
  try:
    for r in page['results']:
      results.append(r)
    b = b+99
  except KeyError:
    print page['error']['message']
    break

print 'Records retrieved: ' + str(len(results)) + '\n'

# save JSON for later use if save
if save:
  filename = 'FDA_20130601-20150101.json'
  with open(filename, "w") as outfile:
      json.dump(results, outfile, sort_keys=True, indent=2)

data = {}
howMany = []
for r in results:

  howMany.append(len(r['patient']['drug']))

  # only include patients who take one drug
  if len(r['patient']['drug']) == 1:
    
    if 'id' not in data:
      data['id'] = []
    data['id'].append(r['safetyreportid'])
  
    if 'drugIndication' not in data:
      data['drugIndication'] = []
    try:
      data['drugIndication'].append(r['patient']['drug'][0]['drugindication'])
    except KeyError:
      data['drugIndication'].append(None)

    if 'serious' not in data:
      data['serious'] = []
    try:
      data['serious'].append(int(r['serious']))
    except KeyError:
      data['serious'].append(None)

    if 'weight' not in data:
      data['weight'] = []
    try:
      data['weight'].append(float(r['patient']['patientweight']))
    except KeyError:
      data['weight'].append(None)

    if 'sex' not in data:
      data['sex'] = []
    try:
      data['sex'].append(int(r['patient']['patientsex']))
    except KeyError:
      data['sex'].append(None)
    
    if 'age' not in data:
      data['age'] = []
    try:
      data['age'].append(float(r['patient']['patientonsetage']))
    except KeyError:
      data['age'].append(None)

    if 'outcome' not in data:
      data['outcome'] = []
    try:
      data['outcome'].append(int(r['patient']['reaction'][0]['reactionoutcome']))
    except KeyError:
      data['outcome'].append(None)

    if 'reaction' not in data:
      data['reaction'] = []
    try:
      data['reaction'].append(r['patient']['reaction'][0]['reactionmeddrapt'])
    except KeyError:
      data['reaction'].append(None)
    
df = pd.DataFrame(data,index=data['id'])

gb = df.groupby(['sex','serious'])
gb.median() # females overrepresented compared to population, females younger, age gap

# most common reactions
yvals = df.reaction.value_counts()[0:6]
xTick = tuple(df.reaction.value_counts().index[0:6])
xvals = range(0,6)
barGraph(xvals,yvals,1,xTick,'adverse reaction','counts',0.5,'adverseReaction.jpeg')

# most common reactions
yvals = df.drugIndication.value_counts()[0:6]
xTick = df.drugIndication.value_counts().index[0:6]
xvals = range(0,6)
barGraph(xvals,yvals,1,xTick,'cause for taking medication','counts',0.5,'reasonForTakingMed.jpeg')

def barGraph(xvals,yvals,width,xTick,xLabel,yLabel,bottomAdjust,saveFigure=0):
  fig, ax = plt.subplots()
  rects = ax.bar(xvals,yvals,width,color='r')
  ax.set_ylabel(yLabel)
  ax.set_title(xLabel)
  ax.set_xticks(xvals)
  ax.set_xticklabels(xTick)
  for tick in ax.xaxis.get_major_ticks():
      tick.label.set_fontsize(8) 
      tick.label.set_rotation('vertical')
  if bottomAdjust != 0:
    fig.subplots_adjust(bottom=bottomAdjust)
  plt.show()
  if isinstance(saveFigure,str):
    fig.set_size_inches(7,5)
    plt.savefig(saveFigure,dpi=100)


url = 'https://api.fda.gov/drug/event.json?search=receivedate:[20040101+TO+20150101]+AND+patient.drug.openfda.substance_name:FESOTERODINE+FUMARATE&count=receivedate'
page = requests.get(url)
page = page.json()

counts = []
for r in page['results']:
  counts.append(r['count'])



