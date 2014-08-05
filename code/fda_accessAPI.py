#--------------------------------------------
# README: this file accesses the FDA API for a year determined by user input (e.g. 2013) and saves the data as a JSON file
#--------------------------------------------

#--------------------------------------------
# modules
#--------------------------------------------
import requests
import json
import pandas as pd
import os
import sys
import argparse

#--------------------------------------------
# parameters
#--------------------------------------------
save = 1
myKey = 'R4RFOXASGiQNvdPPD7XHSRZ0jGTg7xVrhjdtpa1N'
root = 'https://api.fda.gov'
apiDrugs = '/drug/event' # drug event endpoint
apiFormat = '.json?' # results returns in json format

# path names
location_code = '/Users/Friederike/Dropbox/Insight/Project/FDA/code'
location_data = '/Users/Friederike/Dropbox/Insight/Project/FDA/data/FDA_jsons'

#--------------------------------------------
# access API & return JSON
#--------------------------------------------

if __name__ == "__main__":

  parser = argparse.ArgumentParser(description = 'Enter the year of FDA data for download (2004 till present)')
  parser.add_argument("dataToDownload", help = "Enter what year of FDA data to download.", type = str)
  args = parser.parse_args()
  year = args.dataToDownload
  
  yearEnd = str(int(year)+1)
  time_span = '['+year+'0101+TO+'+yearEnd+'0101]'
  query = '&search=receivedate:' + time_span

  a = 99 # number of entries to receive (API only allows queries that return entries < 100)
  b = 0  # number of entries to skip

  results = []
  while b<= 1000:

    limits = '&limit='+str(a)+'&skip='+str(b)
    url = root+apiDrugs+apiFormat+'api_key='+myKey+query+limits

    print 'Accessing: ' + query+limits + '\n'
    page = requests.get(url)
    page = page.json()

    try:
      totalRecords = page['meta']['results']['total']
    except KeyError:
      totalRecords = -1

    try:
      for r in page['results']:
        results.append(r)
      b = b+99
    except KeyError:
      print page['error']['message']
      break

  print 'Records retrieved: ' + str(len(results)) + ' out of ' + str(totalRecords) + '\n'

  # save JSON for later use if save
  if save:
    os.chdir(location_data)
    filename = 'FDA_'+year+'.json'
    with open(filename, "w") as outfile:
        json.dump(results, outfile, sort_keys=True, indent=2)
    os.chdir(location_code)


