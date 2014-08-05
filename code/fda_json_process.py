#--------------------------------------------
# README: Parses the JSON files obtained from FDA API
#--------------------------------------------

#--------------------------------------------
# modules
#--------------------------------------------
import json
import pandas as pd
import os
import sys
import argparse

#--------------------------------------------
# parameters
#--------------------------------------------

# path names
location_code = '/Users/Friederike/Dropbox/Insight/Project/FDA/code'
location_data = '/Users/Friederike/Dropbox/Insight/Project/FDA/data/FDA_jsons'

if __name__ == "__main__":

  #--------------------------------------------
  # load json file
  #--------------------------------------------
  os.chdir(location_data)
  files = [f for f in os.listdir(location_data) if 'json' in f]
  os.chdir(location_code)

  