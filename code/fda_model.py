
#--------------------------------------------
# modules
#--------------------------------------------
import json
import pandas as pd
import os
import pymysql as mdb
from pandas import DataFrame
import numpy as np

import matplotlib.pyplot as plt

from sklearn.linear_model import LogisticRegression
from sklearn import datasets
from sklearn.preprocessing import StandardScaler
from sklearn import cross_validation

from sklearn.grid_search import GridSearchCV
from sklearn.metrics import classification_report
from sklearn.metrics import roc_curve, auc

#--------------------------------------------
# 
#--------------------------------------------



#--------------------------------------------
# 
#--------------------------------------------

DRUGS = {}

# get all distinct rxcuis from aers
con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'aers'); #host, user, password, #database
with con:
  cur = con.cursor()
  query = """SELECT DISTINCT rxcui 
  FROM rxcuis;"""
  cur.execute(query)
  for entry in cur:
    DRUGS[entry[0]] = []

# get all distinct side effects only for physicians and 1 drug taken
con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'fda_data'); #host, user, password, #database
with con:
  cur = con.cursor()
  query = """SELECT DISTINCT reaction 
  FROM adverse_effect AS ae
  JOIN main AS m 
  ON m.safetyreportid = ae.safetyreportid
  JOIN patient AS p
  ON p.safetyreportid = m.safetyreportid
  WHERE m.qualification IN (1,2,3)
  AND p.number_drugs = 1;"""
  cur.execute(query)
  SIDEEFFECTS = []
  for entry in cur:
    SIDEEFFECTS.append(entry[0])

  for rxcui in DRUGS:
    DRUGS[rxcui] = {}
    for sideeffect in SIDEEFFECTS:
      DRUGS[rxcui][sideeffect] = 0

  # frequency of side effects per drug ...
  for rxcui in DRUGS:
    query = """SELECT reaction, count(reaction) 
    FROM adverse_effect AS ae
    JOIN drug_report AS dr 
    ON ae.safetyreportid = dr.safetyreportid
    JOIN main AS m
    ON m.safetyreportid = dr.safetyreportid
    JOIN patient AS p
    ON p.safetyreportid = m.safetyreportid
    WHERE dr.rxcui = '%s' 
    AND m.qualification IN (1,2,3)
    AND p.number_drugs = 1
    GROUP BY reaction;""" %rxcui
    cur.execute(query)
    for entry in cur:
      print entry[1]
      DRUGS[rxcui][entry[0]] = entry[1]

# from counts to proportions
for rxcui in DRUGS:
  total = 0
  for sideeffect in DRUGS[rxcui]:
    total += DRUGS[rxcui][sideeffect]
  for sideeffect in DRUGS[rxcui]:
    DRUGS[rxcui][sideeffect] = float(DRUGS[rxcui][sideeffect]) / (float(total)+0.00000000001)

for rxcui in DRUGS:
  data = []
  for sideeffect in SIDEEFFECTS:
    data.append(DRUGS[rxcui][sideeffect])
  DRUGS[rxcui]['data'] = data

# get all distinct rxcuis from aers
con = mdb.connect(host = 'localhost', user = 'friederike', passwd = 'fsas2403', db = 'aers'); #host, user, password, #database
with con:
  cur = con.cursor()

  # returns side effect classes
  query = """SELECT DISTINCT sideeffect 
  FROM rxcuis;"""
  cur.execute(query)
  seClasses = []
  for entry in cur:
    seClasses.append(entry[0])

#----------------------------------------
# select drugs and their proportions of side effects for each AE Class
#----------------------------------------

SECLASS = {}
Cs = np.linspace(0.01,20,100)
Cs = np.append(Cs, np.linspace(20,1000,50))
threshold = 0.001


for seClass in seClasses:

  print seClass + "\n"
  SECLASS[seClass] = {}
  
  # positive entries
  query = """SELECT rxcui FROM rxcuis WHERE sideeffect = "%s";""" %seClass
  cur.execute(query)
  aeRxcuis = []
  for entry in cur:
    aeRxcuis.append(entry[0])
  xVal = []
  yVal = []
  for rxcui in aeRxcuis:
    xVal.append(DRUGS[rxcui]['data'])
    yVal.append(1)

  # negative entries
  query = """SELECT rxcui FROM rxcuis WHERE sideeffect != "%s";""" %seClass
  cur.execute(query)
  aeRxcuis = []
  for entry in cur:
    aeRxcuis.append(entry[0])
  for rxcui in aeRxcuis:
    xVal.append(DRUGS[rxcui]['data'])
    yVal.append(0)

  # remove columns where all values are below threshold
  df = DataFrame(xVal)
  dfSmall = DataFrame()
  for i in range(0,len(df.columns)):
    if sum(df[i]<threshold) < len(df.index):
      dfSmall[SIDEEFFECTS[i]] = df[i]

  # convert for analysis
  xVal = np.array(dfSmall)
  yVal = np.array(yVal)

  # scale data
  scal_params = StandardScaler().fit(xVal)
  X = StandardScaler().fit_transform(xVal)
  y = yVal

  # split into train and test set
  X, X_hold, y, y_hold = cross_validation.train_test_split(X, y, test_size=0.2, random_state=0)

  # split into train and test set
  X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.4, random_state=0)

  a = []
  b = []
  SECLASS[seClass]['paramsNames'] = []
  SECLASS[seClass]['paramsValues'] = []
  for c in Cs:
    lrL1 = LogisticRegression(C = c, penalty='l1')
    y_score = lrL1.fit(X_train, y_train).predict_proba(X_test)

    # Compute ROC curve and ROC area for each class
    fpr = dict()
    tpr = dict()
    roc_auc = dict()
    fpr, tpr, _ = roc_curve(y_test, y_score[:,1])
    roc_auc = auc(fpr, tpr)
    a.append(roc_auc)
    b.append(float(sum(lrL1.coef_>0))/xVal.shape[1])
    SECLASS[seClass]['paramsNames'].append(dfSmall.columns[(lrL1.coef_>0).tolist()[0]])
    SECLASS[seClass]['paramsNames'].append(lrL1.coef_)


  name = {}
  for i,sideeffect in enumerate(dfSmall.columns):
    if lrL1.coef_.tolist()[0][i] > 0:
      name[lrL1.coef_.tolist()[0][i]] = sideeffect

  SECLASS[seClass]['roc_auc'] = a
  SECLASS[seClass]['nrParameters'] = b
  SECLASS[seClass]['paramCoef'] = name

for seClass in seClasses:

  c = Cs[SECLASS[seClass]['roc_auc'].index(max(SECLASS[seClass]['roc_auc']))]
  print c

  lrL1 = LogisticRegression(C = c, penalty='l1')
  y_score = lrL1.fit(X_train, y_train).predict_proba(X_test)

  # Compute ROC curve and ROC area for each class
  fpr = dict()
  tpr = dict()
  roc_auc = dict()
  fpr, tpr, _ = roc_curve(y_test, y_score[:,1])
  roc_auc = auc(fpr, tpr)

  name = {}
  for i,sideeffect in enumerate(dfSmall.columns):
    if lrL1.coef_.tolist()[0][i] > 0:
      name[lrL1.coef_.tolist()[0][i]] = sideeffect
  SECLASS['coeff'] = name

  # Plot of a ROC curve for a specific class
  plt.figure()
  plt.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
  plt.plot([0, 1], [0, 1], 'k--')
  plt.xlim([0.0, 1.0])
  plt.ylim([0.0, 1.05])
  plt.xlabel('False Positive Rate')
  plt.ylabel('True Positive Rate')
  plt.title('Receiver operating characteristic example')
  plt.legend(loc="lower right")
  plt.show()



# param_grid = [
#   {'C': [1, 10, 100, 1000, 10000], 'kernel': ['logistic']},
#   ]

# scores = ['precision', 'recall']

# for score in scores:
#     print("# Tuning hyper-parameters for %s" % score)
#     print()

#     clf = GridSearchCV(LogisticRegression(C = 1, penalty='l1'), param_grid, cv=5, scoring=scores)
#     clf.fit(X_train, y_train)

#     print("Best parameters set found on development set:")
#     print()
#     print(clf.best_estimator_)
#     print()
#     print("Grid scores on development set:")
#     print()
#     for params, mean_score, scores in clf.grid_scores_:
#         print("%0.3f (+/-%0.03f) for %r"
#               % (mean_score, scores.std() / 2, params))
#     print()

#     print("Detailed classification report:")
#     print()
#     print("The model is trained on the full development set.")
#     print("The scores are computed on the full evaluation set.")
#     print()
#     y_true, y_pred = y_test, clf.predict(X_test)
#     print(classification_report(y_true, y_pred))
#     print()

# Cs = np.linspace(0.00000000000001,10000,30)
# results = []
# for c in Cs:
#   clf = LogisticRegression(C=c)
#   k_fold = cross_validation.KFold(len(y), n_folds=10, indices=True, shuffle=True, random_state=18)

#   AUCs = []
#   AUCs_proba = []
#   for train, test in k_fold:
#       clf.fit(X[train], y[train])
#       AUCs.append(metrics.auc_score(y[test], clf.predict(X[test])))
#       AUCs_proba.append(metrics.auc_score(y[test], clf.predict_proba(X[test])[:, 1]))

#   print "AUCs: "
#   print AUCs
#   print "AUCs (with probabilities): "
#   print AUCs_proba
#   results.append(mean(AUCs_proba))



