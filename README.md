AdverseReactions - or - Drugged-up.me
================

Use of RxNorm (http://www.nlm.nih.gov/research/umls/rxnorm/) and open FDA data to explore what side effects people report to the FDA. 

- fda_accessAPI.py: queries the FDA API and downloads all files for a given year. Returns a list of 100 entries per list item. Each entry is a JSON file and contains one report to the FDA of an adverse reaction. Files are saved per year retrieved. 
- fda_json_toSql.py: Load the JSON files and saves information, after some preprocessing of info reported to the FDA, to MySQL database.
- fda_getRxcui.py: Retrieves numeric ID for drug names (name normalization). 
- aers_to_sql.py: enters adverse side effect classes into MySQL with retrieved drug IDs.
- fda_exploreReportingBias.py: Looks at biases in reporting for Gender and reporting instance.
- fda_model.py (in progress): builds logistic regression model to determine what sideeffects can predict membership of drug side effect class.
