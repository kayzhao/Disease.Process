__author__ = 'kayzhao'

import re
import pandas as pd
from pymongo import MongoClient
from databuild.nci import *

def load_data():
    # code < tab > concept
    # name < tab > parents < tab > synonyms < tab > definition < tab > display
    # name < tab > concept
    # status < tab > semantic
    # type < EOL >
    col_names = ['_id', 'concept_name', 'parents', 'synonym', 'definition',
                 'display_name', 'concept_status', 'semantic_type']
    df = pd.read_csv(diseases_path, sep="\t", comment='#', names=col_names)
    # change to list of dict
    d = []
    df['_id'] = df['_id'].apply(lambda x: "NCI:" + x)
    for record in df.apply(lambda x: x.dropna().to_dict(), axis=1):
        if 'parents' in record:
            l = []
            for x in re.split("\\|", record['parents']):
                l.append(x.strip())
            # print(drugs)
            record['parents'] = l

        if 'synonym' in record:
            l = []
            for x in re.split("\\|", record['synonym']):
                l.append(x.strip())
            # print(drugs)
            record['synonym'] = l

        if 'display_name' in record:
            l = []
            for x in re.split("\\|", record['display_name']):
                l.append(x.strip())
            # print(drugs)
            record['display_name'] = l

        if 'concept_status' in record:
            l = []
            for x in re.split("\\|", record['concept_status']):
                l.append(x.strip())
            # print(drugs)
            record['concept_status'] = l

        one_doc = {str(k): v for k, v in record.items() if k is not None}
        # print(one_doc)
        d.append(one_doc)

    return d


def parse(mongo_collection=None, drop=True):
    if mongo_collection:
        db = mongo_collection
    else:
        client = MongoClient()
        db = client.disease.mesh
    if drop:
        db.drop()

    print("------------nci data parsing--------------")
    diseases = load_data()
    print("load success")
    for x in diseases:
        print(x)
        db.insert_one(x)
    # db.insert_many(kegg_disease)
    print("insert success")
    print("------------nci data parsed success--------------")
