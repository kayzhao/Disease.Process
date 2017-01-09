__author__ = 'kayzhao'

import pandas as pd
from pymongo import MongoClient
from databuild.lncrna import *


def load_disease_list(docs):
    dis_d = dict()
    for doc in docs:
        dis_d['name'] = doc['_id']
        if 'synonym' in doc:
            for x in doc['synonym']:
                dis_d[x.lower().split('(', 1)[0]] = doc['_id']
        if 'name' in doc:
            dis_d[doc['name'].lower()] = doc['_id']
            # print(len(dis_d))
    return dis_d


def load_lnc_disease(d_dis):
    col_names = ['number', 'lncrna_name', 'disease_name', 'dyregluation_type', 'description',
                 'chr', 'start', 'end', 'strand', 'species', 'alias', 'genbank_id', 'pmid']
    f = open(lncRNA_diseases_path, "r", encoding='utf-8', errors='ignore')
    df = pd.read_csv(f, sep='\t', comment="#", header=None, names=col_names)
    del df['number']

    # get rid of nulls
    sub = df.apply(lambda x: x.dropna(), axis=1).to_dict(orient="records")
    sub = [{k: v for k, v in s.items() if v == v} for s in sub]

    print(len(sub))
    for record in sub:
        record['source'] = "The LncRNADisease database"
        # print(record)
        if 'disease_name' in record and record['disease_name'].lower() in d_dis:
            record['disease_id'] = d_dis[record['disease_name'].lower()]

    print(len(sub))
    return sub


def parse(mongo_collection=None, drop=True):
    if mongo_collection:
        db = mongo_collection
    else:
        client = MongoClient()
        db = client.disease.mesh
    if drop:
        db.drop()


if __name__ == "__main__":
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.113:27017/biodis')
    # diseases = load_disease_list(bio_client.biodis.disease_no_umls.find({}))
    diseases = load_disease_list(bio_client.biodis.disease.find({}))
    print(len(diseases))
    d = load_lnc_disease(diseases)
    bio_client.biodis.lncrna.insert_many(d)