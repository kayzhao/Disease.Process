__author__ = 'kayzhao'

import pandas as pd
from pymongo import MongoClient
from databuild.mirna import *
from tqdm import tqdm
from utils.common import list2dict, dict2list


def load_disease_list(docs):
    col_names = ['disease_name', 'doid']
    f = open(diseases_path, "r", encoding='utf-8', errors='ignore')
    dis_d = dict()
    for record in pd.read_csv(f, sep='\t', comment="#", header=None, names=col_names).to_dict(orient="records"):
        dis_d[record['disease_name'].lower()] = record['doid']

    for doc in docs:
        dis_d['name'] = doc['_id']
        if 'synonym' in doc:
            for x in doc['synonym']:
                dis_d[x] = doc['_id']
    return dis_d


def load_hdmm(d_dis):
    col_names = ['number', 'mirna_name', 'disease_name', 'pubmed', 'description']
    f = open(hdmm_path, "r", encoding='utf-8', errors='ignore')
    df = pd.read_csv(f, sep='\t', comment="#", header=None, names=col_names)
    del df['number']

    # get rid of nulls
    sub = df.apply(lambda x: x.dropna(), axis=1).to_dict(orient="records")
    sub = [{k: v for k, v in s.items() if v == v} for s in sub]

    print(len(sub))
    for record in sub:
        # print(record)
        if 'disease_name' in record:
            if record['disease_name'].lower() in d_dis:
                record['disease_id'] = d_dis[record['disease_name'].lower()]
        else:
            del record
    print(len(sub))
    return sub


def load_mi2Rdisease(d_dis):
    col_names = ['mirna_name', 'disease_name', 'regulate', 'origin_source', 'year', 'description']
    f = open(miR2disease_path, "r", encoding='utf-8', errors='ignore')
    df = pd.read_csv(f, sep='\t', comment="#", header=None, names=col_names)
    # df['year'] = df['year'].dropna().astype(int)
    df['year'] = df['year'].map('{:.0f}'.format)

    # get rid of nulls
    sub = df.apply(lambda x: x.dropna(), axis=1).to_dict(orient="records")
    sub = [{k: v for k, v in s.items() if v == v} for s in sub]
    print(len(sub))
    for record in sub:
        # print(record)
        if 'disease_name' in record:
            if record['disease_name'].lower() in d_dis:
                record['disease_id'] = d_dis[record['disease_name'].lower()]
        else:
            del record
    print(len(sub))
    return sub


def parse(mongo_collection=None, drop=True):
    # if mongo_collection:
    # db = mongo_collection
    # else:
    # client = MongoClient()
    # db = client.disease.mesh
    # if drop:
    # db.drop()

    print("------------umls data parsing--------------")

    d_hdmm = load_hdmm()
    d_mi2Rdisease = load_mi2Rdisease()
    for x in d_hdmm:
        print(x)

    print("------------umls data parsed success--------------")


if __name__ == "__main__":
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.110:27017/biodis')
    dis_d = load_disease_list(bio_client.biodis.disease.find({}))
    print(len(dis_d))
    d_hdmm = load_hdmm(dis_d)
    d_mi2Rdisease = load_mi2Rdisease(dis_d)
    bio_client.biodis.d_hdmm.insert_many(d_hdmm)
    bio_client.biodis.d_mi2R.insert_many(d_mi2Rdisease)