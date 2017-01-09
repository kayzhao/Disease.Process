__author__ = 'kayzhao'

import pandas as pd
from pymongo import MongoClient
from databuild.mirna import *


def load_disease_list(docs):
    col_names = ['disease_name', 'doid']
    f = open(diseases_path, "r", encoding='utf-8', errors='ignore')
    dis_d = dict()
    for record in pd.read_csv(f, sep='\t', comment="#", header=None, names=col_names).to_dict(orient="records"):
        dis_d[record['disease_name'].lower()] = record['doid']

    for doc in docs:
        if 'synonym' in doc:
            for x in doc['synonym']:
                dis_d[x.lower()] = doc['_id']
        if 'name' in doc:
            dis_d[doc['name'].lower()] = doc['_id']
            # print(len(dis_d))
    return dis_d


def load_hdmm(d_dis):
    col_names = ['number', 'mirna_name', 'disease_name', 'pubmed', 'description']
    f = open(hdmm_path, "r", encoding='utf-8', errors='ignore')
    df = pd.read_csv(f, sep='\t', comment="#", header=None, names=col_names)
    del df['number']

    # get rid of nulls
    sub = df.apply(lambda x: x.dropna(), axis=1).to_dict(orient="records")
    sub = [{k: v for k, v in s.items() if v == v} for s in sub]

    miR_targets = load_mi2Rtarget()
    print(len(sub))
    for record in sub:
        record['map_id'] = 'for the mapping disease unique id'
        record['source'] = "HMDD (the Human microRNA Disease Database)"
        # print(record)
        if 'disease_name' in record and record['disease_name'].lower() in d_dis:
            record['disease_id'] = d_dis[record['disease_name'].lower()]
        if 'mirna_name' in record and record['mirna_name'] in miR_targets:
            record['mirna_target'] = miR_targets[record['mirna_name']]
    print(len(sub))
    return sub


def load_mi2Rtarget():
    col_names = ['mirna_name', 'validated_target', 'publish_date', 'reference']
    f = open(miR2target_path, "r", encoding='utf-8', errors='ignore')
    dis_d = dict()
    df = pd.read_csv(f, sep='\t', comment="#", header=None, names=col_names)
    df['publish_date'] = df['publish_date'].dropna().map('{:.0f}'.format)
    df['target_source'] = 'Tarbase'
    for mirna_name, subdf in df.groupby('mirna_name'):
        # del subdf['mirna_name']
        # get rid of nulls
        sub = subdf.apply(lambda x: x.dropna(), axis=1).to_dict(orient="records")
        sub = [{k: v for k, v in s.items() if v == v} for s in sub]
        for s in sub:
            if 'publish_date' in s:
                s['publish_date'] = int(s['publish_date'])
        dis_d[mirna_name] = sub
    return dis_d


def load_mi2Rdisease(d_dis):
    col_names = ['mirna_name', 'disease_name', 'regulate', 'origin_source', 'year', 'description']
    f = open(miR2disease_path, "r", encoding='utf-8', errors='ignore')
    df = pd.read_csv(f, sep='\t', comment="#", header=None, names=col_names)
    # df['year'] = df['year'].dropna().astype(int)
    df['year'] = df['year'].dropna().map('{:.0f}'.format)
    miR_targets = load_mi2Rtarget()
    # get rid of nulls
    sub = df.apply(lambda x: x.dropna(), axis=1).to_dict(orient="records")
    sub = [{k: v for k, v in s.items() if v == v} for s in sub]
    print(len(sub))
    for record in sub:
        record['map_id'] = 'for the mapping disease unique id'
        record['source'] = 'miR2disease'
        # print(record)
        if 'year' in record:
            record['year'] = int(record['year'])
        if 'disease_name' in record and record['disease_name'].lower() in d_dis:
            record['disease_id'] = d_dis[record['disease_name'].lower()]
        if 'mirna_name' in record and record['mirna_name'] in miR_targets:
            record['mirna_target'] = miR_targets[record['mirna_name']]
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
    dis_d = load_disease_list(bio_client.biodis.disease_no_umls.find({}))
    print(len(dis_d))
    d_hdmm = load_hdmm(dis_d)
    d_mi2Rdisease = load_mi2Rdisease(dis_d)
    bio_client.biodis.mirna.insert_many(d_hdmm)
    bio_client.biodis.mirna.insert_many(d_mi2Rdisease)