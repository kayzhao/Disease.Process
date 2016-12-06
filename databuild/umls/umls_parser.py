__author__ = 'kayzhao'

import re
import pandas as pd
from pymongo import MongoClient
from databuild.umls import *
from utils.common import list2dict


def load_kegg_data():
    col_names = ['_id', 'name', 'description', 'category', 'drugs', 'xref']
    df = pd.read_csv(umls_xref_path, sep="\t", comment='#', names=col_names)
    # print(len(df.to_records()))
    # change to list of dict
    d = []
    id_replace = {"UMLS": "UMLS_CUI",
                  "ICD-10": "ICD10CM",
                  "MeSH": "MESH"}
    for record in df.apply(lambda x: x.dropna().to_dict(), axis=1):
        if '_id' in record:
            record['_id'] = "KEGG:" + record["_id"]
        if 'drugs' in record:
            drugs = []
            for x in re.split(",| ", record['drugs']):
                if len(x) > 0:
                    drugs.append(x)
            record['drugs'] = drugs
        if 'xref' in record:
            xrefs = []
            for x in record['xref'].split(";"):
                source = x.split(":", 1)[0].upper()
                source = id_replace.get(source, source)
                ids = x[x.find(":") + 1:].strip().replace(u'\xa0', u' ')
                for id in re.split(",| ", ids):
                    if len(id) > 0:
                        xrefs.append(source + ":" + id)
            record['xref'] = list2dict(xrefs)
            # print(xrefs)
        records = {k: v for k, v in record.items()}
        print(records)
        d.append(records)
    return d


def parse(mongo_collection=None, drop=True):
    if mongo_collection:
        db = mongo_collection
    else:
        client = MongoClient()
        db = client.disease.mesh
    if drop:
        db.drop()

    print("------------kegg data parsing--------------")
    kegg_disease = load_kegg_data()
    print("load kegg success")
    db.insert_many(kegg_disease)
    print("insert kegg success")
    print("------------kegg data parsed success--------------")


if __name__ == '__main__':
    # parse()
    client = MongoClient('mongodb://kayzhao:zkj1234@192.168.1.119:27017/src_disease')
    parse(client.src_disease.kegg)