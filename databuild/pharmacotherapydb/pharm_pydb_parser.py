from pymongo import MongoClient
from databuild.pharmacotherapydb import file_path, __METADATA__
from config import DATA_SRC_DATABASE
import pandas as pd

field = __METADATA__['field']


def parse_data():
    col_names = "doid_id	drugbank_id	disease	drug	category	n_curators	n_resources".split("\t")
    df = pd.read_csv(file_path, header=0, sep='\t')
    d = []
    for diseaseID, subdf in df.groupby("doid_id"):
        del subdf['doid_id']
        del subdf['disease']
        sub = subdf.to_dict(orient="records")
        d.append({'_id': diseaseID, 'indications': sub})
    return d


def parse(mongo_collection=None, drop=True):
    if mongo_collection:
        db = mongo_collection
    else:
        client = MongoClient()
        db = client[DATA_SRC_DATABASE][field]
    if drop:
        db.drop()

    print("------------pharmacotherapydb data parsing--------------")
    d = parse_data()
    db.insert_many(d)
    print("insert pharmacotherapydb data success")
    print("------------pharmacotherapydb data parsed success--------------")


if __name__ == '__main__':
    client = MongoClient('mongodb://kayzhao:zkj1234@192.168.1.119:27017/src_disease')
    # client = MongoClient('mongodb://zkj1234:zkj1234@192.168.1.113:27017/src_disease')
    parse(client[DATA_SRC_DATABASE][field])