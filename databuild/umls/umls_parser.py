__author__ = 'kayzhao'

import pandas as pd
from pymongo import MongoClient
from databuild.umls import *
from tqdm import tqdm
import re


def load_mrrel_rrf(db):
    '''
    column name from:
    https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.related_concepts_file_mrrel_rrf/?report=objectonly
    load the UMLS_CUI MRREL data(total records = 70587165)
    :return:
    '''
    col_names = ['CUI1', 'AUI1', 'STYPE1',
                 'REL',  # Relationship of second concept or atom to first concept or atom
                 'CUI2', 'AUI2', 'STYPE2',
                 'RELA',
                 'RUI', 'SRUI', 'SAB', 'SL', 'RG', 'DIR', 'SUPPRESS', 'CVF', 'NONE']

    columns_rename = {
        'CUI1': "_id",
        'AUI1': "aui_1",
        'STYPE1': "stype_1",
        'REL': 'relationship',
        'CUI2': "umls_cui",
        'AUI2': "aui_2",
        'STYPE2': "stype_2",
        'RELA': "additional_label",  # additional_relationship_label
        'RUI': "rui",
        'SRUI': "source_id",
        'SAB': "source_name",
        'SL': "source_relationship",  # source_relationship
        'RG': "relationship_group",
        'DIR': "source_direct",  # source_directionality_flag
        'SUPPRESS': "suppress",  # suppressible_flag
        'CVF': "cvf"  #content_view_flag
    }
    chunksize = 100000

    f = open(umls_mrrel_path, encoding='utf-8')
    total_len = 0
    for df in tqdm(
            pd.read_csv(f, sep='\\|', engine='python', comment="#", header=None, chunksize=chunksize,
                        names=col_names), total=49867785 / chunksize):
        '''
        records statics
        '''
        total_len += len(df.to_records())
        print(len(df.to_records()), total_len)
        del df['NONE']
        # print(df.head(3))

        columns_keep = ['CUI1', 'AUI1', 'STYPE1', 'REL', 'CUI2', 'AUI2', 'STYPE2',
                        'RELA', 'RUI', 'SRUI', 'SAB', 'SL', 'RG']
        df = df.filter(items=columns_keep)
        id_replace = {
            "UMLS": "UMLS_CUI",
            "ICD-9": "ICD9CM",
            "ICD9": "ICD9CM",
            "ICD10": "ICD10CM",
            "ICD-10": "ICD10CM",
            "MeSH": "MESH",
            "MSH": "MESH",
            "ORDO": "ORPHANET",
            "SNOMEDCT_US": "SNOMEDCT",
            "SNOMEDCT_US_2016_03_01": "SNOMEDCT",
            "SNOMEDCT_US_2015_03_01": "SNOMEDCT",
            "HPO": "HP"
        }
        # update the data
        for cui, subdf in df.groupby("CUI1"):
            # print(subdf.head(3))
            subdf = subdf.rename(columns=columns_rename)
            subdf['umls_cui'] = subdf['umls_cui'].apply(lambda x: "UMLS_CUI:" + x)
            subdf['source_name'] = subdf['source_name'].apply(lambda x: id_replace.get(x, x))
            sub = subdf.apply(lambda x: x.dropna(), axis=1).to_dict(orient="records")
            sub = [{k: v for k, v in s.items() if v == v} for s in sub]
            for record in sub:
                if 'relationship_group' in record:
                    record['relationship_group'] = int(record['relationship_group'])
            if len(sub) > 0:
                db.update_one({'_id': "UMLS_CUI:" + cui},
                              {'$set': {"relationships": sub}},
                              upsert=True)


def load_mrconso_rrf():
    '''
    load the UMLS_CUI MRREL data
    :return:
    '''


def parse(mongo_collection=None, drop=True):
    if mongo_collection:
        db = mongo_collection
    else:
        client = MongoClient()
        db = client.disease.mesh
    if drop:
        db.drop()

    print("------------umls data parsing--------------")

    print("------------umls mrconso data --------------")
    # load_mrconso_rrf()

    print("------------umls mrrel data --------------")
    load_mrrel_rrf(db)

    print("------------umls data parsed success--------------")


if __name__ == '__main__':
    # parse()
    # load_mrrel_rrf()
    client = MongoClient('mongodb://kayzhao:zkj1234@192.168.1.110:27017/src_disease')
    parse(client[DATA_SRC_DATABASE]['umls'])