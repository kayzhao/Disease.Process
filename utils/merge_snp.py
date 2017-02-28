__author__ = 'kayzhao'

from pymongo import MongoClient
from databuild.pydb import file_path, __METADATA__
from config import DATA_SRC_DATABASE
import pandas as pd
from databuild.disgenet import *


def process_disgenet_snp(file_path_snp_disease, db):
    df = pd.read_csv(file_path_snp_disease, sep='\t', comment='#', compression='gzip')
    rename = {'diseaseId': 'disease_id',
              'geneId': 'gene_id',
              'geneSymbol': 'gene_symbol',
              'diseaseName': 'disease_name',
              'pubmedId': 'pubmed',
              'snpId': 'rsid',
              'ALT': 'alt',
              'CHROMOSOME': "chromosome",
              'POS': 'pos',
              'REF': 'ref',
              'sourceId': 'source',
              'sentence': 'description'}

    df = df.rename(columns=rename)
    del df['geneSymbol_dbSNP']

    d = []
    for did, subdf in df.groupby("disease_id"):
        records = list(subdf.apply(lambda x: x.dropna().to_dict(), axis=1))
        for record in records:
            if 'year' in record:
                record['year'] = int(record['year'])
            if 'pubmed' in record:
                record['pubmed'] = int(record['pubmed'])
        records = [{k: v for k, v in record.items() if k not in {'disease_id'}} for record in records]
        for s in records:
            dic = dict()
            dic['disease_id'] = did.replace("umls", "UMLS_CUI")
            for k, v in s.items():
                dic[k] = v
            dic['source'] = 'DisGeNET'
            # print(dic)
            d.append(dic)

    db.insert_many(d)


def add_umls_cui(client):
    snp = client.biodis.snp
    dismap = client.biodis.dismap
    did2umls = client.biodis.did2umls

    docs = snp.find({}, no_cursor_timeout=True)
    for doc in docs:
        if "disease_id" in doc:
            print("Disease ID = " + doc['disease_id'])
            doc_id = doc['_id']
            if doc['disease_id'].startswith("UMLS_CUI"):
                snp.update_one({'_id': doc_id}, {'$set': {"umls_cui": doc['disease_id']}}, upsert=True)
            else:
                map_doc = did2umls.find_one({'_id': doc['disease_id']})
                if map_doc is None and 'umls_cui' in map_doc:
                    for x in map_doc['umls_cui']:
                        new_doc = doc.copy()
                        del new_doc['_id']
                        new_doc['umls_cui'] = x
                        snp.insert_one(new_doc)
                    snp.remove({"_id": doc_id})
                else:
                    map_doc = dismap.find_one({'_id': doc['disease_id']})
                    if map_doc is not None and 'umls_cui' in map_doc:
                        for x in map_doc['umls_cui']:
                            new_doc = doc.copy()
                            del new_doc['_id']
                            new_doc['umls_cui'] = x
                            snp.insert_one(new_doc)
                        snp.remove({"_id": doc_id})

def write_disease_ids(client):
    snp = client.biodis.snp
    dismap = client.biodis.dismap
    did2umls = client.biodis.did2umls

    docs = snp.find({}, no_cursor_timeout=True)
    path = "D:/disease/association/snp.txt"
    dpath = "D:/disease/association/diseases_snp.txt"
    f = open(path, 'a', encoding='utf-8')
    fd = open(dpath, 'a', encoding='utf-8')
    s = set()
    umls_s = set()
    for doc in docs:
        if "disease_id" in doc:
            print("Disease ID = " + doc['disease_id'])
            if doc['disease_id'] in s:
                continue
            if doc['disease_id'].startswith("UMLS_CUI"):
                f.write('{}\n'.format(doc['disease_id']))
                umls_s.update([doc['disease_id']])
            else:
                map_doc = did2umls.find_one({'_id': doc['disease_id']})
                if map_doc is not None and 'umls_cui' in map_doc:
                    f.write('{}\t{}\n'.format(doc['disease_id'], map_doc['umls_cui']))
                    umls_s.update(map_doc['umls_cui'])
                else:
                    map_doc = dismap.find_one({'_id': doc['disease_id']})
                    if map_doc is not None and 'umls_cui' in map_doc:
                        f.write('{}\t{}\n'.format(doc['disease_id'], map_doc['umls_cui']))
                        umls_s.update(map_doc['umls_cui'])
                    else:
                        f.write('{}\n'.format(doc['disease_id']))
            s.update([doc['disease_id']])

    # write umls_cui list
    for x in umls_s:
        fd.write('{}\n'.format(x))
    fd.close()
    f.close()


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.113:27017/biodis')

    # print("Disgenet snps")
    # process_disgenet_snp(file_path_snp_disease, bio_client.biodis.snp)
    add_umls_cui(bio_client)
    # write_disease_ids(bio_client)
    print("success")