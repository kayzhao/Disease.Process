__author__ = 'kayzhao'

import gzip
from pymongo import MongoClient
from databuild.ctdbase import *
from databuild.ctdbase import ctd_parser


def process_ctd_chemical(db):
    for relationship, file_path in relationships.items():
        with gzip.open(os.path.join(DATA_DIR_CTD, file_path), 'rt', encoding='utf-8') as f:
            if relationship == "chemicals":
                print("parsing the  " + relationships + "data")
                ctd_parser.process_chemicals(db, f, relationship)

def write_disease_ids(client):
    chemical = client.biodis.chemical
    dismap = client.biodis.dismap
    did2umls = client.biodis.did2umls

    docs = chemical.find({}, no_cursor_timeout=True)
    path = "D:/disease/association/chemical.txt"
    dpath = "D:/disease/association/diseases.txt"
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
                        f.write('{}\t{}\n'.format(doc['disease_id'],map_doc['umls_cui']))
                        umls_s.update(map_doc['umls_cui'])
                    else:
                        f.write('{}\n'.format(doc['disease_id']))
            s.update([doc['disease_id']])

    # write umls_cui list
    for x in umls_s:
        fd.write('{}\n'.format(x))
    fd.close()
    f.close()

def add_umls_cui(client):
    chemical = client.biodis.chemical
    dismap = client.biodis.dismap
    did2umls = client.biodis.did2umls

    docs = chemical.find({}, no_cursor_timeout=True)
    for doc in docs:
        if "disease_id" in doc:
            print("Disease ID = " + doc['disease_id'])
            doc_id = doc['_id']
            if doc['disease_id'].startswith("UMLS_CUI"):
                chemical.update_one({'_id': doc_id}, {'$set': {"umls_cui": doc['disease_id']}}, upsert=True)
            else:
                map_doc = dismap.find_one({'_id': doc['disease_id']})
                if map_doc is not None and 'umls_cui' in map_doc:
                    for x in map_doc['umls_cui']:
                        new_doc = doc.copy()
                        del new_doc['_id']
                        new_doc['umls_cui'] = x
                        chemical.insert_one(new_doc)
                    chemical.remove({"_id": doc_id}, safe=True)


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.113:27017/biodis')
    local_client = MongoClient('mongodb://kayzhao:kayzhao@127.0.0.1:27017/biodis')

    # process_ctd_chemical(bio_client.biodis.chemical)
    # add_umls_cui(local_client)
    write_disease_ids(bio_client)
    print("success")
