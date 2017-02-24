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
                        new_doc = doc
                        del new_doc['_id']
                        new_doc['umls_cui'] = x
                        chemical.insert_one(new_doc)
                    chemical.remove({"_id": doc_id}, safe=True)


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.113:27017/biodis')
    local_client = MongoClient('mongodb://kayzhao:kayzhao@127.0.0.1:27017/biodis')

    # process_ctd_chemical(bio_client.biodis.chemical)
    add_umls_cui(local_client)
    print("success")
