__author__ = 'kayzhao'
from pymongo import MongoClient


def add_umls_cui(client, collection):
    dismap = client.biodis.dismap
    did2umls = client.biodis.did2umls

    docs = collection.find({}, no_cursor_timeout=True)
    for doc in docs:
        if "disease_id" in doc:
            if "umls_cui" in doc:
                break
            doc_id = doc['_id']
            if doc['disease_id'].startswith("UMLS_CUI"):
                collection.update_one({'_id': doc_id}, {'$set': {"umls_cui": doc['disease_id']}}, upsert=True)
            else:
                map_doc = did2umls.find_one({'_id': doc['disease_id']})
                if map_doc is not None and 'umls_cui' in map_doc:
                    for x in map_doc['umls_cui']:
                        new_doc = doc.copy()
                        del new_doc['_id']
                        new_doc['umls_cui'] = x
                        collection.insert_one(new_doc)
                    collection.remove({"_id": doc_id})
                else:
                    map_doc = dismap.find_one({'_id': doc['disease_id']})
                    if map_doc is not None and 'umls_cui' in map_doc:
                        for x in map_doc['umls_cui']:
                            new_doc = doc.copy()
                            del new_doc['_id']
                            new_doc['umls_cui'] = x
                            collection.insert_one(new_doc)
                        collection.remove({"_id": doc_id})


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.113:27017/biodis')
    local_client = MongoClient('mongodb://kayzhao:kayzhao@127.0.0.1:27017/biodis')

    collections = ['go', 'gene', 'drug', 'chemical']
    for x in collections:
        print('collection = ' + x)
        add_umls_cui(local_client, local_client.biodis[x])
    print("success")
