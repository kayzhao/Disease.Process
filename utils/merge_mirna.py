__author__ = 'kayzhao'

from pymongo import MongoClient
from databuild.pydb import file_path, __METADATA__
from config import DATA_SRC_DATABASE
import pandas as pd
from databuild.disgenet import *


def load_disease_list(client):
    disease_dict = dict()
    disease_all = client.biodis.disease_all
    docs = disease_all.find({"_id": {'$regex': "^UMLS_CUI"}})
    for doc in docs:
        if 'synonym' in doc:
            for x in doc['synonym']:
                if x.endswith('(ENG)'):
                    x = x[:x.find('(ENG)')]
                    disease_dict[x.lower()] = doc['_id']
                    # print(len(dis_d))
    return disease_dict


def umls_cui_field(client):
    mirna = client.biodis.mirna
    docs = mirna.find()
    disease_dict = load_disease_list(client)
    for doc in docs:
        if 'disease_name' in doc:
            name = doc['disease_name'].lower()
            print(name)
            if name in disease_dict:
                mirna.update_one({'_id': doc['_id']}, {'$set': {"umls_cui": disease_dict[name]}}, upsert=True)
    return disease_dict


def doid_field(client):
    disease_dict = dict()
    disease_all = client.biodis.disease_all
    docs = disease_all.find({"_id": {'$regex': "^DOID"}})
    for doc in docs:
        if 'name' in doc:
            disease = doc['name'].lower()
            if disease.endswith(')'):
                disease = disease[:disease.find('(')]
            disease = disease.strip(' ')
            disease_dict[disease] = doc['_id']
        if 'synonym' in doc:
            for x in doc['synonym']:
                disease = x.lower()
                if disease.endswith(')'):
                    disease = disease[:disease.find('(')]
                disease = disease.strip(' ')
                disease_dict[disease] = doc['_id']
                # print(len(dis_d))

    mirna = client.biodis.mirna
    docs = mirna.find()
    for doc in docs:
        if 'disease_name' in doc:
            name = doc['disease_name'].lower()
            if name.endswith(')'):
                name = name[:name.find('(')]
            name = name.strip(' ')
            if name in disease_dict:
                mirna.update_one({'_id': doc['_id']}, {'$set': {"doid": disease_dict[name]}}, upsert=True)
        if 'disease_id' in doc:
            mirna.update_one({'_id': doc['_id']}, {'$set': {"doid": doc['disease_id']}}, upsert=True)
    return disease_dict


def add_umls_cui(client):
    mirna = client.biodis.mirna
    dismap = client.biodis.dismap
    did2umls = client.biodis.did2umls

    docs = mirna.find({}, no_cursor_timeout=True)
    for doc in docs:
        if "doid" in doc:
            print("Disease ID = " + doc['doid'])
            doc_id = doc['_id']
            if doc['doid'].startswith("UMLS_CUI"):
                mirna.update_one({'_id': doc_id}, {'$set': {"umls_cui": doc['doid']}}, upsert=True)
            else:
                map_doc = did2umls.find_one({'_id': doc['doid']})
                if map_doc is not None and 'umls_cui' in map_doc:
                    for x in map_doc['umls_cui']:
                        new_doc = doc.copy()
                        del new_doc['_id']
                        new_doc['umls_cui'] = x
                        mirna.insert_one(new_doc)
                    mirna.remove({"_id": doc_id})
                else:
                    map_doc = dismap.find_one({'_id': doc['doid']})
                    if map_doc is not None and 'umls_cui' in map_doc:
                        for x in map_doc['umls_cui']:
                            new_doc = doc.copy()
                            del new_doc['_id']
                            new_doc['umls_cui'] = x
                            mirna.insert_one(new_doc)
                        mirna.remove({"_id": doc_id})


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
    # load_disease_list(bio_client)
    # umls_cui_field(bio_client)
    # doid_field(bio_client)
    print("success")
