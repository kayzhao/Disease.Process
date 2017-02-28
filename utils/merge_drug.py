__author__ = 'kayzhao'

from pymongo import MongoClient
from databuild.pydb import file_path
import pandas as pd


def store_py_drug(db):
    print("------pydb drug----------")
    col_names = "doid_id	drugbank_id	disease	drug	category	n_curators	n_resources".split(
        "\t")
    df = pd.read_csv(file_path, header=0, sep='\t', names=col_names)
    # df = df.rename(columns=col_names)
    d = []
    columns_rename = {'doid_id': 'disease_id',
                      'disease': "disease_name",
                      'drugbank_id': 'drug_id',
                      'drug': 'drug_name',
                      'category': 'category'}
    for diseaseID, subdf in df.groupby("doid_id"):
        subdf = subdf.rename(columns=columns_rename)
        subdf['drug_id'] = subdf['drug_id'].apply(lambda x: "DrugBank:" + x)
        sub = subdf.to_dict(orient="records")
        sub = [{k: v for k, v in s.items() if v == v} for s in sub]
        for s in sub:
            s['source'] = "PharmacotherapyDB"
            d.append(s)

    db.insert_many(d)


def store_ndfrt_drug(docs, db):
    print("ndfrt --- drug----------")
    d = []
    for doc in docs:
        if "drugs" in doc:
            for x in doc['drugs']:
                x['disease_id'] = "UMLS_CUI:" + x['umls_cui']
                del x['umls_cui']
                x['drug_name'] = x['name']
                del x['name']
                x['source'] = "National Drug File - Reference Terminology (NDF-RT)"
                d.append(x)
    db.insert_many(d)


def store_kegg_drug(docs, db):
    print("kegg disease")
    drug_d = []
    for doc in docs:
        print(doc['_id'])
        if "drugs" in doc:
            for x in doc['drugs']:
                dic = dict()
                dic['disease_id'] = doc['_id']
                dic['drug_name'] = x['name']
                dic['source'] = "(KEGG) Kyoto Encyclopedia of Genes and Genomes"
                if 'ref' in x:
                    if 'DG' in x['ref']:
                        # dic['drug_ref'] = ["KEGG:" + r for r in x['ref']['DG']]
                        dic['drug_id'] = "KEGG:" + x['ref']['DG'][0]
                        dic['drug_id_type'] = "DGroup"
                    if 'DR' in x['ref']:
                        # dic['drug_ref'] = ["KEGG:" + r for r in x['ref']['DR']]
                        dic['drug_id'] = "KEGG:" + x['ref']['DR'][0]
                        dic['drug_id_type'] = "Drug"
                drug_d.append(dic)

    # insert
    db.insert_many(drug_d)


def write_disease_ids(client):
    drug = client.biodis.drug
    dismap = client.biodis.dismap
    did2umls = client.biodis.did2umls

    docs = drug.find({}, no_cursor_timeout=True)
    path = "D:/disease/association/drug.txt"
    dpath = "D:/disease/association/diseases_drug.txt"
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
    drug = client.biodis.drug
    dismap = client.biodis.dismap
    did2umls = client.biodis.did2umls

    docs = drug.find({}, no_cursor_timeout=True)
    for doc in docs:
        if "disease_id" in doc:
            print("Disease ID = " + doc['disease_id'])
            doc_id = doc['_id']
            if doc['disease_id'].startswith("UMLS_CUI"):
                drug.update_one({'_id': doc_id}, {'$set': {"umls_cui": doc['disease_id']}}, upsert=True)
            else:
                map_doc = dismap.find_one({'_id': doc['disease_id']})
                if map_doc is not None and 'umls_cui' in map_doc:
                    for x in map_doc['umls_cui']:
                        new_doc = doc.copy()
                        del new_doc['_id']
                        new_doc['umls_cui'] = x
                        drug.insert_one(new_doc)
                    drug.remove({"_id": doc_id})


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.113:27017/biodis')

    # drug relations
    # store_py_drug(bio_client.biodis.drug)
    # store_ndfrt_drug(src_client.src_disease.ndfrt.find({}), bio_client.biodis.drug)
    # store_kegg_drug(src_client.src_disease.kegg.find({}), bio_client.biodis.drug)
    # add_umls_cui(bio_client)
    write_disease_ids(bio_client)
    print("success")