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


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.110:27017/biodis')

    # drug relations
    store_py_drug(bio_client.biodis.drug)
    store_ndfrt_drug(src_client.src_disease.ndfrt.find({}), bio_client.biodis.drug)
    store_kegg_drug(src_client.src_disease.kegg.find({}), bio_client.biodis.drug)

    print("success")