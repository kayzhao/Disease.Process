__author__ = 'kayzhao'

import re
import pandas as pd
from pymongo import MongoClient
from databuild.kegg import *
from utils.common import list2dict
import bson


def load_kegg_data():
    # ID	Name	Description	Category	Gene	Drug	Marker	Reference	Other DBs
    col_names = ['_id', 'name', 'description', 'category', 'genes', 'drugs', 'markers', 'reference', 'xref']
    df = pd.read_csv(diseases_path, sep="\t", comment='#', names=col_names)
    # change to list of dict
    d = []
    id_replace = {"UMLS": "UMLS_CUI",
                  "ICD-10": "ICD10CM",
                  "MeSH": "MESH"}
    df['_id'] = df['_id'].apply(lambda x: "KEGG:" + x)
    for record in df.apply(lambda x: x.dropna().to_dict(), axis=1):
        # if '_id' in record:
        # record['_id'] = "KEGG:" + record["_id"]

        if 'drugs' in record:
            drugs = []
            for x in re.split(",| ", record['drugs']):
                if len(x) > 0:
                    drugs.append(x)
            record['drugs'] = drugs

        if 'genes' in record:
            genes = []
            for x in re.split("\\|", record['genes']):
                if len(x) > 0:
                    x = x.strip()
                    if 'HSA:' in x:
                        gene_name = x[:x.find("HSA:")].strip().strip(',')
                        gene_refs = re.split(" |[,;]", x[x.find("HSA:"):])
                        gene_refs = [x for x in gene_refs if len(x) > 0]
                        genes.append({
                            'gene_name': gene_name,
                            'gene_ref': list2dict(gene_refs)
                        })
                    else:
                        gene_name = x.strip().strip(',')
                        genes.append({
                            'gene_name': gene_name,
                        })
            # print(genes)
            record['genes'] = genes

        if 'markers' in record:
            markers = []
            for x in re.split("\\|", record['markers']):
                if len(x) > 0:
                    x = x.strip()
                    if 'HSA:' in x:
                        gene_name = x[:x.find("HSA:")].strip().strip(',')
                        gene_refs = re.split(" |[,;]", x[x.find("HSA:"):])
                        gene_refs = [x for x in gene_refs if len(x) > 0]
                        markers.append({
                            'gene_name': gene_name,
                            'gene_ref': list2dict(gene_refs)
                        })
                    else:
                        gene_name = x.strip().strip(',')
                        markers.append({
                            'gene_name': gene_name,
                        })
            # print(markers)
            record['markers'] = markers

        if 'reference' in record:
            reference = []
            for x in re.split(",| ", record['reference']):
                if len(x) > 0:
                    reference.append(x)
            record['reference'] = list2dict(reference)

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

        one_doc = {str(k): v for k, v in record.items() if k is not None}
        print(one_doc)
        d.append(one_doc)

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
    # client = MongoClient('mongodb://kayzhao:zkj1234@192.168.1.100:27017/src_disease')
    # parse(client.src_disease.kegg)
    load_kegg_data()