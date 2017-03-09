from collections import defaultdict
from itertools import chain

import pandas as pd
from pymongo import MongoClient
from databuild.disgenet import *


def process_gene(file_path_gene_disease):
    df = pd.read_csv(file_path_gene_disease, sep='\t', comment='#', compression='gzip')
    rename = {'diseaseId': '_id',
              'geneId': 'gene_id',
              'geneName': 'gene_name',
              'diseaseName': 'label',
              'sourceId': 'source',
              'NofPmids': '#pmids',
              'NofSnps': '#snps'}

    df.sourceId = df.sourceId.str.split(",")
    df = df.rename(columns=rename)
    d = []
    for did, subdf in df.groupby("_id"):
        records = subdf.to_dict(orient='records')
        records = [{k: v for k, v in record.items() if k not in {'_id', 'label', 'description'}} for record in records]
        drecord = {'_id': did.replace("umls", "UMLS_CUI"), 'genes': records}
        d.append(drecord)
    return {x['_id']: x for x in d}


def process_snp(file_path_snp_disease):
    df = pd.read_csv(file_path_snp_disease, sep='\t', comment='#', compression='gzip')
    rename = {'diseaseId': '_id',
              'geneId': 'gene_id',
              'geneSymbol': 'gene_symbol',
              'diseaseName': 'label',
              'pubmedId': 'pubmed',
              'snpId': 'rsid',
              'ALT': 'alt',
              'CHROMOSOME': "chr",
              'POS': 'pos',
              'REF': 'ref',
              'sourceId': 'source',
              'sentence': 'description'}

    df = df.rename(columns=rename)
    del df['geneSymbol_dbSNP']

    d = []
    for did, subdf in df.groupby("_id"):
        records = list(subdf.apply(lambda x: x.dropna().to_dict(), axis=1))
        for record in records:
            if 'year' in record:
                record['year'] = int(record['year'])
            if 'pubmed' in record:
                record['pubmed'] = int(record['pubmed'])
        records = [{k: v for k, v in record.items() if k not in {'_id', 'label'}} for record in records]
        drecord = {'_id': did.replace("umls", "UMLS_CUI"), 'snps': records}
        d.append(drecord)
    return {x['_id']: x for x in d}


def parse(mongo_collection=None, drop=True):
    if mongo_collection:
        db = mongo_collection
    else:
        client = MongoClient()
        db = client.disease.disgenet
    if drop:
        db.drop()

    print("------------disgenet data parsing--------------")
    d_gene = process_gene(file_path_gene_disease)
    print("load gene dict success")
    d_snp = process_snp(file_path_snp_disease)
    print("load snp dict success")

    # print(len(d_gene))
    # print(len(d_snp))

    d = defaultdict(dict)
    for key in set(chain(*[list(d_gene.keys()), list(d_snp.keys())])):
        # merge two dict
        d[key] = dict(d_gene.get(key, {}), **d_snp.get(key, {}))

    # print(len(d))
    print("build dict within gene and snp together")

    # db.insert_many(d.values())
    db.insert_many(list(d.values()))
    print("insert into mongodb success")
    print("------------disgenet data parsed success--------------")
