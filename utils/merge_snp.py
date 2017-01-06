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


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.110:27017/biodis')

    print("Disgenet snps")
    process_disgenet_snp(file_path_snp_disease, bio_client.biodis.snp)

    print("success")