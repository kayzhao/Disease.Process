__author__ = 'kayzhao'

from utils.mongo import get_src_conn
from utils.common import *
from config import DATA_SRC_DATABASE
from pymongo import MongoClient


def get_columns_to_keep(relationship: str):
    if relationship in {'GO_BP', 'GO_CC', 'GO_MF'}:
        columns_keep = ['GOID', 'InferenceGeneSymbols']
    elif relationship == "pathways":
        columns_keep = ['PathwayID', 'InferenceGeneSymbol']
    elif relationship == "chemicals":
        columns_keep = ['CasRN', 'ChemicalID', 'DirectEvidence', 'InferenceGeneSymbol', 'InferenceScore', 'OmimIDs',
                        'PubMedIDs']
    elif relationship == "genes":
        columns_keep = ['GeneID', 'DirectEvidence', 'InferenceScore', 'InferenceChemicalName', 'OmimIDs', 'PubMedIDs']
    return columns_keep


columns_rename = {'GOID': 'go',
                  'InferenceGeneSymbols': 'inference_gene_symbols',
                  'PathwayID': 'pathway',
                  'CasRN': 'casrn',
                  'ChemicalID': 'chemical',
                  'DirectEvidence': 'direct_evidence',
                  'InferenceGeneSymbol': 'inference_gene_symbol',
                  'InferenceScore': 'inference_score',
                  'OmimIDs': 'omim',
                  'GeneID': 'gene',
                  'InferenceChemicalName': 'inference_chemical_name',
                  'PubMedIDs': 'pubmed'}


def parse_ctd_genes():
    client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    ctd = client.src_disease.ctd
    ctd_genes = client.src_disease.ctd_genes
    docs = ctd.find()
    for doc in docs:
        genes = list()
        d = dict()
        if 'genes' in doc:
            for g in doc['genes']:
                obj = loadobj(g['filename'], client.src_disease, mode="gridfs")
                columns_keep = get_columns_to_keep('genes')
                for x in obj:
                    print(x)
                    # d['_id'] = doc['_id']
                    # d['genes'] = genes
                    # ctd_genes.insert_many(d)


if __name__ == "__main__":
    parse_ctd_genes()
    print("success")