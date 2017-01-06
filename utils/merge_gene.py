__author__ = 'kayzhao'

from pymongo import MongoClient
import pandas as pd
from databuild.disgenet import *


def format_ctd_gene_data(docs, collection):
    num = 0
    for doc in docs:
        num += 1
        print(num)
        if doc['source'] == 'The Comparative Toxicogenomics Database':
            if "gene_id" in doc:
                # print(doc['disease_id'], doc['gene_id'])
                gene_id = str(doc['gene_id'])
                collection.update_one({'_id': doc['_id']}, {'$set': {"gene_id": gene_id}}, upsert=True)

            continue

            if "omim" in doc:
                print(doc['_id'])
                omims = []
                for x in doc['omim']:
                    omims.append(x[:x.find('.')])
                # doc['omim'] = omims
                print(omims)
                collection.update_one({'_id': doc['_id']}, {'$set': {"omim": omims}}, upsert=True)
        else:
            break


def process_disgenet_gene(file_path_gene_disease, db):
    df = pd.read_csv(file_path_gene_disease, sep='\t', comment='#', compression='gzip')
    rename = {'diseaseId': 'disease_id',
              'diseaseName': 'disease_name',
              'geneId': 'gene_id',
              'geneName': 'gene_name',
              'sourceId': 'origin_source',
              'NofPmids': 'num_of_pmids',
              'NofSnps': 'num_of_snps'}

    df.sourceId = df.sourceId.str.split(",")
    df = df.rename(columns=rename)
    d = []
    for did, subdf in df.groupby("disease_id"):
        # gene_id to string
        subdf['gene_id'] = subdf['gene_id'].apply(lambda x: str(x))
        # subdf['gene_id'] = subdf['gene_id'].astype(str)
        records = subdf.to_dict(orient='records')
        records = [{k: v for k, v in record.items() if k not in {'disease_id'}} for record in records]
        for s in records:
            dic = dict()
            dic['disease_id'] = did.replace("umls", "UMLS_CUI")
            for k, v in s.items():
                dic[k] = v
            dic['source'] = 'DisGeNET'
            # print(dic)
            d.append(dic)
            # db.update_one({'disease_id': dic['disease_id'],
            # 'gene_id': dic['gene_id'],
            # 'gene_name': dic['gene_name']},
            # {'$set': dic}, upsert=True)

    # insert
    db.insert_many(d)


def process_kegg_relations(docs, db):
    print("kegg disease")
    genes_d = []
    for doc in docs:
        print(doc['_id'])
        if "genes" in doc:
            for x in doc['genes']:
                dic = dict()
                dic['disease_id'] = doc['_id']
                dic['gene_name'] = x['name']
                dic['source'] = "(KEGG) Kyoto Encyclopedia of Genes and Genomes"
                if 'ref' in x:
                    dic['gene_ref'] = x['ref']
                dic['type'] = 'Gene'
                genes_d.append(dic)

        if "markers" in doc:
            for x in doc['markers']:
                dic = dict()
                dic['disease_id'] = doc['_id']
                dic['gene_name'] = x['name']
                dic['source'] = "(KEGG) Kyoto Encyclopedia of Genes and Genomes"
                if 'ref' in x:
                    dic['gene_ref'] = x['ref']
                dic['type'] = 'Marker'
                genes_d.append(dic)
    # insert
    db.insert_many(genes_d)


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.110:27017/biodis')

    # # Disgenet genes
    process_disgenet_gene(file_path_gene_disease, bio_client.biodis.gene)
    #
    # # kegg gene
    # process_kegg_relations(src_client.src_disease.kegg.find({}), bio_client.biodis.gene)

    # format the gene data
    # format_ctd_gene_data(bio_client.biodis.gene.find({}), bio_client.biodis.gene)

    print("success")