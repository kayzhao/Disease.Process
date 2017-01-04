__author__ = 'kayzhao'

import networkx as nx
from collections import Counter
from config import db_names
from utils.mongo import get_src_conn
from collections import defaultdict
from utils.common import dict2list
from config import DATA_SRC_DATABASE
import networkx as nx
from config import db_names
from utils.common import dict2list, list2dict
from pymongo import MongoClient
from pymongo import MongoClient
from databuild.pydb import file_path, __METADATA__
from config import DATA_SRC_DATABASE
import pandas as pd
from databuild.disgenet import *
import re


def get_ids_info(db):
    print("get the db disease ids statistics ")
    all_ids = set()
    print("get the db disease all_ids set ")
    all_ids.update(set([x['_id'] for x in db.find({}, {'_id': 1})]))
    print(len(all_ids))
    for k, v in Counter([x.split(":", 1)[0] for x in all_ids]).items():
        print("%s \t %d" % (k, v))


def get_db_info():
    print("get the db disease statistics ")
    all_ids = set()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        all_ids.update(set([db_name + ":" + x['_id'] for x in db.find({}, {'_id': 1})]))

    for k, v in Counter([x.split(":", 1)[0] for x in all_ids]).items():
        print("%s \t %d" % (k, v))


def get_equiv_doid(g, did, cutoff=2):
    """
    For a given ID, get the DOIDs it is equivalent to within 2 hops.
    """
    if did.startswith("DOID:"):
        return [did]
    if did not in g:
        return []
    equiv = list(nx.single_source_shortest_path_length(g, did, cutoff=cutoff).keys())
    return [x for x in equiv if x.startswith("DOID:")]


def get_equiv_umlsid(g, did, cutoff=2):
    """
    For a given ID, get the DOIDs it is equivalent to within 2 hops.
    """
    if did.startswith("UMLS"):
        return [did]
    if did not in g:
        return []
    equiv = list(nx.single_source_shortest_path_length(g, did, cutoff=cutoff).keys())
    return [x for x in equiv if x.startswith("UMLS:")]


def build_id_graph():
    g = nx.Graph()
    db_xrefs = defaultdict()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        docs = db.find({'xref': {'$exists': True}}, {'xref': 1})
        all_id_types = set()
        # get the xref docs count
        if docs.count() > 0:
            # print("%s \t %d" % (db_name, docs.count()))
            for doc in docs:
                for xref in dict2list(doc['xref']):
                    g.add_edge(doc['_id'].upper(), xref.upper())
                    all_id_types.add(xref.upper().split(":", 1)[0])
            db_xrefs[db_name] = list(all_id_types)
    return g, db_xrefs


def get_db_xrefs():
    print("get the db xref statistics ")
    '''
    get the db xref records
    :return: d the xref data
    '''
    d = defaultdict()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        docs = db.find({'xref': {'$exists': True}}, {'xref': 1})
        # get the xref docs count
        if docs.count() > 0:
            print("%s \t %d" % (db_name, docs.count()))
            for doc in docs:
                d[doc['_id']] = dict2list(doc['xref'])
    return d


def build_did_graph():
    '''
    create the id xref graph
    :return:id graph
    '''
    g = nx.Graph()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        docs = db.find({'xref': {'$exists': True}}, {'xref': 1})
        # get the xref docs count
        if docs.count() > 0:
            # print("%s \t %d" % (db_name, docs.count()))
            for doc in docs:
                for xref in dict2list(doc['xref']):
                    g.add_edge(doc['_id'].upper(), xref.upper())
    return g


def num_doids_in_sg(g, cutoff):
    d = defaultdict(list)
    all_ids = set()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        all_ids.update(set([x['_id'] for x in db.find({}, {'_id': 1})]))

    for id in all_ids:
        if id.startswith("DOID"):
            continue
        if id not in g:
            # no xref data
            d[id.split(":")[0]].append(0)
            continue
        neighbors = list(nx.single_source_shortest_path_length(g, id, cutoff=cutoff).keys())
        pre = [x.split(":")[0] for x in neighbors]
        d[id.split(":")[0]].append(pre.count("DOID"))
    d = dict(d)

    return {k: Counter(v) for k, v in d.items()}


def num_umlsids_in_sg(g, cutoff):
    d = defaultdict(list)
    all_ids = set()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        all_ids.update(set([x['_id'] for x in db.find({}, {'_id': 1})]))

    for id in all_ids:
        if id.startswith("UMLS_CUI"):
            continue
        if id not in g:
            # no xref data
            d[id.split(":")[0]].append(0)
            continue
        neighbors = list(nx.single_source_shortest_path_length(g, id, cutoff=cutoff).keys())
        pre = [x.split(":")[0] for x in neighbors]
        d[id.split(":")[0]].append(pre.count("UMLS_CUI"))
    d = dict(d)
    return {k: Counter(v) for k, v in d.items()}


def doid_mapping_test():
    g = build_did_graph()
    print("build id graph success")
    print("cuttoff is 1")
    for k, v in num_doids_in_sg(g, 1).items():
        num_nomapping = 0
        num_mapping = 0
        for n in v:
            if n > 0:
                num_mapping += v[n]
            else:
                num_nomapping = v[n]
        print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))

    print("cuttoff is 2")
    for k, v in num_doids_in_sg(g, 2).items():
        num_nomapping = 0
        num_mapping = 0
        for n in v:
            if n > 0:
                num_mapping += v[n]
            else:
                num_nomapping = v[n]
        print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))

    print("cuttoff is 3")
    for k, v in num_doids_in_sg(g, 3).items():
        num_nomapping = 0
        num_mapping = 0
        for n in v:
            if n > 0:
                num_mapping += v[n]
            else:
                num_nomapping = v[n]
        print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))


def umlsid_mapping_test():
    g = build_did_graph()
    print("build id graph success")

    print("cuttoff is 1")

    for k, v in num_umlsids_in_sg(g, 1).items():
        num_nomapping = 0
        num_mapping = 0
        for n in v:
            if n > 0:
                num_mapping += v[n]
            else:
                num_nomapping = v[n]
        print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))

    print("cuttoff is 2")
    for k, v in num_umlsids_in_sg(g, 2).items():
        num_nomapping = 0
        num_mapping = 0
        for n in v:
            if n > 0:
                num_mapping += v[n]
            else:
                num_nomapping = v[n]
        print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))

    print("cuttoff is 3")
    for k, v in num_umlsids_in_sg(g, 3).items():
        num_nomapping = 0
        num_mapping = 0
        for n in v:
            if n > 0:
                num_mapping += v[n]
            else:
                num_nomapping = v[n]
        print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))


def retrive_data():
    print("retrive_data")
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://192.168.1.110:27017/biodis')
    bio_client.biodis.genes.insert_many(src_client.src_disease.gene.find({}))
    # for doc in src_client.src_disease.gene.find({}):
    # bio_client.biodis.genes.insert_one(doc)


def store_drug(db):
    print("drug----------")
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
        sub = subdf.to_dict(orient="records")
        sub = [{k: v for k, v in s.items() if v == v} for s in sub]
        for s in sub:
            d.append(s)
    return d


def store_ndfrt_drug(docs, db):
    print("drug----------")
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


def process_disgenet_gene(file_path_gene_disease, db):
    df = pd.read_csv(file_path_gene_disease, sep='\t', comment='#', compression='gzip')
    rename = {'diseaseId': 'disease_id',
              'geneId': 'gene_id',
              'geneName': 'gene_name',
              'diseaseName': 'disease_name',
              'sourceId': 'source',
              'NofPmids': 'num_of_pmids',
              'NofSnps': 'num_of_snps'}

    df.sourceId = df.sourceId.str.split(",")
    df = df.rename(columns=rename)
    d = []
    for did, subdf in df.groupby("disease_id"):
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
    return d


def process_disgenet_snp(file_path_snp_disease):
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
    return d


def process_do(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('Disease Ontology')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['Disease Ontology']

        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_efo(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v

        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('Experimental Factor Ontology')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['Experimental Factor Ontology']

        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_orphanet(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('ORPHANET')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['ORPHANET']

        '''
        only record the relations exist
        '''
        if "phenotypes" in dic:
            del dic['phenotypes']
            dic['phenotypes'] = True
        if "disease_gene_associations" in dic:
            del dic['disease_gene_associations']
            dic['disease_gene_associations'] = True

        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_kegg(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('KEGG')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['KEGG']

        '''
        only record the relations exist
        '''
        if 'reference' in dic:
            if 'xref' in dic:
                dic['xref']['PMID'] = dic['reference']['PMID']
            else:
                dic['xref'] = {'PMID': dic['reference']['PMID']}
            del dic['reference']
        if "genes" in dic:
            del dic['genes']
            dic['genes'] = True
        if "markers" in dic:
            del dic['markers']
            dic['markers'] = True
        if "drugs" in dic:
            del dic["drugs"]
            dic['drugs'] = True
        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_hpo(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('Human Phenotype Ontology')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['Human Phenotype Ontology']
        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_nfdrt(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('National Drug File - Reference Terminology (NDF-RT)')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['National Drug File - Reference Terminology (NDF-RT)']

        '''
        only record the relations exist
        '''
        if "drugs" in dic:
            del dic["drugs"]
            dic['drugs'] = True
        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_mesh(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('MeSH (Medical Subject Headings)')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['MeSH (Medical Subject Headings)']

        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_omim(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('OMIM (Online Mendelian Inheritance in Man)')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['OMIM (Online Mendelian Inheritance in Man)']

        '''
        only record the relations exist
        '''
        if "genes" in dic:
            del dic["genes"]
            dic['genes'] = True
        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_doc_2_disease(docs, db):
    d = []
    for doc in docs:
        print(doc["_id"])
        dic = dict()
        for k, v in doc.items():
            if not isinstance(v, (int, float)) and len(v) != 0:
                dic[k] = v

        db_d = db.find_one({"_id": dic['_id']})
        if db_d:

            # source merge
            l = db_d['source'] + dic['source']
            dic['source'] = list(set(l))

            # xref merge
            if "xref" in dic and "xref" in db_d:
                temp_xref = dict()
                # remove the null xref
                for k, v in dic['xref'].items():
                    if len(v) != 0:
                        temp_xref[k] = v
                dic['xref'] = temp_xref
                # merge the xref list
                l = dict2list(db_d['xref']) + dict2list(dic['xref'])
                dic['xref'] = list2dict(list(set(l)))

            # synonym merge
            if "synonym" in dic and "synonym" in db_d:
                l = [x + '(ENG)' for x in dic['synonym']]
                l = l + db_d['synonym']
                dic['synonym'] = list(set(l))

        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)

    return d


def process_disease_go_relations(type, disease_id_field, docs, db):
    all_ids = set()
    l = ['go_cc', 'go_bp', 'go_mf', 'pathways']
    for x in docs:
        # if set(['go_cc', 'go_bp', 'go_mf', 'pathways']).issubset(x):
        all_ids.update(set([x[disease_id_field]]))
    # all_ids.update(set([x[disease_id_field] for x in docs]))
    print(len(all_ids))
    for id in all_ids:
        print(id)
        db.update_one({'_id': id}, {"$set": {type: True}})


def process_disease_relations(type, disease_id_field, docs, db):
    all_ids = set()
    all_ids.update(set([x[disease_id_field] for x in docs]))
    print(len(all_ids))
    for id in all_ids:
        print(id)
        db.update_one({'_id': id}, {"$set": {type: True}})


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.110:27017/biodis')

    # get_ids_info(bio_client.biodis.disease)
    # get_db_xrefs()
    # doid_mapping_test()
    # umlsid_mapping_test()

    # g = build_did_graph()
    # for k, v in db_xrefs.items():
    # print(k, v)

    # nx.write_edgelist(g, "C:/Users/Administrator/Desktop/id_xrefs.txt")
    # nx.write_adjlist(g, "C:/Users/Administrator/Desktop/ids.txt")

    # ID = "MESH:D010211"
    # for i in range(1, 5, 1):
    # ids = get_equiv_doid(g, ID, i)
    # print("%d \t %s" % (i, len(ids)))
    # for x in ids:
    # print(x)

    # store_drug()
    # store_ndfrt_drug(src_client.src_disease.ndfrt.find({}), bio_client.biodis.drug)

    # print("Disgenet snps and genes")
    # d_gene = process_disgenet_gene(file_path_gene_disease, bio_client.biodis.gene)
    # d_snp = process_disgenet_snp(file_path_snp_disease)
    # bio_client.biodis.snp.insert_many(d_snp)
    # bio_client.biodis.genes.insert_many(d_gene)

    # print("do")
    # process_do(src_client.src_disease.do.find({}), bio_client.biodis.do)

    # print("efo")
    # process_efo(src_client.src_disease.efo.find({}), bio_client.biodis.do)

    # print("orphanet")
    # process_orphanet(src_client.src_disease.orphanet.find({}), bio_client.biodis.do)

    # print("kegg")
    # process_kegg(src_client.src_disease.kegg.find({}), bio_client.biodis.do)

    # print("hpo")
    # process_kegg(src_client.src_disease.hpo.find({}), bio_client.biodis.do)

    # print("ndfrt")
    # process_nfdrt(src_client.src_disease.ndfrt.find({}), bio_client.biodis.do)

    # print("mesh")
    # process_mesh(src_client.src_disease.mesh.find({}), bio_client.biodis.do)

    # print("omim")
    # process_omim(src_client.src_disease.omim.find({}), bio_client.biodis.do)

    # print("merge all to disease")
    # process_doc_2_disease(bio_client.biodis.do.find({}), bio_client.biodis.disease)

    # umls drugs
    # process_disease_relations(
    # "drugs",
    # bio_client.biodis.do.find({"_id": {'$regex': "^UMLS_CUI"}}),
    # bio_client.biodis.disease
    # )

    # the disease associations collection
    # process_disease_relations(
    # "chemicals",
    #     "disease_id",
    #     bio_client.biodis.chemical.find(),
    #     bio_client.biodis.disease
    # )

    # the disease associations collection
    process_disease_go_relations(
        "gos",
        "_id",
        bio_client.biodis.go.find(),
        bio_client.biodis.disease
    )

    print("success")