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
from utils.common import dict2list
from pymongo import MongoClient
from pymongo import MongoClient
from databuild.pydb import file_path, __METADATA__
from config import DATA_SRC_DATABASE
import pandas as pd


def get_ids_info():
    print("get the db disease ids statistics ")
    all_ids = set()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        all_ids.update(set([x['_id'] for x in db.find({}, {'_id': 1})]))

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


def store_drug():
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
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.110:27017/biodis')
    bio_client.biodis.drug.insert_many(d)


if __name__ == "__main__":
    # get_db_info()
    # get_ids_info()
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

    store_drug()

    print("success")