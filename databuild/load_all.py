# Merge
import networkx as nx
from config import db_names
from utils.common import dict2list
from pymongo import MongoClient
from tqdm import tqdm


def build_id_graph():
    g = nx.Graph()
    for db_name in db_names:
        db = MongoClient().disease[db_name]
        for doc in db.find({'xref': {'$exists': True}}, {'xref': 1}):
            for xref in dict2list(doc['xref']):
                g.add_edge(doc['_id'], xref)
    return g


def get_equiv_doid(g, did):
    """
    For a given ID, get the DOIDs it is equivalent to within 2 hops.
    """
    if did.startswith("doid:"):
        return [did]
    if did not in g:
        return []
    equiv = list(nx.single_source_shortest_path_length(g, did, cutoff=2).keys())
    return [x for x in equiv if x.startswith("doid:")]


def parse_all():
    from databuild.do import do_parser
    from databuild.disgenet import disgenet_parser
    from databuild.hpo import hpo_parser
    from databuild.mesh import mesh_parser
    from databuild.ctdbase import ctd_parser
    from databuild.orphanet import orphanet_parser
    from databuild.ndfrt import ndfrt_parser
    from databuild.omim import omim_parser

    client = MongoClient('mongodb://zkj1234:zkj1234@192.168.1.113:27017/src_disease')
    do_parser.parse(client.src_disease.do, False)
    disgenet_parser.parse(client.src_disease.disgenet, False)
    hpo_parser.parse(client.src_disease.hpo, False)
    mesh_parser.parse(client.src_disease.mesh, False)
    ctd_parser.parse(client.src_disease.ctd)
    ndfrt_parser.parse(client.src_disease.ndfrt)
    omim_parser.parse(client.src_disease.omim)
    orphanet_parser.parse(client.src_disease.orphanet)


def merge_one(db_name):
    disease = MongoClient().disease.disease
    g = build_id_graph()
    db = MongoClient().disease[db_name]
    if db.count() == 0:
        print("Warning: {} is empty".format(db))
    for doc in db.find():
        doids = get_equiv_doid(g, doc['_id'])
        for doid in doids:
            disease.update_one({'_id': doid}, {'$push': {db_name: doc}}, upsert=True)


def merge(mongo_collection=None, drop=True):
    # # merge docs
    if mongo_collection:
        disease = mongo_collection
    else:
        client = MongoClient()
        disease = client.disease.disease
    if drop:
        disease.drop()

    g = build_id_graph()

    # make initial primary d with all DOID docs
    db = MongoClient().disease.do
    d = [{'_id': doc['_id'], 'do': doc} for doc in db.find()]
    disease.insert_many(d)

    # fill in from other sources
    for db_name in tqdm(set(db_names) - {'do'}):
        print(db_name)
        db = MongoClient().disease[db_name]
        if db.count() == 0:
            print("Warning: {} is empty".format(db))
        for doc in db.find():
            doids = get_equiv_doid(g, doc['_id'])
            for doid in doids:
                disease.update_one({'_id': doid}, {'$push': {db_name: doc}}, upsert=True)


if __name__ == '__main__':
    parse_all()
