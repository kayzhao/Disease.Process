# Merge
import networkx as nx
from config import db_names
from utils.common import dict2list
from pymongo import MongoClient
from tqdm import tqdm
from utils.mapping import *


def parse_all():
    '''
    parse all database
    :return:
    '''
    from databuild.do import do_parser
    from databuild.disgenet import disgenet_parser
    from databuild.hpo import hpo_parser
    from databuild.mesh import mesh_parser
    from databuild.ctdbase import ctd_parser
    from databuild.orphanet import orphanet_parser
    from databuild.ndfrt import ndfrt_parser
    from databuild.omim import omim_parser
    from databuild.kegg import kegg_parser
    from databuild.efo import efo_parser
    from databuild.umls import umls_parser
    from databuild.pydb import pydb_parser

    # the client for mongodb database
    # client = MongoClient('mongodb://zkj1234:zkj1234@192.168.1.113:27017/src_disease')
    client = MongoClient('mongodb://192.168.1.110:27017/src_disease')
    # do_parser.parse(client.src_disease.do, False)
    # hpo_parser.parse(client.src_disease.hpo, False)
    # kegg_parser.parse(client.src_disease.kegg, False)
    # efo_parser.parse(client.src_disease.efo, False)
    # omim_parser.parse(client.src_disease.omim, False)

    # disgenet_parser.parse(client.src_disease.disgenet, False)
    # mesh_parser.parse(client.src_disease.mesh, False)
    # pydb_parser.parse(client.src_disease.pydb, False)
    ndfrt_parser.parse(client.src_disease.ndfrt, True)
    # orphanet_parser.parse(client.src_disease.orphanet, False)

    # large data
    # umls_parser.parse(client.src_disease.umls, False)
    # ctd_parser.parse(client.src_disease, client.src_disease.ctd, False)


def merge_one(db_name):
    disease = MongoClient().disease.disease
    g = build_did_graph()
    db = MongoClient().disease[db_name]
    if db.count() == 0:
        print("Warning: {} is empty".format(db))
    for doc in db.find():
        doids = get_equiv_dtype_id(g, doc['_id'])
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

    g = build_did_graph()

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
            doids = get_equiv_dtype_id(g, doc['_id'])
            for doid in doids:
                disease.update_one({'_id': doid}, {'$push': {db_name: doc}}, upsert=True)


if __name__ == '__main__':
    parse_all()
