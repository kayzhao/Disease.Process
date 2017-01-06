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
    client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    # client = MongoClient('mongodb://192.168.1.110:27017/biodis')
    # do_parser.parse(client.src_disease.do, False)
    # hpo_parser.parse(client.src_disease.hpo, False)
    kegg_parser.parse(client.src_disease.kegg_new, True)
    # efo_parser.parse(client.src_disease.efo, False)
    # omim_parser.parse(client.src_disease.omim, False)

    # disgenet_parser.parse(client.src_disease.disgenet, False)
    # mesh_parser.parse(client.src_disease.mesh, False)
    # pydb_parser.parse(client.src_disease.pydb, False)
    # ndfrt_parser.parse(client.src_disease.ndfrt, True)
    # orphanet_parser.parse(client.src_disease.orphanet, True)

    # large data
    # umls_parser.parse(client.src_disease.umls, False)
    # ctd_parser.parse(client.biodis, client.biodis.chemical, True)


if __name__ == '__main__':
    parse_all()
