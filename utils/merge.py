__author__ = 'kayzhao'

import networkx as nx
from collections import Counter
from config import db_names
from utils.mongo import get_src_conn
from collections import defaultdict
from utils.common import dict2list
from config import DATA_SRC_DATABASE


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
        db = get_src_conn().src_disease[db_name]
        all_ids.update(set([x['_id'] for x in db.find({}, {'_id': 1})]))

    for id in all_ids:
        if id.startswith("DOID:"):
            continue
        if id not in g:
            continue
        neighbors = list(nx.single_source_shortest_path_length(g, id, cutoff=cutoff).keys())
        pre = [x.split(":")[0] for x in neighbors]
        d[id.split(":")[0]].append(pre.count("DOID"))
    d = dict(d)
    return {k: Counter(v) for k, v in d.items()}


def id_mapping_test():
    g = build_did_graph()
    print("build id graph success")

    print("cuttoff is 1")
    for k, v in num_doids_in_sg(g, 1).items():
        print(k, v)

    print("cuttoff is 2")
    for k, v in num_doids_in_sg(g, 2).items():
        print(k, v)


if __name__ == "__main__":
    # get_db_info()
    # get_ids_info()
    id_mapping_test()

    g, db_xrefs = build_id_graph()
    for k, v in db_xrefs.items():
        print(k, v)

    # nx.write_edgelist(g, "C:/Users/Administrator/Desktop/id_xrefs.txt")
    # nx.write_adjlist(g, "C:/Users/Administrator/Desktop/ids.txt")

    ID = "MESH:D010211"
    for i in range(1, 5, 1):
        ids = get_equiv_doid(g, ID, i)
        print("%d \t %s" % (i, len(ids)))
        for x in ids:
            print(x)
    print("success")