__author__ = 'kayzhao'

import networkx as nx
from collections import Counter
from config import db_names
from utils.mongo import get_src_conn
from collections import defaultdict
from utils.common import dict2list


def getDBInfo():
    all_ids = set()
    for db_name in db_names:
        db = get_src_conn().disease[db_name]
        all_ids.update(set([x['_id'] for x in db.find({}, {'_id': 1})]))

    for k, v in Counter([x.split(":", 1)[0] for x in all_ids]).items():
        print(k, v)


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


def build_id_graph():
    g = nx.Graph()
    for db_name in db_names:
        db = get_src_conn().disease[db_name]
        for doc in db.find({'xref': {'$exists': True}}, {'xref': 1}):
            for xref in dict2list(doc['xref']):
                g.add_edge(doc['_id'], xref)
    return g


def num_doids_in_sg(g, cutoff):
    d = defaultdict(list)

    all_ids = set()

    for db_name in db_names:
        db = get_src_conn().disease[db_name]
        all_ids.update(set([x['_id'] for x in db.find({}, {'_id': 1})]))

    for id in all_ids:
        if id.startswith("doid:"):
            continue
        if id not in g:
            continue
        neighbors = list(nx.single_source_shortest_path_length(g, id, cutoff=cutoff).keys())
        pre = [x.split(":")[0] for x in neighbors]
        d[id.split(":")[0]].append(pre.count("doid"))
    d = dict(d)
    return {k: Counter(v) for k, v in d.items()}


def id_mapping_test():
    g = build_id_graph()
    print("build id graph success")

    print("cuttoff is 1")
    for k, v in num_doids_in_sg(g, 1).items():
        print(k, v)

    print("cuttoff is 2")
    for k, v in num_doids_in_sg(g, 2).items():
        print(k, v)

    print("cuttoff is 100")
    for k, v in num_doids_in_sg(g, 100).items():
        print(k, v)


if __name__ == "__main__":
    getDBInfo()
    id_mapping_test()
