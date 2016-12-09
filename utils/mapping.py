__author__ = 'kayzhao'

import networkx as nx
from collections import Counter
from config import db_names
from utils.mongo import get_src_conn
from collections import defaultdict
from utils.common import dict2list
from config import DATA_SRC_DATABASE


def get_ids_info():
    '''
    get the db disease ids statistics
    :return:
    '''
    print("get the db disease ids statistics ")
    all_ids = set()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        all_ids.update(set([x['_id'] for x in db.find({}, {'_id': 1})]))

    for k, v in Counter([x.split(":", 1)[0] for x in all_ids]).items():
        print("%s \t %d" % (k, v))


def get_db_info():
    '''
    get the db disease statistics
    :return:
    '''
    print("get the db disease statistics ")
    all_ids = set()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        all_ids.update(set([db_name + ":" + x['_id'] for x in db.find({}, {'_id': 1})]))

    for k, v in Counter([x.split(":", 1)[0] for x in all_ids]).items():
        print("%s \t %d" % (k, v))


def get_equiv_dtype_id(g, did, cutoff=2, dtype="DOID"):
    """
    For a given ID, get the type disease ID,
    it is equivalent to within default 2 hops.
    """
    if did.startswith(dtype):
        return [did]
    if did not in g:
        return []
    equiv = set(nx.single_source_shortest_path_length(g, did, cutoff=cutoff).keys())
    return [x for x in equiv if x.startswith(dtype)]


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

    id_except_pre = ["URL", "HTTP", "HTTPS", "DOI", "WIKI", "GAID"]
    g = nx.Graph()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        docs = db.find({'xref': {'$exists': True}}, {'xref': 1})
        # get the xref docs count
        if docs.count() > 0:
            # print("%s \t %d" % (db_name, docs.count()))
            for doc in docs:
                for xref in dict2list(doc['xref']):
                    if xref.split(":", 1)[0] not in id_except_pre:
                        g.add_edge(doc['_id'].upper(), xref.upper())
    return g


def num_dtype_ids_in_sg(g, cutoff=2, dtype="DOID"):
    d = defaultdict(list)
    all_ids = set()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        all_ids.update(set([x['_id'] for x in db.find({}, {'_id': 1})]))

    for id in all_ids:
        if id.startswith(dtype):
            continue
        if id not in g:
            # no xref data
            d[id.split(":")[0]].append(0)
            continue
        neighbors = list(nx.single_source_shortest_path_length(g, id, cutoff=cutoff).keys())
        pre = [x.split(":")[0] for x in neighbors]
        d[id.split(":")[0]].append(pre.count(dtype))
    d = dict(d)

    return {k: Counter(v) for k, v in d.items()}


def get_connected_subgraph(g):
    '''
    get the connected component sub graphs
    :param g:
    :return:list of sub graphs
    '''
    if g is None:
        return []
    print(nx.number_connected_components(g))
    graphs = list(nx.connected_component_subgraphs(g))
    d = []
    for sub in graphs:
        d.append(len(sub.nodes()))

    return [x for x in graphs], Counter(d)


def id_mapping_test(dtype="DOID"):
    g = build_did_graph()
    print("build id graph success")
    print("cuttoff is 1")
    for k, v in num_dtype_ids_in_sg(g, 1, dtype=dtype).items():
        num_nomapping = 0
        num_mapping = 0
        for n in v:
            if n > 0:
                num_mapping += v[n]
            else:
                num_nomapping = v[n]
        print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))

    print("cuttoff is 2")
    for k, v in num_dtype_ids_in_sg(g, 2, dtype=dtype).items():
        num_nomapping = 0
        num_mapping = 0
        for n in v:
            if n > 0:
                num_mapping += v[n]
            else:
                num_nomapping = v[n]
        print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))

    print("cuttoff is 3")
    for k, v in num_dtype_ids_in_sg(g, 3, dtype=dtype).items():
        num_nomapping = 0
        num_mapping = 0
        for n in v:
            if n > 0:
                num_mapping += v[n]
            else:
                num_nomapping = v[n]
        print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))


if __name__ == "__main__":
    # get_db_info()
    # get_ids_info()
    # get_db_xrefs()
    # id_mapping_test(dtype="DOID")

    g = build_did_graph()
    graphs, counts = get_connected_subgraph(g)
    for k, v in sorted(counts.items()):
        print("%d\t%d" % (k, v))
    # nx.write_edgelist(g, "C:/Users/Administrator/Desktop/id_xrefs/id_xrefs.txt")
    # nx.write_adjlist(g, "C:/Users/Administrator/Desktop/ids.txt")

    ID = "MESH:D005067"
    # ID = "MESH:D010211"
    for i in range(1, 10, 1):
        ids = get_equiv_dtype_id(g, ID, i, dtype="DOID")
        print("%d \t %s" % (i, len(ids)))
    print("success")