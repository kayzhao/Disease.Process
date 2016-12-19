__author__ = 'kayzhao'

import networkx as nx
from collections import Counter
from config import db_names
from utils.mongo import get_src_conn
from collections import defaultdict
from utils.common import dict2list
from config import DATA_SRC_DATABASE
from pymongo import MongoClient


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
    id_types = ["UMLS_CUI", "HP", "DOID", "KEGG", "MESH", "OMIM", "EFO", "ORPHANET"]
    g = nx.Graph()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        docs = db.find({'xref': {'$exists': True}}, {'xref': 1})
        for doc in docs:
            for xref in dict2list(doc['xref']):
                # if xref.split(":", 1)[0] not in id_except_pre:
                # g.add_edge(doc['_id'].upper(), xref.upper())
                if xref.split(":", 1)[0] in id_types:
                    g.add_edge(doc['_id'].upper(), xref.upper())
    return g


def get_equiv_dtype_id(g, did, cutoff=2, dtype="DOID"):
    """
    For a given ID, get the type disease ID,
    it is equivalent to within default 2 hops.
    """
    if did.startswith(dtype):
        return [did]
    if did not in g:
        return []
    d_path = nx.single_source_shortest_path(g, did, cutoff=cutoff)
    d_path_length = nx.single_source_shortest_path_length(g, did, cutoff=cutoff)
    # for k, v in d_path.items():
    # print(k, v, d_path_length[k])
    equiv = set(d_path_length.keys())
    return [x for x in equiv if x.startswith(dtype)]


def num_dtype_ids_in_sg(all_ids, g, cutoff=2, dtype="DO"):
    d = defaultdict(list)
    id_types = ["UMLS_CUI", "HP", "DOID", "KEGG", "MESH", "OMIM", "EFO", "ORPHANET"]

    for id in all_ids:
        if id.startswith(dtype):
            continue
        if id.split(":", 1)[0] not in id_types:
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
    :return:
        list of sub graphs
        the distribution
    '''
    if g is None:
        return []
    # print(nx.number_connected_components(g))
    graphs = list(nx.connected_component_subgraphs(g))
    d = []
    for sub in graphs:
        d.append(len(sub.nodes()))

    # get the connected components distribution
    for k, v in sorted(Counter(d).items()):
        print("%d\t%d" % (k, v))

    return [x for x in graphs], Counter(d)


def id_mapping_test(dtype="DO"):
    g = build_did_graph()
    mapping_cuttof = 700

    all_ids = set()
    for db_name in db_names:
        db = get_src_conn()[DATA_SRC_DATABASE][db_name]
        all_ids.update(set([x['_id'] for x in db.find({}, {'_id': 1})]))

    print("build id graph success")
    print("cuttoff is 1")
    for k, v in num_dtype_ids_in_sg(all_ids, g, 1, dtype=dtype).items():
        num_nomapping = 0
        num_mapping = 0
        for n in v:
            if n > 0:
                num_mapping += v[n]
            else:
                num_nomapping = v[n]
        if num_mapping > mapping_cuttof or num_nomapping > mapping_cuttof:
            print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))

    print("cuttoff is 2")
    for k, v in num_dtype_ids_in_sg(all_ids, g, 2, dtype=dtype).items():
        num_nomapping = 0
        num_mapping = 0
        for n in v:
            if n > 0:
                num_mapping += v[n]
            else:
                num_nomapping = v[n]
        if num_mapping > mapping_cuttof or num_nomapping > mapping_cuttof:
            print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))

    print("cuttoff is 3")
    for k, v in num_dtype_ids_in_sg(all_ids, g, 3, dtype=dtype).items():
        num_nomapping = 0
        num_mapping = 0
        for n in v:
            if n > 0:
                num_mapping += v[n]
            else:
                num_nomapping = v[n]
        if num_mapping > mapping_cuttof or num_nomapping > mapping_cuttof:
            print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))


def get_umls_data():
    '''
    get the umls collection data
    :return:
        list of umls data
    '''
    from databuild.umls import umls_parser

    client = MongoClient('mongodb://202.197.66.216:27017/src_disease')
    all_data = umls_parser.get_mrconso_xref_nums(client.src_disease.umls)
    all_ids = all_data['all_xref_ids']
    f = open("D:/disease/umls/umls_xrefs.txt", 'w', encoding='utf-16le', errors='ignore')
    for k, v in all_data['all_umls_xrefs'].items():
        f.write(k + "\t" + ''.join([x + "|" for x in v if x is not None]) + "\n")
    for k, v in Counter([x.split(":", 1)[0] for x in all_data['all_xref_ids']]).items():
        print("%s \t %d" % (k, v))
    return all_ids


if __name__ == "__main__":
    '''
    get the data info
    '''
    # get_db_info()
    # get_ids_info()
    # get_db_xrefs()

    '''
    build the disease id xref graph
    '''
    # g = build_did_graph()

    '''
    the connected sub graph test
    '''
    # get_connected_subgraph(g)

    '''
    export the graph data
    '''
    # nx.write_edgelist(g, "C:/Users/Administrator/Desktop/id_xrefs/id_xrefs.txt")
    # nx.write_adjlist(g, "C:/Users/Administrator/Desktop/id_xrefs/ids.txt")

    '''
    disease type id mapping test
    '''
    # id_mapping_test("DOID")
    id_mapping_test("UMLS_CUI")

    '''
    single id test
    '''
    # ID = "MESH:D005067"
    # ID = "MESH:D010211"
    # ID = "MESH:D019867"
    # ID = "MESH:D008219"
    # for i in range(1, 11, 1):
    # ids = get_equiv_dtype_id(g, ID, i, dtype="DOID")
    # print("%d \t %s" % (i, len(ids)))
    # print(ids)

    # get_umls_data()
    print("success")