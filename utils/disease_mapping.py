__author__ = 'kayzhao'

import networkx as nx
from collections import Counter
from collections import defaultdict
from utils.common import dict2list
from pymongo import MongoClient

id_types = [
    "UMLS_CUI", "HP", "DOID", "KEGG", "MESH", "OMIM", "ICD9CM", "ICD10CM",
    "EFO", "ORPHANET", "NCI", "RXNORM", "NDFRT", "SNOMEDCT", "OTHER"
]

xref_types = [
    "KEGG", "STEDMAN", "MUCOUS", "MUSCULARIS", "SWEAT", "HMDB", "METACYC", "GALEN",
    "HPPO", "LIPID_MAPS", "SNOMEDCT", "MONARCH", "REAXYS", "TAO", "KNAPSACK",
    "DDD", "NCI", "DOID", "ICD10CM", "GAID", "GMELIN", "MAT", "HP", "OPENCYC",
    "MUSCULAR", "GC", "EHDAA", "ORPHANET", "CITEXPLORE", "BTO", "CEREBELLAR", "CALOHA",
    "NUI", "CSP", "NDFRT", "UKB", "REACTOME", "MEDLINEPLUS", "RXNORM", "NASAL", "ICD9CM",
    "NEUROMICS", "EMEDICINE", "EFO", "NCIT", "ORCIRD", "UHPO", "CHEMIDPLUS", "JA", "NIHR", "OMID",
    "MESH", "MEDDRA", "UMLS_CUI", "OMIM", "PHENOTIPS",
]


def get_ids_info(all_diseases_docs):
    '''
    get the db disease ids statistics
    :return:
    '''
    print("get the db disease ids statistics ")
    all_ids = set()
    for x in all_diseases_docs:
        if x['_id'].split(':', 1)[0] in id_types:
            all_ids.update(set([x['_id']]))
    f = open("D:/disease/mapping/disease_ids_info.txt", 'w', encoding='utf-8')
    for k, v in Counter([x.split(":", 1)[0] for x in all_ids]).items():
        f.write('{}\t{}\n'.format(k, v))
        print("%s \t %d" % (k, v))
    f.close()
    return all_ids


def get_xrefs_idtype(xref_docs):
    print("get the db xref statistics ")
    '''
    get the db xref records
    :return: d the xref data
    '''
    d = defaultdict()
    # get the xref docs count
    all_type = set()
    for doc in xref_docs:
        for xref in dict2list(doc['xref']):
            all_type.update(set([xref.split(":", 1)[0]]))
    f = open("D:/disease/mapping/mapping_ids_type.txt", 'w', encoding='utf-8')
    for x in all_type:
        print(x)
        f.write('"{}",'.format(x))
    f.write('\n')
    f.close()


def get_db_xrefs(xref_docs):
    print("get the db xref statistics ")
    '''
    get the db xref records
    :return: d the xref data
    '''
    d = defaultdict()
    # get the xref docs count
    if xref_docs.count() > 0:
        for doc in xref_docs:
            d[doc['_id']] = dict2list(doc['xref'])
    return d


def build_did_graph(xref_docs):
    '''
    create the id xref graph
    :return:id graph
    '''
    g = nx.Graph()
    for doc in xref_docs:
        for xref in dict2list(doc['xref']):
            if xref.split(":", 1)[0] in xref_types:
                g.add_edge(doc['_id'].upper(), xref.upper())
    return g


def get_equiv_dtype_id(g, did, cutoff=2, dtype="UMLS"):
    """
    For a given ID, get the type disease ID,
    it is equivalent to within default 2 hops.
    """
    if did.startswith(dtype):
        return [did]
    if did not in g:
        return []
    # d_path = nx.single_source_shortest_path(g, did, cutoff=cutoff)
    d_path_length = nx.single_source_shortest_path_length(g, did, cutoff=cutoff)
    equiv = set(d_path_length.keys())
    return [x for x in equiv if x.startswith(dtype)]


def num_dtype_ids_in_sg(all_ids, g, cutoff=2, dtype="UMLS"):
    d = defaultdict(list)
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


def id_mapping_test(xref_docs, all_diseases_docs, dtype="UMLS", cutoff=3):
    g = build_did_graph(xref_docs)
    print("build id graph success")
    all_ids = get_ids_info(all_diseases_docs)
    print("load all ids success")

    f = open("D:/disease/mapping/disease_mapping_test.txt", 'w', encoding='utf-8')
    for i in range(0, cutoff, 1):
        f.write('cutoff = {}\n'.format((i + 1)))
        print('\ncutoff = {}'.format((i + 1)))
        for k, v in num_dtype_ids_in_sg(all_ids, g, (i + 1), dtype=dtype).items():
            num_nomapping = 0
            num_mapping = 0
            for n in v:
                if n > 0:
                    num_mapping += v[n]
                else:
                    num_nomapping = v[n]
            if k in id_types:
                f.write('{}\t{}\t{}\n'.format(k, num_nomapping, num_mapping))
                print("%s\t%d\t%d" % (k, num_nomapping, num_mapping))


def single_test(id, g, dtype, cutoff=3):
    for i in range(1, cutoff, 1):
        ids = get_equiv_dtype_id(g, id, i, dtype=dtype)
        print("%d \t %s" % (i, len(ids)))
        print(ids)


def get_disease_data(all_disease_docs):
    '''
    get the umls collection data
    :return:
        list of umls data
    '''
    from databuild.umls import umls_parser

    all_data = umls_parser.get_mrconso_xref_nums(all_disease_docs)
    all_ids = all_data['all_xref_ids']
    f = open("D:/disease/umls/umls_xrefs.txt", 'w', encoding='utf-16le', errors='ignore')
    for k, v in all_data['all_umls_xrefs'].items():
        f.write(k + "\t" + ''.join([x + "|" for x in v if x is not None]) + "\n")
    for k, v in Counter([x.split(":", 1)[0] for x in all_data['all_xref_ids']]).items():
        print("%s \t %d" % (k, v))
    return all_ids


def get_id_mapping_statics(id_map_docs, step="step 1"):
    print("get the dis map ids statistics ")
    d = dict()
    for x in id_types:
        d[x] = dict()
        for y in id_types:
            d[x][y.lower()] = 0

    print(d)
    for doc in id_map_docs:
        type = doc['_id'].split(':', 1)[0]
        if type not in id_types:
            continue
        # type to itself
        d[type][type.lower()] += 1
        for k, v in doc.items():
            # print(k, v)
            if k.upper() in id_types and len(v) > 0:
                d[type][k] += 1

    # append the value to
    f = open("D:/disease/mapping/disease_ids_info.txt", 'a', encoding='utf-8')
    f.write("\n this is for the {} statistics\n".format(step))
    for k, v in d.items():
        f.write('{}\t'.format(k))

        for key in v.keys():
            f.write('{}\t'.format(key))
        f.write('\n\t')

        for k1, v1 in v.items():
            f.write('{}\t'.format(v1))
            print("%s\t %d" % (k1, v1))
        f.write('\n')
    f.close()


def store_map_step1(dis, dis_map):
    print("store_map_step1")
    # docs = dis_xref.find({'xref': {'$exists': True}}, {'xref': 1})
    docs = dis.find({})
    for doc in docs:
        type = doc['_id'].split(':', 1)[0]
        if type not in id_types:
            continue
        d = dict()
        d["_id"] = doc['_id']
        if 'xref' in doc:
            d['other'] = []
            for k, v in doc['xref'].items():
                if k in id_types:
                    if k == 'HP':
                        d[k.lower()] = v
                    else:
                        d[k.lower()] = [k + ":" + x for x in v]
                else:
                    for x in v:
                        d['other'].append(k + ":" + x)

        # get rid of null
        one_doc = {k: v for k, v in d.items() if len(v) != 0}
        print(one_doc['_id'])
        dis_map.insert_one(one_doc)
    print("insert success")


def store_map_step2(dis_xref, dis_map):
    print('step 2')


def store_map_step3(dis_xref, dis_map):
    print("step 3")


def store_map_step4(dis_xref, dis_map):
    print("step 4")


def build_dis_map(dis_xref, dis_map):
    '''
    build the disease id map for the id mapping
    '''

    # the first step , get the xref data
    store_map_step1(dis_xref, dis_map)
    get_id_mapping_statics(dis_map.find({}), step='step 1')

    exit()

    # the second step, use the umls xref data
    store_map_step2(dis_xref, dis_map)
    get_id_mapping_statics(dis_map, step='step 2')

    store_map_step3(dis_xref, dis_map)
    get_id_mapping_statics(dis_map, step='step 3')

    store_map_step4(dis_xref, dis_map)
    get_id_mapping_statics(dis_map, step='step 4')


if __name__ == "__main__":
    # src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.113:27017/biodis')

    # all_docs = bio_client.biodis.disease.find({}, {'_id': 1})
    # disease_docs = bio_client.biodis.disease.find({'xref': {'$exists': True}}, {'xref': 1})
    # disease_no_umls_docs = bio_client.biodis.disease_no_umls.find({'xref': {'$exists': True}}, {'xref': 1})
    '''
    get the data info
    '''
    # get_ids_info()
    # get_db_xrefs()
    # get_xrefs_idtype()
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
    # id_mapping_test(disease_docs, all_docs, dtype="UMLS_CUI", cutoff=3)
    # id_mapping_test(disease_no_umls_docs, all_docs, dtype="UMLS_CUI", cutoff=3)

    '''
    single id test
    '''
    # ID = "MESH:D005067"
    # ID = "MESH:D010211"
    # ID = "MESH:D019867"
    # ID = "MESH:D008219"


    # get_umls_data()

    '''
    build id map
    '''
    # build_dis_map(bio_client.biodis.disease_no_umls, bio_client.biodis.dismap_no_umls)
    # store_map_step1(bio_client.biodis.disease_no_umls, bio_client.biodis.dismap_no_umls)
    get_id_mapping_statics(bio_client.biodis.dismap_no_umls.find({}), step="step 1")
    print("success")