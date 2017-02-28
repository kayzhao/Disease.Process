__author__ = 'kayzhao'

from tqdm import tqdm
from utils.mapping import *


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


def process_disease_go_relations(type, disease_id_field, docs, db):
    all_ids = set()
    for x in docs:
        # if set(['go_cc', 'go_bp', 'go_mf', 'pathways']).issubset(x):
        all_ids.update(set([x[disease_id_field]]))
    # all_ids.update(set([x[disease_id_field] for x in docs]))
    print(len(all_ids))
    for id in all_ids:
        print(id)
        db.update_one({'_id': id}, {"$set": {type: True}})


def process_disease_kegg_relations(type, disease_id_field, docs, db):
    all_ids = set()
    for x in docs:
        if type in x:
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

def add_umls_cui(client,collection):
    dismap = client.biodis.dismap
    did2umls = client.biodis.did2umls

    docs = collection.find({}, no_cursor_timeout=True)
    for doc in docs:
        if "disease_id" in doc:
            print("Disease ID = " + doc['disease_id'])
            doc_id = doc['_id']
            if doc['disease_id'].startswith("UMLS_CUI"):
                collection.update_one({'_id': doc_id}, {'$set': {"umls_cui": doc['disease_id']}}, upsert=True)
            else:
                map_doc = did2umls.find_one({'_id': doc['disease_id']})
                if map_doc is not None and 'umls_cui' in map_doc:
                    for x in map_doc['umls_cui']:
                        new_doc = doc.copy()
                        del new_doc['_id']
                        new_doc['umls_cui'] = x
                        collection.insert_one(new_doc)
                    collection.remove({"_id": doc_id})
                else:
                    map_doc = dismap.find_one({'_id': doc['disease_id']})
                    if map_doc is not None and 'umls_cui' in map_doc:
                        for x in map_doc['umls_cui']:
                            new_doc = doc.copy()
                            del new_doc['_id']
                            new_doc['umls_cui'] = x
                            collection.insert_one(new_doc)
                        collection.remove({"_id": doc_id})

if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.113:27017/biodis')
    local_client = MongoClient('mongodb://kayzhao:kayzhao@127.0.0.1:27017/biodis')

    # get_ids_info(bio_client.biodis.disease)
    # get_db_xrefs()
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


    # the disease associations collection
    # process_disease_relations(
    # "drugs",
    # bio_client.biodis.do.find({"_id": {'$regex': "^UMLS_CUI"}}),
    # bio_client.biodis.disease
    # )

    # the disease associations collection
    # process_disease_go_relations(
    # "gos",
    # "_id",
    # bio_client.biodis.go.find(),
    #     bio_client.biodis.disease
    # )
    collections=['go','gene','drug','chemical']
    for x in collections:
        add_umls_cui(bio_client,bio_client.biodis[x])
    print("success")