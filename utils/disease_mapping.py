__author__ = 'kayzhao'

import networkx as nx
from collections import Counter
from collections import defaultdict
from utils.common import dict2list
from pymongo import MongoClient
import collections
import datetime

id_types = [
    "UMLS_CUI", "HP", "DOID", "KEGG", "MESH", "OMIM", "ICD9CM",
    "ICD10CM", "EFO", "ORPHANET", "NCI", "RXNORM", "SNOMEDCT", "OTHER"
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
        l = []
        l.append(x['_id'])
        if 'xref' in x:
            l = l + dict2list(x['xref'])
        all_ids.update(set(l))
        print("get_ids_info: len of all ids {}".format(len(all_ids)))

    # write the static to file
    f = open("D:/disease/mapping/disease_ids_info.txt", 'a', encoding='utf-8')
    f.write("the db disease ids statistics \n ")
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
            type = xref.split(":", 1)[0]
            if type not in xref_types:  # if xref too large select some types
                continue
            # if xref.startswith("HP"):  # if xref start with HP as HP:HP:0000001
            # xref = xref.split(":", 1)[1]
            g.add_edge(doc['_id'].upper(), xref.upper())
    return g


def get_equiv_dtype_id(g, did, cutoff=2, dtype="UMLS_CUI"):
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
    """
        get the num of xref graph mapping
    :param all_ids:
    :param g:
    :param cutoff:
    :param dtype:
    :return:
    """
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
    """
        xref graph id mapping test
    :param xref_docs:
    :param all_diseases_docs:
    :param dtype:
    :param cutoff:
    :return:
    """
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
    '''
    test disease id
    :param id: disease id
    :param g: xref graph
    :param dtype: the mapping disease id type
    :param cutoff:the shortest path cutoff
    :return:
    '''
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


def get_id_mapping_statics(dismap, step="step 1"):
    print("----- {} datetime : {}--- dismap statistics ".format(step, datetime.datetime.now()))
    d = dict()
    # init the dict
    for x in id_types:
        d[x] = dict()
        for y in id_types:
            d[x][y.lower()] = 0

    map_docs = dismap.find({})
    for doc in map_docs:
        type = doc['_id'].split(':', 1)[0]
        if type not in id_types:
            continue
        # type to itself
        d[type][type.lower()] += 1
        for k, v in doc.items():
            # print(k, v)
            if k.upper() not in id_types:
                continue
            if k.upper() == type or k == '_id':
                continue
            if len(v):
                d[type][k] += 1

    # append the value to
    path = "D:/disease/mapping/dismap_info.txt"
    path = "/home/zkj/disease/mapping/dismap_info.txt"
    f = open(path, 'a', encoding='utf-8')
    f.write("\n\n----------------------  {} {} ----------------------------\n\n".format(step, datetime.datetime.now()))
    no_statistic_types = ['OTHER', 'EFO']
    for k, v in collections.OrderedDict(sorted(d.items())).items():
        f.write('{}\t'.format(k))

        od_v = collections.OrderedDict(sorted(v.items()))

        for key in od_v.keys():
            if key in no_statistic_types:
                continue
            f.write('{}\t'.format(key.upper()))
        f.write('\n\t')
        for k1, v1 in od_v.items():
            if k1 in no_statistic_types:
                continue
            f.write('{}\t'.format(v1))
            # print("%s\t %d" % (k1, v1))
        f.write('\n')
    f.close()


def get_collection_fields(collection):
    """
    get the collection fields
    :param collection:
    :return:
    """
    from bson.code import Code

    '''
    get the field name
    '''
    mapper = Code("""function(){ for (var key in this) { emit(key, null); }}""")
    reducer = Code("""function(key, stuff){return null;}""")
    distinctFields = collection.map_reduce(
        mapper,
        reducer,
        out={'inline': 1},
        full_response=True
    )
    fields = []
    for x in distinctFields['results']:
        fields.append(x['_id'])
    fields = list(set(fields))
    return fields


def complete_map_doc(dismap):
    """
    complete the map doc of the mapping ids
    <br>
    if has <br>
    DOID:5353 -> UMLS_CUI:C0009373
    <br>
    then store the <br>
    UMLS_CUI:C0009373 -> DOID:5353
    :param dismap:
    :return:
    """
    map_docs = dismap.find({}, no_cursor_timeout=True)
    for doc in map_docs:
        did = doc['_id']
        did_type = did.split(':', 1)[0].lower()
        # print("complete_map_doc : id {}".format(did))
        for k, v in doc.items():
            if k == 'other':
                continue
            if k.upper() in id_types and len(v) > 0:
                v = list(set(v))  # remove the duplication
                for x in v:
                    # avoid the error type id
                    type = x.split(':', 1)[0]
                    if type not in id_types:
                        continue
                    dismap.update_one({'_id': x}, {'$addToSet': {did_type: did}}, upsert=True)


def remove_error_map_doc(dismap, disease):
    """
    remove the error map_doc
    :param dismap:
    :param disease:
    :return:
    """
    print('remove the _id "KEGG:" not exists')
    kegg_docs = disease.find({"_id": {'$regex': "^KEGG"}})
    kegg_ids = []
    for doc in kegg_docs:
        kegg_ids.append(doc['_id'])

    # remove the _id "KEGG:" not exists
    docs = dismap.find({"_id": {'$regex': "^KEGG"}})
    for doc in docs:
        did = doc['_id']
        # print("remove_error_map_doc kegg id : id {}".format(did))
        if did not in kegg_ids:
            dismap.remove({"_id": did})

    print('remove the kegg field "KEGG:" not exists')
    # remove the kegg field "KEGG:" not exists
    docs = dismap.find({"kegg": {'$exists': True}})
    for doc in docs:
        did = doc['_id']
        # print("remove_error_map_doc kegg fields: id {}".format(did))
        kegg = []
        for x in doc['kegg']:
            if x in kegg_ids:
                kegg.append(x)
        dismap.update_one({"_id": did}, {"$set": {"kegg": kegg}})


def store_map_step1(disease, dismap):
    """
    store the xref map to build the init id map
    :param dis:
    :param dismap:
    :return:
    """
    print("store map step1")
    # docs = dis_xref.find({'xref': {'$exists': True}}, {'xref': 1})
    docs = disease.find({}, no_cursor_timeout=True)
    for doc in docs:
        # print("store_map_step1: id {}".format(doc['_id']))
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
        dismap.insert_one(one_doc)

    # remove error map doc
    print("remove error map doc")
    remove_error_map_doc(dismap, disease)
    # complete the doc
    print("complete the doc")
    complete_map_doc(dismap)


def build_umls2umls(disease_all, umls2umls):
    """
    build the umls id to umls id relationships collection
    :param disease_all:
    :param umls2umls:
    :return:
    """
    for doc in disease_all.find({"_id": {'$regex': "^UMLS_CUI"}}):
        print("build_umls2umls: id {}".format(doc['_id']))
        if 'relationships' not in doc:
            continue
        rel_doc = dict()
        rel_doc['_id'] = doc['_id']
        for x in doc['relationships']:
            rel = x['relationship'].lower()
            if rel in rel_doc:
                rel_doc[rel].append(x['umls_cui'])
            else:
                rel_doc[rel] = [x['umls_cui']]
        # remove duplications
        for k, v in rel_doc.items():
            if k == '_id':
                continue
            rel_doc[k] = list(set(v))
        # print(rel_doc)
        umls2umls.insert_one(rel_doc)


def build_did2umls(disease_all, did2umls, umls2umls):
    """
        build the disease id to umls id collection
    :param disease_all:
    :param did2umls:
    :param umls2umls:
    :return:
    """
    for doc in disease_all.find({"_id": {'$regex': "^UMLS_CUI"}}):
        print("build_did2umls: id {}".format(doc['_id']))
        if 'xref' not in doc:
            continue
        for x in dict2list(doc['xref']):
            if x.split(':', 1)[0] in id_types:
                umls_cui = doc['_id']
                did2umls.update_one({'_id': x}, {'$addToSet': {'umls_cui': umls_cui}}, upsert=True)


def update_did2umls_v1(did2umls, umls2umls):
    """
        version 1 (too much memory to use)
        remove the did2umls collection umls cuis duplication
        update the did2umls use the umls 2 umls relationships data
        ref url:
        https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/abbreviations.html#REL
        https://ncim.nci.nih.gov/ncimbrowser/pages/rela_help_info.jsf
    :param did2umls:
    :param umls2umls:
    :return:
    """
    # load umls2umls relationships
    # init the umls to umls dict
    '''
    RL	the relationship is similar or "alike". the two concepts are similar or "alike".
        In the current edition of the Metathesaurus, most relationships with this attribute are mappings provided by a source,
        named in SAB and SL; hence concepts linked by this relationship may be synonymous, i.e. self-referential: CUI1 = CUI2.
        In previous releases, some MeSH Supplementary Concept relationships were represented in this way.
    SY	source asserted synonymy.
    '''

    synonym_fields = ['sy', 'rl']
    umls2umls_dict = dict()
    for doc in umls2umls.find({}):
        umls_rels = []
        for k, v in doc.items():
            if k in synonym_fields:
                umls_rels += v
        umls2umls_dict[doc['_id']] = list(set(umls_rels))
    print("load umls 2 umls success")

    # update and remove the duplication
    for doc in did2umls.find({}):
        print("update_did2umls: id {}".format(doc['_id']))
        umls_cuis = doc['umls_cui']
        # add umls 2 umls fields
        for umls in doc['umls_cui']:
            if umls not in umls2umls_dict:
                continue
            umls_cuis += umls2umls_dict[umls]
        umls_cuis = list(set(umls_cuis))  # remove duplications
        did2umls.update_one({'_id': doc['_id']}, {'$set': {'umls_cui': umls_cuis}}, upsert=True)


def update_did2umls_v2(did2umls, umls2umls):
    """
        version 1 (too much memory to use)
        remove the did2umls collection umls cuis duplication
        update the did2umls use the umls 2 umls relationships data
        ref url:
        https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/abbreviations.html#REL
        https://ncim.nci.nih.gov/ncimbrowser/pages/rela_help_info.jsf
    :param did2umls:
    :param umls2umls:
    :return:
    """
    # load umls2umls relationships
    # init the umls to umls dict
    '''
    RL	the relationship is similar or "alike". the two concepts are similar or "alike".
        In the current edition of the Metathesaurus, most relationships with this attribute are mappings provided by a source,
        named in SAB and SL; hence concepts linked by this relationship may be synonymous, i.e. self-referential: CUI1 = CUI2.
        In previous releases, some MeSH Supplementary Concept relationships were represented in this way.
    SY	source asserted synonymy.
    '''
    # too much time to use
    synonym_fields = ['sy', 'rl']
    # update and remove the duplication
    for doc in did2umls.find({}, no_cursor_timeout=True):
        print("update_did2umls: id {}".format(doc['_id']))
        umls_cuis = doc['umls_cui']
        # add umls 2 umls fields
        for umls in doc['umls_cui']:
            if '\ufeff' not in umls:
                continue
            u2u_doc = umls2umls.find_one({'_id': umls})
            if u2u_doc is None:
                continue
            for key in synonym_fields:
                if key in u2u_doc:
                    umls_cuis += u2u_doc[key]
            print(len(umls_cuis))
        umls_cuis = list(set(umls_cuis))  # remove duplications
        umls_cuis = [x for x in umls_cuis if '\ufeff' not in x]
        did2umls.update_one({'_id': doc['_id']}, {'$set': {'umls_cui': umls_cuis}}, upsert=True)


def store_map_step2(did2umls, disease, disease_all, dismap):
    """
    update the id map use umls xref data
    :param did2umls:
    :param disease_all:
    :param dismap:
    :return:
    """
    print("step 2")
    # init the did to umls dict
    did2umls_dict = dict()
    for doc in did2umls.find({}):
        did2umls_dict[doc['_id']] = list(set(doc['umls_cui']))

    print("load disease id to umls_cui success ")
    # update the mapping list
    map_docs = dismap.find({}, no_cursor_timeout=True)
    for doc in map_docs:
        # print("store_map_step2: id {}".format(doc['_id']))
        # all indirect xref data mapping to umls
        dids = []
        dids.append(doc['_id'])
        for doc_k, doc_v in doc.items():
            if doc_k == '_id':
                continue
            dids = dids + doc_v
        # remove duplication
        dids = list(set(dids))
        # update the doc mapping data using UMLS xref
        for did in dids:
            if did not in did2umls_dict:
                continue
            umls_list = did2umls_dict[did]  # get the umls list
            # update the umls_cui
            if 'umls_cui' in doc:
                doc['umls_cui'] = umls_list + doc['umls_cui']
            else:
                doc['umls_cui'] = umls_list
            # use umls_cui xref data to update id mapping
            for u in umls_list:
                u_doc = disease_all.find_one({"_id": u})  # get the umls document data
                if u_doc is None or "xref" not in u_doc:
                    continue
                # update the mapping data
                for x in dict2list(u_doc['xref']):
                    type = x.split(':', 1)[0]  # get the id type, such as: DOID
                    if type not in id_types:
                        continue
                    xref_id = x
                    # # hp ids
                    # if type == "HP":
                    # xref_id = x.split(':', 1)[1]
                    if type.lower() in doc:
                        doc[type.lower()].append(xref_id)
                    else:
                        doc[type.lower()] = [xref_id]  # new id list

        # remove the duplication
        for k, v in doc.items():
            if k != '_id':
                doc[k] = list(set(doc[k]))
        # update the disease id map
        dismap.update_one({'_id': doc['_id']}, {'$set': doc}, upsert=True)

    # remove error map doc
    print("remove error map doc")
    remove_error_map_doc(dismap, disease)
    # complete the doc
    print("complete the doc")
    complete_map_doc(dismap)


def store_map_step3_v1(disease, disease_all, dismap):
    """
        the third step, build the xref graph
    :param disease:
    :param disease_all:
    :param dismap:
    :return:
    """
    print('step 3')
    xref_docs = disease_all.find({'xref': {'$exists': True}})
    g = build_did_graph(xref_docs)
    print("build id graph success")

    # update the mapping list
    map_docs = dismap.find({}, no_cursor_timeout=True)
    for doc in map_docs:
        did = doc['_id']
        # print("store_map_step3: id {}".format(doc['_id']))
        for x in id_types:
            if x == 'OTHER':
                continue
            # get ids form xref graph
            # ids = get_equiv_dtype_id(g, did, cutoff=3, dtype=x)
            ids = get_equiv_dtype_id(g, did, cutoff=2, dtype=x)
            if len(ids) == 0:
                continue
            if x.lower() in doc and len(doc[x.lower()]):
                doc[x.lower()] = list(set(ids + doc[x.lower()]))
            else:
                doc[x.lower()] = list(set(ids))
        dismap.update_one({'_id': did}, {'$set': doc}, upsert=True)

    # remove error map doc
    remove_error_map_doc(dismap, disease)
    # complete the doc
    print("complete the doc")
    complete_map_doc(dismap)


def store_map_step3_v2(disease, disease_all, dismap):
    """
        the third step, build the xref graph(version 2):
            if DOID to UMLS then
                continue
            else
                get_equiv_dtype_id() use xref graph
                cutoff = max_cutoff
    :param disease:
    :param disease_all:
    :param dismap:
    :return:
    """
    print('step 3')
    xref_docs = disease_all.find({'xref': {'$exists': True}})
    g = build_did_graph(xref_docs)
    print("build id graph success")

    max_cutoff = 6

    # update the mapping list
    map_docs = dismap.find({}, no_cursor_timeout=True)
    for doc in map_docs:
        did = doc['_id']
        did_type = did.split(":", 1)[0]
        # print("store_map_step3: id {}".format(doc['_id']))
        for x in id_types:
            if x == 'OTHER' or x == did_type:
                continue
            # has mapping ,skip this type
            if x.lower() in doc and len(doc[x.lower()]):
                continue
            # get ids form xref graph
            # ids = get_equiv_dtype_id(g, did, cutoff=3, dtype=x)
            # ids = get_equiv_dtype_id(g, did, cutoff=2, dtype=x)
            for i in range(1, max_cutoff):
                ids = get_equiv_dtype_id(g, did, cutoff=i, dtype=x)
                if len(ids):
                    doc[x.lower()] = list(set(ids))
                    break
        dismap.update_one({'_id': did}, {'$set': doc}, upsert=True)

    # remove error map doc
    remove_error_map_doc(dismap, disease)
    # complete the doc
    print("complete the doc")
    complete_map_doc(dismap)


def store_map_step4():
    """
        name or synonym
    :return:
    """
    print("step 4")


def duplicate_collection(collection_old, collection_new):
    print("copy start ")
    for x in collection_old.find({}):
        collection_new.insert_one(x)
    print("copy end ")


def build_dis_map(client):
    """
        build the disease id map for the id mapping
    :param client:
    :return:
    """
    # the collection
    disease = client.biodis.disease
    disease_all = client.biodis.disease_all
    dismap = client.biodis.dismap
    did2umls = client.biodis.did2umls
    umls2umls = client.biodis.umls2umls
    dismap_all_step1 = client.biodis.dismap_step1
    dismap_all_step2 = client.biodis.dismap_step2
    dismap_all_step3 = client.biodis.dismap_step3

    '''
        before build dismap,build the umls2umls and did2umls
    '''
    # build_umls2umls(disease_all,umls2umls)
    # build_did2umls(disease_all, did2umls, umls2umls)
    # update_did2umls_v2(did2umls, umls2umls)

    '''
        the first step , get the xref data
    '''
    store_map_step1(disease, dismap)
    get_id_mapping_statics(dismap, step='step 1')
    duplicate_collection(dismap, dismap_all_step1)

    '''
        the second step, use the umls xref data
    '''
    store_map_step2(did2umls, disease, disease_all, dismap)
    get_id_mapping_statics(dismap, step='step 2')
    duplicate_collection(dismap, dismap_all_step2)
    '''
        the third step, build the xref graph
    '''
    # store_map_step3(disease, dismap)
    store_map_step3_v2(disease, disease_all, dismap)
    get_id_mapping_statics(dismap, step='step 3')
    duplicate_collection(dismap, dismap_all_step3)


if __name__ == "__main__":
    # src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.113:27017/biodis')
    kaypc_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.106:27017/biodis')
    local_client = MongoClient('mongodb://kayzhao:kayzhao@127.0.0.1:27017/biodis')

    # all_docs = bio_client.biodis.disease.find({})
    # disease_docs = bio_client.biodis.disease.find({'xref': {'$exists': True}}, {'xref': 1})
    # disease_no_umls_docs = bio_client.biodis.disease_no_umls.find({'xref': {'$exists': True}}, {'xref': 1})
    '''
    get the data info
    '''
    # get_ids_info(all_docs)
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
    build_dis_map(local_client)

    # clone the collection
    # duplicate_collection(bio_client.biodis.dismap_no_umls_bak, bio_client.biodis.dismap_no_umls)
    # duplicate_collection(bio_client.biodis.dismap_no_umls, bio_client.biodis.dismap_step_1)
    # build_dis_map(bio_client.biodis.disease_no_umls, bio_client.biodis.dismap_no_umls)
    # duplicate_collection(bio_client.biodis.dismap_step_2, bio_client.biodis.dismap_no_umls)
    # print("step 2 rebuild dismap")
    # duplicate_collection(bio_client.biodis.dismap__all_step2, bio_client.biodis.dismap)

    '''
    build diseaseid 2 umls id dict
    '''
    # build_did2umls(bio_client.biodis.disease_all, bio_client.biodis.did2umls)
    # remove_did2umls_duplication(bio_client.biodis.did2umls)
    # build umls 2 umls
    # build_umls2umls(bio_client.biodis.disease_all, bio_client.biodis.umls2umls)

    '''
    step 1
    '''
    # store_map_step1(bio_client.biodis.disease_no_umls, bio_client.biodis.dismap_no_umls)
    # get_id_mapping_statics(bio_client.biodis.dismap_no_umls, step="step 1-1")

    '''
    remove the error kegg ids from doid xref
    '''
    # remove_error_map_doc(bio_client.biodis.dismap_no_umls, bio_client.biodis.disease)
    # get_id_mapping_statics(bio_client.biodis.dismap_no_umls, step="step 1-2")

    '''
    step 2
    '''
    # store_map_step2(
    # kaypc_client.biodis.did2umls,
    # kaypc_client.biodis.disease,
    # kaypc_client.biodis.dismap_2
    # )
    # get_id_mapping_statics(bio_client.biodis.dismap_no_umls, step="step 2")

    '''
    step 3
    '''
    # store_map_step3(bio_client.biodis.disease, bio_client.biodis.dismap_no_umls)
    # get_id_mapping_statics(bio_client.biodis.dismap_no_umls, step="step 3")

    # print("dismap_no_umls step3")
    # store_map_step3(local_client.biodis.disease, local_client.biodis.dismap_no_umls)
    # get_id_mapping_statics(local_client.biodis.dismap_no_umls, step='dismap_no_umls step 3')
    # print("dismap step3")
    # store_map_step3(local_client.biodis.disease, local_client.biodis.dismap)
    # get_id_mapping_statics(local_client.biodis.dismap, step='dismap step 3')


    # step static
    # client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.113:27017/biodis')
    # dismap_step1 = client.biodis.dismap_step1
    # dismap_step2 = client.biodis.dismap_step2
    # dismap_step3 = client.biodis.dismap_step3
    # disease = client.biodis.disease
    # get_id_mapping_statics(dismap_step1, step='step 1')
    # get_id_mapping_statics(dismap_step2, step='step 2')
    # get_id_mapping_statics(dismap_step3, step='step 3')
    # remove_error_map_doc(dismap_step1,disease)
    # get_id_mapping_statics(dismap_step1, step='step 1')
    '''
    remove error
    '''
    # remove_error_map_doc(bio_client.biodis.dismap_no_umls, bio_client.biodis.disease)
    # remove_did2umls_duplication(bio_client.biodis.did2umls)

    print("success")