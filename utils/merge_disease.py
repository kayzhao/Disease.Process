__author__ = 'kayzhao'

from utils.common import dict2list, list2dict
from pymongo import MongoClient


def process_do(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('Disease Ontology')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['Disease Ontology']

        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_efo(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v

        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('Experimental Factor Ontology')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['Experimental Factor Ontology']

        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_orphanet(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('ORPHANET')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['ORPHANET']

        '''
        only record the relations exist
        '''
        if "phenotypes" in dic:
            del dic['phenotypes']
            dic['phenotypes'] = True
        if "disease_gene_associations" in dic:
            del dic['disease_gene_associations']
            dic['disease_gene_associations'] = True

        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_kegg(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('(KEGG) Kyoto Encyclopedia of Genes and Genomes')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['(KEGG) Kyoto Encyclopedia of Genes and Genomes']

        '''
        only record the relations exist
        '''
        if 'reference' in dic:
            if 'xref' in dic:
                dic['xref']['PMID'] = dic['reference']['PMID']
            else:
                dic['xref'] = {'PMID': dic['reference']['PMID']}
            del dic['reference']
        if "genes" in dic:
            del dic['genes']
            dic['genes'] = True
        if "markers" in dic:
            del dic['markers']
            dic['markers'] = True
        if "drugs" in dic:
            del dic["drugs"]
            dic['drugs'] = True
        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_hpo(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('Human Phenotype Ontology')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['Human Phenotype Ontology']
        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_nfdrt(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('National Drug File - Reference Terminology (NDF-RT)')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['National Drug File - Reference Terminology (NDF-RT)']

        '''
        only record the relations exist
        '''
        if "drugs" in dic:
            del dic["drugs"]
            dic['drugs'] = True
        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_mesh(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('MeSH (Medical Subject Headings)')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['MeSH (Medical Subject Headings)']

        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_omim(docs, db):
    d = []
    for doc in docs:
        dic = dict()
        for k, v in doc.items():
            if len(v) != 0:
                dic[k] = v
        db_d = db.find_one({"_id": dic['_id']})
        if db_d and "source" in db_d:
            db_d['source'].append('OMIM (Online Mendelian Inheritance in Man)')
            dic['source'] = list(set(db_d['source']))
        else:
            dic['source'] = ['OMIM (Online Mendelian Inheritance in Man)']

        '''
        only record the relations exist
        '''
        if "genes" in dic:
            del dic["genes"]
            dic['genes'] = True
        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)
    return d


def process_doc_2_disease(docs, db):
    d = []
    for doc in docs:
        print(doc["_id"])
        dic = dict()
        for k, v in doc.items():
            if not isinstance(v, (int, float)) and len(v) != 0:
                dic[k] = v

        db_d = db.find_one({"_id": dic['_id']})
        if db_d:
            # source merge
            l = db_d['source'] + dic['source']
            dic['source'] = list(set(l))

            # xref merge
            if "xref" in dic and "xref" in db_d:
                temp_xref = dict()
                # remove the null xref
                for k, v in dic['xref'].items():
                    if len(v) != 0:
                        temp_xref[k] = v
                dic['xref'] = temp_xref
                # merge the xref list
                l = dict2list(db_d['xref']) + dict2list(dic['xref'])
                dic['xref'] = list2dict(list(set(l)))

            # synonym merge
            if "synonym" in dic and "synonym" in db_d:
                l = [x + '(ENG)' for x in dic['synonym']]
                l = l + db_d['synonym']
                dic['synonym'] = list(set(l))

        d.append(dic)
        db.update_one({'_id': dic['_id']}, {'$set': dic}, upsert=True)

    return d


def update_hpxref_data(disease):
    """
    update the xref data
    :param disease:
    :return:
    """
    # docs = disease.find({
    # 'xref': {'$exists': True},
    # '_id': {'$regex': "^HP"}
    # })
    docs = disease.find({'xref': {'$exists': True}})
    for doc in docs:
        if len(doc['xref']) == 0:
            print("remove xref id = {}".format(doc['_id']))
            # remove the hp field
            disease.update_one(
                {'_id': doc['_id']},
                {'$unset': {'xref': ""}},
                upsert=True)
            continue
        if 'HP' not in doc['xref']:
            continue
        hpids = []
        for x in doc['xref']['HP']:
            print(doc['_id'], x)
            if ':' not in x:
                hpid = x
            else:
                hpid = x.split(":", 1)[1]
            if hpid.isnumeric():
                hpids.append(hpid)
        print(hpids)
        if len(hpids):
            print("update xref.HP id = {}".format(doc['_id']))
            # update the hp ids
            disease.update_one(
                {'_id': doc['_id']},
                {'$set': {'xref.HP': hpids}},
                upsert=True)
        else:
            print("remove xref.HP id = {}".format(doc['_id']))
            # remove the hp field
            disease.update_one(
                {'_id': doc['_id']},
                {'$unset': {'xref.HP': ""}},
                upsert=True)


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.110:27017/biodis')

    print("do")
    process_do(src_client.src_disease.do.find({}), bio_client.biodis.do)

    print("efo")
    process_efo(src_client.src_disease.efo.find({}), bio_client.biodis.do)

    print("orphanet")
    process_orphanet(src_client.src_disease.orphanet.find({}), bio_client.biodis.do)

    print("kegg")
    process_kegg(src_client.src_disease.kegg.find({}), bio_client.biodis.disease_no_umls)

    print("hpo")
    process_kegg(src_client.src_disease.hpo.find({}), bio_client.biodis.do)

    print("ndfrt")
    process_nfdrt(src_client.src_disease.ndfrt.find({}), bio_client.biodis.do)

    print("mesh")
    process_mesh(src_client.src_disease.mesh.find({}), bio_client.biodis.do)

    print("omim")
    process_omim(src_client.src_disease.omim.find({}), bio_client.biodis.do)

    print("merge all to disease")
    process_omim(bio_client.biodis.disease_no_umls.find({}), bio_client.biodis.do)
    # process_doc_2_disease(
    # bio_client.biodis.disease_no_umls.find({"_id": {'$regex': "^KEGG"}}),
    # bio_client.biodis.disease)

    print("success")