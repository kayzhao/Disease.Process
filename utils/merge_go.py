__author__ = 'kayzhao'

from databuild.ctdbase.ctd_parser import *


def parse_df_2_go(db, df, relationship: str):
    """
    df is parsed and added to mongodb (db)
    """
    # columns_keep = get_columns_to_keep(relationship)
    total = len(set(df.DiseaseID))

    for diseaseID, subdf in tqdm(df.groupby("DiseaseID"), total=total):
        sub = subdf.rename(columns=columns_rename).to_dict(orient="records")
        sub = [{k: v for k, v in s.items() if v == v} for s in sub]  # get rid of nulls
        records = []
        for s in sub:
            s['annotation_type'] = relationship.lower()
            s['source'] = 'The Comparative Toxicogenomics Database'
            records.append(s)
        db.insert_many(records)


def process_ctd_go(db):
    go_l = ["pathways", "GO_BP", "GO_CC", "GO_MF"]
    for relationship, file_path in relationships.items():
        print(relationship + "\t" + file_path)
        with gzip.open(os.path.join(DATA_DIR_CTD, file_path), 'rt', encoding='utf-8') as f:
            if relationship in go_l:
                print("parsing the  " + relationship + " data")
                df = parse_csv_to_df(f)
                parse_df_2_go(db, df, relationship)


def add_umls_cui(client):
    go = client.biodis.go
    dismap = client.biodis.dismap
    did2umls = client.biodis.did2umls

    docs = go.find({}, no_cursor_timeout=True)
    for doc in docs:
        if "disease_id" in doc:
            print("Disease ID = " + doc['disease_id'])
            doc_id = doc['_id']
            if doc['disease_id'].startswith("UMLS_CUI"):
                go.update_one({'_id': doc_id}, {'$set': {"umls_cui": doc['disease_id']}}, upsert=True)
            else:
                map_doc = dismap.find_one({'_id': doc['disease_id']})
                if map_doc is not None and 'umls_cui' in map_doc:
                    for x in map_doc['umls_cui']:
                        new_doc = doc.copy()
                        del new_doc['_id']
                        new_doc['umls_cui'] = x
                        go.insert_one(new_doc)
                    go.remove({"_id": doc_id})


def write_disease_ids(client):
    go = client.biodis.go
    dismap = client.biodis.dismap
    did2umls = client.biodis.did2umls

    docs = go.find({}, no_cursor_timeout=True)
    path = "D:/disease/association/go.txt"
    dpath = "D:/disease/association/diseases_go.txt"
    f = open(path, 'a', encoding='utf-8')
    fd = open(dpath, 'a', encoding='utf-8')
    s = set()
    umls_s = set()
    for doc in docs:
        if "disease_id" in doc:
            print("Disease ID = " + doc['disease_id'])
            if doc['disease_id'] in s:
                continue
            if doc['disease_id'].startswith("UMLS_CUI"):
                f.write('{}\n'.format(doc['disease_id']))
                umls_s.update([doc['disease_id']])
            else:
                map_doc = did2umls.find_one({'_id': doc['disease_id']})
                if map_doc is not None and 'umls_cui' in map_doc:
                    f.write('{}\t{}\n'.format(doc['disease_id'], map_doc['umls_cui']))
                    umls_s.update(map_doc['umls_cui'])
                else:
                    map_doc = dismap.find_one({'_id': doc['disease_id']})
                    if map_doc is not None and 'umls_cui' in map_doc:
                        f.write('{}\t{}\n'.format(doc['disease_id'], map_doc['umls_cui']))
                        umls_s.update(map_doc['umls_cui'])
                    else:
                        f.write('{}\n'.format(doc['disease_id']))
            s.update([doc['disease_id']])

    # write umls_cui list
    for x in umls_s:
        fd.write('{}\n'.format(x))
    fd.close()
    f.close()


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.113:27017/biodis')

    # process_ctd_go(bio_client.biodis.go_ctd_temp)
    # add_umls_cui(bio_client)
    write_disease_ids(bio_client)
    print("success")
