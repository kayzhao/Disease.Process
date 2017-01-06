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


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.110:27017/biodis')

    process_ctd_go(bio_client.biodis.go_ctd_temp)
    print("success")