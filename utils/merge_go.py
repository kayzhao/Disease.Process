__author__ = 'kayzhao'

import gzip
from pymongo import MongoClient
from databuild.ctdbase import *
from databuild.ctdbase import ctd_parser


def process_ctd_go(db):
    go_l = ['go_cc', 'go_bp', 'go_mf', 'pathways']
    for relationship, file_path in relationships.items():
        print(relationship + "\t" + file_path)
        with gzip.open(os.path.join(DATA_DIR_CTD, file_path), 'rt', encoding='utf-8') as f:
            if relationship in go_l:
                print("parsing the  " + relationship + "data")
                df = ctd_parser.parse_csv_to_df(f)
                ctd_parser.parse_df(db, df, relationship)


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.110:27017/biodis')

    process_ctd_go(bio_client.biodis.go)
    print("success")