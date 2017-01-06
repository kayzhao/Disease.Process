__author__ = 'kayzhao'

import gzip
from pymongo import MongoClient
from databuild.ctdbase import *
from databuild.ctdbase import ctd_parser


def process_ctd_chemical(db):
    for relationship, file_path in relationships.items():
        with gzip.open(os.path.join(DATA_DIR_CTD, file_path), 'rt', encoding='utf-8') as f:
            if relationship == "chemicals":
                print("parsing the  " + relationships + "data")
                ctd_parser.process_chemicals(db, f, relationship)


if __name__ == "__main__":
    src_client = MongoClient('mongodb://kay123:kayzhao@192.168.1.110:27017/src_disease')
    bio_client = MongoClient('mongodb://kayzhao:kayzhao@192.168.1.110:27017/biodis')

    process_ctd_chemical(bio_client.biodis.chemical)
    print("success")