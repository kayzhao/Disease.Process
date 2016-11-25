__author__ = 'kayzhao'
from pymongo import MongoClient
from utils.common import dump2gridfs, loadobj


def test_dump2gridfs():
    raise NotImplementedError


def test_loadobj():
    client = MongoClient()
    db = client.disease
    for x in loadobj('ctd_genes_mesh:D052439.obj', db, 'gridfs'):
        for key, value in x.items():
            print(key)
            print(value)


if __name__ == "__main__":
    test_loadobj()