__author__ = 'kayzhao'

from pymongo import MongoClient


def getDBTypes():
    from utils.typeutils import compare_types
    print("typing")
    client = MongoClient('mongodb://zkj1234:zkj1234@192.168.1.113:27017/disease')
    db = client.disease.do
    docs = []
    for n, doc in enumerate(db.find()):
        docs.append(doc)
        if n > 100:
            break

    compare_types(docs)


if __name__ == "__main__":
    getDBTypes()