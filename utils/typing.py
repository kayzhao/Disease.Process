__author__ = 'kayzhao'

from pymongo import MongoClient
from typing import List
import json
from utils import *


def getTypes():
    client = MongoClient()
    db = client.mydisease.mydisease
    doc = db.find_one()
    docs = []
    for n, doc in enumerate(db.find()):
        docs.append(doc)
        if n > 100:
            break
    compare_types(docs)