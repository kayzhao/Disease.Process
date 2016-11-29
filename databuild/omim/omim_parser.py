__author__ = 'kayzhao'
import xml.etree.ElementTree as et
from collections import defaultdict

import pandas as pd
from pymongo import MongoClient
from databuild.omim import *


def parse_mimTitles():
    mimTitles_names = "Prefix	Mim Number	Preferred Title; symbol	Alternative Title(s); symbol(s)	Included Title(s); symbols".split(
        "\t")
    mimTitles = pd.read_csv(mimTitles_path, sep='\t', comment='#', names=mimTitles_names)
    mimTitles['Mim Number'] = mimTitles['Mim Number'].astype(str)
    # print(mimTitles.head())

    prefix = {0: "Other, mainly phenotypes with suspected mendelian basis",
              "Asterisk": "Gene description",
              "Plus": "Gene and phenotype, combined",
              "Caret": "Obsolete",
              "Percent": "Phenotype description or locus, molecular basis unknown",
              "Number Sign": "Phenotype description, molecular basis known"}
    mimTitles['type'] = mimTitles.Prefix.fillna(0).apply(prefix.get)
    del mimTitles['Prefix']
    pts = mimTitles['Preferred Title; symbol'].apply(lambda x: pd.Series(x.split(';', 1)))
    pts.columns = ['title', 'symbol']
    mimTitles = pd.concat([mimTitles, pts], axis=1)
    del mimTitles['Preferred Title; symbol']

    mimTitles.title = mimTitles.title.str.strip()
    mimTitles.symbol = mimTitles.symbol.str.strip()
    mimTitles.rename(columns={'Mim Number': "_id"}, inplace=True)
    mimTitles._id = mimTitles._id.apply(lambda x: "omim:" + x)
    # print(mimTitles.head())

    mimTitles_records = [{k: v for k, v in zip(mimTitles.columns, list(record)[1:])} for record in
                         mimTitles.to_records()]
    mimTitles_records = [{k: v for k, v in record.items() if v == v} for record in mimTitles_records]

    # print(mimTitles_records[0])

    # Fix Alternative Title(s); symbol(s)
    for record in mimTitles_records:
        if 'Alternative Title(s); symbol(s)' in record:
            altlist = [x for x in record['Alternative Title(s); symbol(s)'].split(";;")]
            del record['Alternative Title(s); symbol(s)']
            record['alternative'] = []
            for alt in altlist:
                if not alt.count(";"):
                    record['alternative'].append({'title': alt.strip()})
                else:
                    record['alternative'].append(
                        {'title': alt.split(";", 1)[0].strip(), 'symbol': alt.split(";", 1)[1].strip()})
    # print(mimTitles_records[0])

    # Fix Included Title(s); symbols
    for record in mimTitles_records:
        if 'Included Title(s); symbols' in record:
            altlist = [x for x in record['Included Title(s); symbols'].split(";;")]
            del record['Included Title(s); symbols']
            record['included'] = []
            for alt in altlist:
                if not alt.count(";"):
                    record['included'].append({'title': alt.strip()})
                else:
                    record['included'].append(
                        {'title': alt.split(";", 1)[0].strip(), 'symbol': alt.split(";", 1)[1].strip()})

    # pd.DataFrame.to_csv(mimTitles, os.path.join(DATA_DIR_OMIM, "mims.txt"))
    # print(mimTitles_records[0])
    print([x for x in mimTitles_records if 'alternative' in x and 'included' in x])
    return mimTitles_records


def parse_geneMap():
    geneMap_names = "Sort	Month	Day	Year	Cyto Location	Gene Symbols	Confidence	Gene Name	MIM Number	Mapping Method	Comments	Phenotypes	Mouse Gene Symbol".split(
        '\t')

    return


def parse_geneMap2():
    geneMap2_names = "Chromosome	Genomic Position Start	Genomic Position End	Cyto Location	Computed Cyto Location	Mim Number	Gene Symbols	Gene Name	Approved Symbol	Entrez Gene ID	Ensembl Gene ID	Comments	Phenotypes	Mouse Gene Symbol/ID".split(
        "\t")
    geneMap2 = pd.read_csv(geneMap2_path, sep='\t', comment='#', names=geneMap2_names)
    print(geneMap2.head())
    return


def parse_morbidMap():
    morbidMap_names = "Phenotype	Gene Symbols	MIM Number	Cyto Location".split("\t")

    return


def parse(mongo_collection=None, drop=True):
    if mongo_collection:
        db = mongo_collection
    else:
        client = MongoClient()
        db = client.disease.omim
    if drop:
        db.drop()

    print("------------omim data parsing--------------")

    print("------------parsing omim data[minTitles]--------------")
    mimTitles_records = parse_mimTitles()

    db.insert_many(mimTitles_records)
    print("------------omim data parsed success--------------")


if __name__ == "__main__":
    # parse_mimTitles()
    # parse_geneMap2()
    client = MongoClient('mongodb://zkj1234:zkj1234@192.168.1.113:27017/disease')
    parse(client.disease.omim)
    # parse()