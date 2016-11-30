__author__ = 'kayzhao'
import pandas as pd
from pymongo import MongoClient
from databuild.omim import *
from collections import defaultdict
from itertools import chain


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

    # write the df to csv
    # pd.DataFrame.to_csv(mimTitles, os.path.join(DATA_DIR_OMIM, "mims.txt"))

    return {x['_id']: x for x in mimTitles_records}

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


def parse_mim2gene():
    columns_rename = {
        'MIM Number': '_id',
        'MIM Entry Type': 'entry_type',
        'Entrez Gene ID (NCBI)': 'entrezgene',
        'Approved Gene Symbol (HGNC)': 'hgnc',
        'Ensembl Gene ID (Ensembl)': 'ensembl',
    }
    mim2gene_names = "MIM Number	MIM Entry Type	Entrez Gene ID (NCBI)	Approved Gene Symbol (HGNC)	Ensembl Gene ID (Ensembl)".split(
        "\t")
    mim2genes = pd.read_csv(mim2gene_path, sep='\t', comment='#', names=mim2gene_names)
    mim2genes.rename(columns=columns_rename, inplace=True)
    mim2genes._id = mim2genes._id.astype(str)
    mim2genes._id = mim2genes._id.apply(lambda x: "omim:" + x)
    # print(mim2genes.head())

    # change to dict
    d = []
    for did, subdf in mim2genes.groupby("_id"):
        records = list(subdf.apply(lambda x: x.dropna().to_dict(), axis=1))
        for record in records:
            if 'ensembl' in record:
                record['ensembl'] = [x for x in record['ensembl'].split(",")]
            if 'entrezgene' in record:
                record['entrezgene'] = int(record['entrezgene'])
        records = [{k: v for k, v in record.items() if k not in {'_id'}} for record in records]
        drecord = {'_id': did, 'genes': records}
        # print(drecord)
        d.append(drecord)

    return {x['_id']: x for x in d}


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
    print("------------parsing omim data[gene]--------------")
    mim2gene_records = parse_mim2gene()

    d = defaultdict(dict)
    for key in set(chain(*[list(mimTitles_records.keys()), list(mim2gene_records.keys())])):
        # merge two dict
        d[key] = dict(mimTitles_records.get(key, {}), **mim2gene_records.get(key, {}))

    print(len(d))
    for x in d.values():
        db.insert_one(x)
    print("------------omim data parsed success--------------")


if __name__ == "__main__":
    # parse_mimTitles()
    # parse_geneMap2()
    # client = MongoClient('mongodb://zkj1234:zkj1234@192.168.1.113:27017/disease')
    # parse(client.disease.omim)
    # parse()
    parse_mim2gene()