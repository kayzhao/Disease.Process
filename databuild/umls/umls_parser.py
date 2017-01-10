__author__ = 'kayzhao'

import pandas as pd
from pymongo import MongoClient
from databuild.umls import *
from tqdm import tqdm
from utils.common import list2dict, dict2list

# normalize the id type
id_replace = {
    "CCS_10": "CCS10",

    "ICD-9": "ICD9CM",
    "ICD9": "ICD9CM",
    "ICD10": "ICD10CM",
    "ICD-10": "ICD10CM",

    "ORDO": "ORPHANET",
    "SNOMEDCT_US": "SNOMEDCT",
    "HPO": "HP",

    "HL7V2.5": "HL7V25",
    "HL7V3.0": "HL7V30",

    # MSH
    "MSH": "MESH",
}


def load_mrconso_rrf(db):
    '''
    columns name from
    https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.concept_names_and_sources_file_mr/?report=objectonly
    load the UMLS_CUI MRCONSO data(total records = 12287973)
    :return:
    '''
    col_names = ['CUI', 'LAT', 'TS', 'LUI', 'STT', 'SUI', 'ISPREF', 'AUI', 'SAUI',
                 'SCUI', 'SDUI', 'SAB', 'TTY', 'CODE', 'STR', 'SRL', 'SUPPRESS', 'CVF', 'NONE']

    columns_rename = {
        'CUI': "_id",
        'LAT': "language",
        'TS': "term_status",
        'LUI': "lui",
        'STT': "string_type",
        'SUI': "sui",
        'ISPREF': "atom_status",
        'AUI': "aui",
        'SAUI': "source_aui",
        'SCUI': "source_cui",
        'SDUI': "source_dui",
        'SAB': "source_abbreviation",
        'TTY': "source_term_type",
        'CODE': "source_identifier",
        'STR': "string",
        'SRL': "source_restriction_level",
        'SUPPRESS': "suppress",  # suppressible_flag
        'CVF': "cvf"  # content_view_flag
    }
    chunksize = 100000

    f = open(umls_xref_path, "r", encoding='utf-16le', errors='ignore')
    total_len = 0

    for df in tqdm(pd.read_csv(f, sep='\\|', engine='python', comment="#", header=None, chunksize=chunksize,
                               names=col_names), total=49867785 / chunksize):
        '''
        records statics
        '''
        total_len += len(df.to_records())
        print(len(df.to_records()), total_len)

        del df['NONE']

        # update or insert the data
        for cui, subdf in df.groupby("CUI"):
            # format the sub-df
            subdf = subdf.rename(columns=columns_rename)
            subdf['_id'] = subdf['_id'].apply(lambda x: "UMLS_CUI:" + x)
            subdf['source_abbreviation'] = subdf['source_abbreviation'].apply(lambda x: id_replace.get(x, x))
            subdf['cvf'] = subdf['cvf'].dropna().map('{:.0f}'.format)
            subdf['source_aui'] = subdf['source_aui'].dropna().map('{:.0f}'.format)
            subdf['source_restriction_level'] = subdf['source_restriction_level'].dropna().map('{:.0f}'.format)

            # delete the _id field
            del subdf['_id']

            # get rid of nulls
            sub = subdf.apply(lambda x: x.dropna(), axis=1).to_dict(orient="records")
            sub = [{k: v for k, v in s.items() if v == v} for s in sub]

            # xrefs and synonyms
            synonyms = []
            xrefs = []
            for record in sub:
                if 'string' in record and 'language' in record:
                    synonyms.append(record['string'] + "(" + record['language'] + ")")
                    del record['string']
                    del record['language']

                if 'source_abbreviation' in record and 'source_identifier' in record:
                    xrefs.append(record['source_abbreviation'] + ":" + record['source_identifier'])
                    del record['source_abbreviation']
                    del record['source_identifier']

            # remove duplicates
            synonyms = list(set(synonyms))
            xrefs = list(set(xrefs))

            if len(sub) > 0:
                db.update_one(
                    {'_id': "UMLS_CUI:" + cui},
                    {'$set':
                         {
                             "concepts": sub,
                             "synonym": synonyms,
                             "xref": list2dict(xrefs)
                         }
                    }, upsert=True)


def load_mrrel_rrf(db):
    '''
    column name from:
    https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.related_concepts_file_mrrel_rrf/?report=objectonly
    load the UMLS_CUI MRREL data(total records = 70587165)
    :return:
    '''
    col_names = ['CUI1', 'AUI1', 'STYPE1',
                 'REL',  # Relationship of second concept or atom to first concept or atom
                 'CUI2', 'AUI2', 'STYPE2',
                 'RELA',
                 'RUI', 'SRUI', 'SAB', 'SL', 'RG', 'DIR', 'SUPPRESS', 'CVF', 'NONE']

    columns_rename = {
        'CUI1': "_id",
        'AUI1': "aui_1",
        'STYPE1': "stype_1",
        'REL': 'relationship',
        'CUI2': "umls_cui",
        'AUI2': "aui_2",
        'STYPE2': "stype_2",
        'RELA': "additional_label",  # additional_relationship_label
        'RUI': "rui",
        'SRUI': "source_id",
        'SAB': "source_abbreviation",
        'SL': "source_relationship",  # source_relationship
        'RG': "relationship_group",
        'DIR': "source_direct",  # source_directionality_flag
        'SUPPRESS': "suppress",  # suppressible_flag
        'CVF': "cvf"  # content_view_flag
    }
    chunksize = 100000

    f = open(umls_mrrel_path, encoding='us-ascii')
    total_len = 0
    for df in tqdm(pd.read_csv(f, sep='\\|', engine='python', comment="#", header=None, chunksize=chunksize,
                               names=col_names), total=49867785 / chunksize):
        '''
        records statics
        '''
        total_len += len(df.to_records())
        print(len(df.to_records()), total_len)
        del df['NONE']

        # update or insert the data
        for cui, subdf in df.groupby("CUI1"):
            # format the sub-df
            subdf = subdf.rename(columns=columns_rename)
            subdf['umls_cui'] = subdf['umls_cui'].apply(lambda x: "UMLS_CUI:" + x)
            subdf['source_abbreviation'] = subdf['source_abbreviation'].apply(lambda x: id_replace.get(x, x))
            subdf['relationship_group'] = subdf['relationship_group'].dropna().map('{:.0f}'.format)
            # delete the _id field
            del subdf['_id']

            # get rid of nulls
            sub = subdf.apply(lambda x: x.dropna(), axis=1).to_dict(orient="records")
            sub = [{k: v for k, v in s.items() if v == v} for s in sub]

            # get the relationships,relationships ids or additional ship labels
            relationships = []
            ruis = []
            for record in sub:
                ruis.append(record['rui'])
                if 'relationship' in record and 'additional_label' in record:
                    relationships.append({
                        'umls_cui': record['umls_cui'],
                        'relationship': record['relationship'],
                        'additional_label': record['additional_label']
                    })
                if 'relationship' in record and 'additional_label' not in record:
                    relationships.append({
                        'umls_cui': record['umls_cui'],
                        'relationship': record['relationship'],
                    })

            # remove duplicates
            ruis = list(set(ruis))
            relationships = [dict(t) for t in set([tuple(d.items()) for d in relationships])]

            if len(relationships) > 0:
                db.update_one(
                    {'_id': "UMLS_CUI:" + cui},
                    {'$set': {"ruis": ruis, "relationships": relationships}}, upsert=True)


def get_mrconso_xref_nums(docs):
    '''

    :param db:
    :return:
    all_data:dict , all umls data
    1.{umls_cui:source_ids}
    2.{'all_xref_ids':list}
    '''
    all_data = dict()
    all_ids = set()
    all_umls_xrefs = dict()
    for doc in docs:
        if 'xref' in doc:
            one_xref = set(dict2list(doc['xref']))
            all_ids.update(one_xref)
            all_umls_xrefs[doc['_id']] = list(one_xref)
        print(len(all_ids))

    # xref ids data
    all_data['all_xref_ids'] = all_ids
    all_data['all_umls_xrefs'] = all_umls_xrefs

    return all_data


def parse(mongo_collection=None, drop=True):
    if mongo_collection:
        db = mongo_collection
    else:
        client = MongoClient()
        db = client.disease.mesh
    if drop:
        db.drop()

    print("------------umls data parsing--------------")

    print("------------umls mrconso data --------------")
    # load_mrconso_rrf(db)

    print("------------umls mrrel data --------------")
    load_mrrel_rrf(db)

    print("------------umls data parsed success--------------")