import re

from networkx.readwrite import json_graph
from pymongo import MongoClient

from utils.obo import read_obo
from utils.common import list2dict
from databuild.hpo import *

# # This is copied from diseaseontology.parser with modifications.
def graph_to_d(graph):
    """
    :param graph: A networkx graph made from reading ontology
    :type graph: networkx.classes.multidigraph.MultiDiGraph
    :return:
    """
    node_link_data = json_graph.node_link_data(graph)
    nodes = node_link_data['nodes']

    idx_id = {idx: node['id'] for idx, node in enumerate(nodes)}
    for link in node_link_data['links']:
        # store the edges (links) within the graph
        key = link['key']
        source = link['source']
        target = link['target']

        if key not in nodes[source]:
            nodes[source][key] = set()
        nodes[source][key].add(idx_id[target])

    # for mongo insertion
    for node in nodes:
        node['_id'] = node['id'].upper()
        if "alt_id" in node:
            node['alt_id'] = [x.upper() for x in node['alt_id']]
        if "is_a" in node:
            node['is_a'] = [x.upper() for x in node['is_a']]
        if "property_value" in node:
            del node['property_value']
        del node['id']
        for k, v in node.items():
            if isinstance(v, set):
                node[k] = list(v)
    d = {node['_id']: node for node in nodes}

    return d


def parse_synonym(line):
    # line = "synonym: \"The other white meat\" EXACT MARKETING_SLOGAN [MEAT:00324, BACONBASE:03021]"
    return line[line.index("\"") + 1:line.rindex("\"")] if line.count("\"") == 2 else line


def parse_def(line):
    definition = line[line.index("\"") + 1:line.rindex("\"")] if line.count("\"") == 2 else line
    if line.endswith("]") and line.count("["):
        left_bracket = [m.start() for m in re.finditer('\[', line)]
        right_bracket = [m.start() for m in re.finditer('\]', line)]
        endliststr = line[left_bracket[-1] + 1:right_bracket[-1]]
        endlist = [x.strip().replace("\\\\", "").replace("\\", "") for x in endliststr.split(", ")]
        return definition, endlist
    else:
        return definition, None


def parse_xref(xrefs):
    xrefs = [x for x in xrefs if ":" in x]
    xrefs = [x.split(":", 1)[0].upper() + ":" + x.split(":", 1)[1] for x in xrefs]
    # # <--- this is different between HPO and DO
    xrefs = [x.split(" ", 1)[0] for x in xrefs]
    id_replace = {
        "UMLS": "UMLS_CUI",
        "ICD-9": "ICD9CM",
        "ICD9": "ICD9CM",
        "ICD10": "ICD10CM",
        "ICD-10": "ICD10CM",
        "MeSH": "MESH",
        "MSH": "MESH",
        "ORDO": "ORPHANET",
        "SNOMEDCT_US_2016_03_01": "SNOMEDCT",
        "SNOMEDCT_US_2015_03_01": "SNOMEDCT",
        "URL`": "URL",
        "HPO": "HP"
    }
    for n, xref in enumerate(xrefs):
        source = xref.split(":", 1)[0]
        source = id_replace.get(source, source)
        xrefs[n] = source + ":" + xref.split(":", 1)[1]
    return list2dict(xrefs)


def parse(mongo_collection=None, drop=True):
    if mongo_collection:
        db = mongo_collection
    else:
        client = MongoClient()
        db = client.disease.hpo
    if drop:
        db.drop()

    print("------------hpo data parsing--------------")
    graph = read_obo(open(file_path).readlines())
    print("read hpo obo file success")
    d = graph_to_d(graph)
    print("build the graph to dict")
    for value in d.values():
        if 'xref' in value:
            value['xref'] = parse_xref(value['xref'])
        if 'synonym' in value:
            value['synonym'] = list(map(parse_synonym, value['synonym']))
        if 'def' in value:
            value['def'], ref = parse_def(value['def'])
            if ref:
                if 'xref' in value:
                    value['xref'].update(parse_xref(ref))
                else:
                    value['xref'] = parse_xref(ref)
                # # v--- this is different between HPO and DO
                value['xref'] = {k: v for k, v in value['xref'].items() if k not in {"hpo", "ddd"}}

    # db.insert_many(d.values())
    db.insert_many(list(d.values()))
    print("insert into mongodb success")
    print("------------hpo data parsed success--------------")
