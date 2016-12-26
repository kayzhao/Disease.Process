__author__ = 'kayzhao'

__METADATA__ = {
    "src_name": 'MeSH',
    "src_url": 'https://www.nlm.nih.gov/mesh/',
    "version": "2017",
    "field": "mesh",
    "license": "",
    "license_url": ""
}

from config import *

# data directory data/mesh
DATA_DIR_MESH = os.path.join(DATA_DIR, "mesh")

# downloaded from: https://www.nlm.nih.gov/mesh/download_mesh.html
# https://www.nlm.nih.gov/mesh/filelist.html
desc_url = "ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/asciimesh/d2017.bin"
supp_url = "ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/asciimesh/c2017.bin"
sem_groups = "https://semanticnetwork.nlm.nih.gov/download/SemGroups.txt"

desc_path = os.path.join(DATA_DIR_MESH, os.path.split(desc_url)[1])
supp_path = os.path.join(DATA_DIR_MESH, os.path.split(supp_url)[1])
sem_groups_path = os.path.join(DATA_DIR_MESH, os.path.split(sem_groups)[1])


def get_mapping():
    mapping = {
        "mesh": {
            "properties": {
                "_id": {
                    "type": "string"
                },
                "record_type": {
                    "type": "string"
                },
                "note": {
                    "type": "string"
                },
                "semantic_type_id": {
                    "type": "string"
                },
                "term": {
                    "type": "string"
                },
                "see_also": {
                    "type": "string"
                },
                "last_updated": {
                    "type": "string"
                },
                "synonym": {
                    "type": "string"
                },
                "descriptor_class": {
                    "type": "string"
                },
                "semantic_type": {
                    "type": "string"
                },
                "tree": {
                    "type": "string"
                }
            }
        }
    }
    return mapping
