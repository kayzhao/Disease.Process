__author__ = 'kayzhao'

__METADATA__ = {
    "src_name": 'UMLS',
    "src_url": 'https://uts.nlm.nih.gov/home.html',
    "version": "2016",
    "field": "umls",
    "license": "",
    "license_url": ""
}

from config import *

# data directory data/mesh
DATA_DIR_UMLS = os.path.join(DATA_DIR, "umls")

# get data from https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html

diseases_url = "https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html"

umls_xref_path = os.path.join(DATA_DIR_UMLS, "MRCONSO.RRF")
