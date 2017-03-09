__author__ = 'kayzhao'

__METADATA__ = {
    "src_name": 'NCI Thesaurus',
    "src_url": 'https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/',
    "version": "2017",
    "field": "",
    "license": "",
    "license_url": ""
}

from config import *

# data directory data/mesh
DATA_DIR_NCI = os.path.join(DATA_DIR, "nci")

# get data from [https://github.com/kayzhao/GetFromKEGG]

diseases_url = "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/"

diseases_path = os.path.join(DATA_DIR_NCI, "Thesaurus.txt")
