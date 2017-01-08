__author__ = 'kayzhao'

__METADATA__ = {
    "src_name": 'MiRNA',
    "src_url": 'https://uts.nlm.nih.gov/home.html',
    "version": "2016",
    "field": "umls",
    "license": "",
    "license_url": ""
}

from config import *

# data directory data/mesh
DATA_DIR_MIRNA = os.path.join(DATA_DIR, "disease_miRNA")

# get data from
# http://www.mir2disease.org/
# http://www.cuilab.cn/hmdd

diseases_path = os.path.join(DATA_DIR_MIRNA, "diseases.txt")
hdmm_path = os.path.join(DATA_DIR_MIRNA, "HMDD2.txt")
miR2disease_path = os.path.join(DATA_DIR_MIRNA, "miR2Disease.txt")
