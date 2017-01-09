__author__ = 'kayzhao'

__METADATA__ = {
    "src_name": 'LncRNA',
    "src_url": '',
    "version": "",
    "field": "",
    "license": "",
    "license_url": ""
}

from config import *

# data directory data/mesh
DATA_DIR_MIRNA = os.path.join(DATA_DIR, "disease_lncRNA")

# get data from
# http://www.mir2disease.org/
# http://www.cuilab.cn/hmdd

lncRNA_diseases_path = os.path.join(DATA_DIR_MIRNA, "lncRNA-disease-experi.txt")

