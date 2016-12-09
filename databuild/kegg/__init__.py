__author__ = 'kayzhao'

__METADATA__ = {
    "src_name": 'KEGG',
    "src_url": 'http://www.kegg.jp/',
    "version": "2016",
    "field": "kegg",
    "license": "",
    "license_url": ""
}

from config import *

# data directory data/mesh
DATA_DIR_KEGG = os.path.join(DATA_DIR, "kegg")

# get data from [https://github.com/kayzhao/GetFromKEGG]

diseases_url = "http://www.kegg.jp/dbget-bin/www_bget?ds:H00001"

diseases_path = os.path.join(DATA_DIR_KEGG, "Kegg_Diseases_All_Data.txt")
