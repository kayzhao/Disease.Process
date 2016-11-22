__author__ = 'kayzhao'

from config import *

# data directory data/do
DATA_DIR_DO = os.path.join(DATA_DIR, "do")

# downloaded from: http://purl.obolibrary.org/obo/doid.obo
url = "http://purl.obolibrary.org/obo/doid.obo"
file_path = os.path.join(DATA_DIR_DO, os.path.split(url)[1])