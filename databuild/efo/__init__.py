__author__ = 'kayzhao'

from config import *

# data directory data/do
DATA_DIR_EFO = os.path.join(DATA_DIR, "efo")

# downloaded from: http://purl.obolibrary.org/obo/doid.obo
url = "http://sourceforge.net/p/efo/code/HEAD/tree/trunk/src/efoinobo/efo.obo"
file_path = os.path.join(DATA_DIR_EFO, os.path.split(url)[1])