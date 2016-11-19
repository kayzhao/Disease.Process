__author__ = 'kayzhao'

from config import *

# data directory data/hpo
DATA_DIR_HPO = os.path.join(DATA_DIR, "hpo")

# downloaded from: http://human-phenotype-ontology.github.io/downloads.html
url = "http://purl.obolibrary.org/obo/hp.obo"
file_path = os.path.join(DATA_DIR_HPO, "hp.obo")
