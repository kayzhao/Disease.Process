__author__ = 'kayzhao'

__METADATA__ = {
    "src_name": 'DisGeNet',
    "src_url": 'http://www.disgenet.org/web/DisGeNET/menu',
    "field": "disgenet",
    "license": "ODbL",
    "license_url": "http://opendatacommons.org/licenses/odbl/"
}

from config import *

# data directory data/do
DATA_DIR_DISCONNECT = os.path.join(DATA_DIR, "disconnect")

# The file contains gene-disease associations from UNIPROT, CTD (human subset), ClinVar, Orphanet, and the GWAS Catalog.
url_gene_disease = "http://disease-connect.org/download/Disease-Gene_v1.csv.gz"
file_disconnect_gene_disease = os.path.join(DATA_DIR_DISCONNECT, 'Disease-Gene_v1.csv')