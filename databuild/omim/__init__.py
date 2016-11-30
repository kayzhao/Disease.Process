__author__ = 'kayzhao'

from config import *

# data directory data/omim
DATA_DIR_OMIM = os.path.join(DATA_DIR, "omim")

# Download from https://omim.org/downloads/
mim2gene_url = "https://omim.org/static/omim/data/mim2gene.txt"
mimTitles_url = "http://data.omim.org/downloads/YusepqJtQDuqSPctv6tmVQ/mimTitles.txt"
geneMap_url = "http://data.omim.org/downloads/YusepqJtQDuqSPctv6tmVQ/genemap.txt"
geneMap2_url = "http://data.omim.org/downloads/YusepqJtQDuqSPctv6tmVQ/genemap2.txt"
morbidMap_url = "http://data.omim.org/downloads/YusepqJtQDuqSPctv6tmVQ/morbidmap.txt"


# data file path
mim2gene_path = os.path.join(DATA_DIR_OMIM, os.path.split(mim2gene_url)[1])
mimTitles_path = os.path.join(DATA_DIR_OMIM, os.path.split(mimTitles_url)[1])
geneMap_path = os.path.join(DATA_DIR_OMIM, os.path.split(geneMap_url)[1])
geneMap2_path = os.path.join(DATA_DIR_OMIM, os.path.split(geneMap2_url)[1])
morbidMap_path = os.path.join(DATA_DIR_OMIM, os.path.split(morbidMap_url)[1])


