# LOGGING #
import logging
import os

# Data Path of Project
DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

# disease database names
db_names = [
    'do',
    'hpo',
    'mesh',
    'omim',
    'umls',
    'icd',

    # the bio-disease
    'kegg',
    'efo',
    'snomedct'
    'pharmgkb',

    # the mydisease.info
    'disgenet',
    'ctd',
    'ndfrt',
    'orphanet',
    'pydb'
]

LOGGER_NAME = "disease.data"
# this will affect any logging calls
logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(logging.DEBUG)
# log to console by default
logger.addHandler(logging.StreamHandler())

ALLOWED_OPTIONS = ['_source', 'start', 'from_', 'size', 'sort', 'explain',
                   'version', 'aggs', 'fetch_all', 'species', 'fields',
                   'userfilter', 'exists', 'missing', 'include_tax_tree',
                   'species_facet_filter']

ES_DOC_TYPE = 'disease'

STATUS_CHECK_ID = '1017'

FIELD_NOTES_PATH = ''
JSONLD_CONTEXT_PATH = ''

# ################ #
# Mydeisease HUB VARS  #
# ################ #

DATA_SRC_SERVER = '202.197.66.216'
DATA_SRC_PORT = 27017
DATA_SRC_DATABASE = 'src_disease'
DATA_SRC_MASTER_COLLECTION = 'src_master'  # for metadata of each src collections
DATA_SRC_DUMP_COLLECTION = 'src_dump'  # for src data download information
DATA_SRC_BUILD_COLLECTION = 'src_build'  # for src data build information

DATA_TARGET_SERVER = 'localhost'
DATA_TARGET_PORT = 27017
DATA_TARGET_DATABASE = 'disease'
DATA_TARGET_MASTER_COLLECTION = 'db_master'

# webserver to show hub status
DATA_WWW_ROOT_URL = "http://localhost:8000"

DATA_SERVER_USERNAME = 'kayzhao'
DATA_SERVER_PASSWORD = 'kayzhao'

LOG_FOLDER = 'logs'

ES_HOST = 'localhost:9500'
ES_INDEX_NAME = 'diseasedoc'
ES_INDEX_TYPE = 'disease'

species_li = ['human', 'mouse', 'rat', 'fruitfly', 'nematode', 'zebrafish', 'thale-cress', 'frog', 'pig']

taxid_d = {'human': 9606,
           'mouse': 10090,
           'rat': 10116,
           'fruitfly': 7227,
           'nematode': 6239,
           'zebrafish': 7955,
           'thale-cress': 3702,
           'frog': 8364,
           'pig': 9823,
}

DATA_ARCHIVE_ROOT = '<path_to_data_archive_root_folder>'