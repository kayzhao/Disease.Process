# LOGGING #
import logging
import os

# Data Path of Project
DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")

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

DATA_SRC_MASTER_COLLECTION = 'src_master'  # for metadata of each src collections
DATA_SRC_DUMP_COLLECTION = 'src_dump'  # for src data download information
DATA_SRC_BUILD_COLLECTION = 'src_build'  # for src data build information
DATA_SRC_DATABASE = 'disease_src'

DATA_TARGET_MASTER_COLLECTION = 'db_master'
