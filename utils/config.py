DATA_SRC_SERVER = 'localhost'
DATA_SRC_PORT = 27017
DATA_SRC_DATABASE = 'genedoc_src'
DATA_SRC_MASTER_COLLECTION = 'src_master'  # for metadata of each src collections
DATA_SRC_DUMP_COLLECTION = 'src_dump'  # for src data download information
DATA_SRC_BUILD_COLLECTION = 'src_build'  # for src data build information

DATA_TARGET_SERVER = 'localhost'
DATA_TARGET_PORT = 27017
DATA_TARGET_DATABASE = 'disease'
DATA_TARGET_MASTER_COLLECTION = 'db_master'

# webserver to show hub status
DATA_WWW_ROOT_URL = "http://localhost:8000"

DATA_SERVER_USERNAME = ''
DATA_SERVER_PASSWORD = ''

LOG_FOLDER = 'logs'

ES_HOST = 'localhost:9500'
ES_INDEX_NAME = 'genedoc'
ES_INDEX_TYPE = 'gene'

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
