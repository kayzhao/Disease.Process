__author__ = "kayzhao"

__METADATA__ = {
    "src_name": 'NDF-RT2',
    "src_url": 'https://www.nlm.nih.gov/research/umls/sourcereleasedocs/current/NDFRT/',
    "version": "2016-09-08",
    "field": "ndfrt",
    "license": "public domain?",
    "license_url": ""
}

from config import *

# data directory data/mesh
DATA_DIR_NDFRT = os.path.join(DATA_DIR, "ndfrt")


# downloaded from: https://www.cancer.gov/research/resources/terminology/fmt
# http://evs.nci.nih.gov/ftp1/NDF-RT/NDF-RT%20Documentation.pdf
url = "http://evs.nci.nih.gov/ftp1/NDF-RT/NDF-RT_XML.zip"
path = os.path.join(DATA_DIR_NDFRT, "NDFRT_Public_2016.11.07_TDE.xml")


def get_mapping():
    mapping = {
        "ndfrt": {
            "properties": {
                "_id": {
                    "type": "string"
                },
                "xref": {
                    "properties": {
                        "mesh": {
                            "type": "string"
                        },
                        "nui": {
                            "type": "string"
                        },
                        "rxnorm_cui": {
                            "type": "string"
                        },
                        "snomedct_us_2016_03_01": {
                            "type": "string"
                        }
                    }
                },
                "drugs_used_for_treatment": {
                    "properties": {
                        "rxnorm_cui": {
                            "type": "string"
                        },
                        "umls_cui": {
                            "type": "string"
                        },
                        "fda_unii": {
                            "type": "string"
                        },
                        "nui": {
                            "type": "string"
                        },
                        "level": {
                            "type": "string"
                        },
                        "name": {
                            "type": "string"
                        }
                    }
                },
                "synonyms": {
                    "type": "string"
                },
                "name": {
                    "type": "string"
                }
            }
        }
    }
    return mapping
