# Query
#query_fields = ["analysis_id", "disease_abbr", "files"]

# Directory structure
dirbuild_module_name = 'osdcquery.links'
dirbuild_class_name = 'TcgaLinks'
target_dir = '/glusterfs/data/TCGA/'
link_dir = '~/'

fs_handler_module_name = 'osdcquery.fs_handler'
fs_handler_class_name = 'UnixFsHandler'

non_disease_dir = 'none'

es_url = 'http://172.16.1.33:9200'
es_index = 'tcga-cghub'
es_doc_type = 'analysis'

cdb_url = 'http://172.16.1.33:5984'
cdb_osdc = 'tcga-osdc'
cdb_cghub = 'tcga-cghub'
