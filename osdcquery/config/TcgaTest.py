# Query
#query_fields = ["analysis_id", "disease_abbr", "files"]

# Directory structure
dirbuild_module_name = 'osdcquery.links'
dirbuild_class_name = 'TcgaLinks'
target_dir = '/glusterfs/data/TCGA/'
link_dir = '/tmp'

fs_handler_module_name = 'osdcquery.fs_handler'
fs_handler_class_name = 'UnixFsHandler'

non_disease_dir = 'none'

es_url = 'http://172.16.1.3'
es_index = 'tcga-cghub'
es_doc_type = 'analysis'

cdb_url = 'https://172.16.1.3:6984'
cdb_osdc = 'tcga-osdc'
cdb_cghub = 'tcga-cghub'
cdb_query = 'tcga-query'
cdb_query_username = 'service_query'
cdb_query_password = 'eS^6A!fv'
