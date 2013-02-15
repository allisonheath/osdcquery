# Query
query_module_name = 'osdcquery.elastic_search'
query_class_name = 'EsQuery'
query_fields = ["analysis_id", "disease_abbr", "files"]

# Directory structure
dirbuild_module_name = 'osdcquery.dir_build'
dirbuild_class_name = 'TcgaDirBuild'
target_dir = '/glusterfs/data/TCGA/'
link_dir = '~/'

fs_handler_module_name = 'osdcquery.fs_handler'
fs_handler_class_name = 'UnixFsHandler'

field_module_name = 'osdcquery.elastic_search'
field_class_name = 'FieldList'

non_disease_dir = 'none'

url = 'http://172.16.1.11/cghub/analysis/'
