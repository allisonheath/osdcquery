# Query
query_module_name = 'elastic_search'
query_class_name = 'EsQuery'
query_fields = ["analysis_id", "disease_abbr", "files"]

# Directory structure
dirbuild_module_name = 'dir_build'
dirbuild_class_name = 'TcgaDirBuild'
target_dir = '/glusterfs/data/TCGA/'
link_dir = '/tmp/qtool/newdir/'

fs_handler_module_name = 'fs_handler'
fs_handler_class_name = 'UnixFsHandler'
