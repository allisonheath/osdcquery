Basic query tool for querying an elasticsearch setup serving file metadata 
with file names and paths.  The query tool will then create symlinks to those
files in a new directory for each query.


python -m osdcquery.osdcquery -v "breast cancer2" 'http://localhost:9200/test_data/file_query_result.json' "disease_abbr:BRCA"
