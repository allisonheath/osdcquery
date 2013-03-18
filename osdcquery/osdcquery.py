#!/usr/bin/python

# Apache 2 License

'''Runs query and builds symlinks '''

import datetime
import importlib
import json
import os
import os.path
import requests

from optparse import OptionParser
from util import get_class
from util import get_simple_logger
from util import shared_options
from pyelasticsearch import ElasticSearch

def create_summary(query_name, query_url, query_string, config, num_files,
    num_links):
    ''' Create a json summary that describes the results of the query 
    operation.  This json is written out to a file that can be used 
    for updating queries.
    '''
    return json.dumps({
        "Query Name": query_name,
        "URL": query_url,
        "Query": query_string,
        "Configuration Module": config,
        "Number of Files Found": num_files,
        "Number of Files Linked": num_links,
        "Date of Query": datetime.datetime.now().isoformat()
    }, sort_keys=True, indent=4)


#because of the inconsistency between _count and _search
def get_count_query(query_string):
    query = {
                "query_string": {
                    "query": query_string
                }
            }
    return query

def get_search_query(query_string, fields=None, size=10):
    query = {}
    query['query'] = get_count_query(query_string)
    query['size'] = size

    #the docs say you can use * to get all fields, doesn't seem to work
    if fields:
        query['fields'] = fields
    return query

def get_osdc_status(query_results, cdb_url, cdb_db):
    keys = {}
    keys['keys'] = []
    for entry in query_results['hits']['hits']:
        keys['keys'].append(entry['_id'])

    bulk_url = ''.join([cdb_url, '/', cdb_db, '/_all_docs?include_docs=true'])
    headers = {'content-type' : 'application/json'}
    r = requests.post(bulk_url, data=json.dumps(keys), headers=headers)
    rjson = r.json()

    status = {}

    for row in rjson['rows']:
        if 'error' in row:
            status[row['key']] = {}
            status[row['key']]['error'] = row['error']
        else:
            status[row['key']] = {}
            status[row['key']]['md5_ok'] = row['doc']['md5_ok']

    return status

def main():
    '''Runs query and builds symlinks '''

    usage = ("( python -m osdcquery.osdcquery | %prog ) [options] query_name"
        "[url] query_string\n If url is missing it will use the default from"
        " the configuration module")

    parser = OptionParser(usage=usage)

    # add shared options
    shared_options(parser)

    parser.add_option("-t", "--target_dir", dest="target_dir",
        help="target directory (where the original files are located)")

    parser.add_option("-l", "--link_dir", dest="link_dir",
        help="link directory (where to put the generated symlinks)")

    parser.add_option("-i", "--dangle", dest="dangle", action="store_true",
        help="ignore nonexisting target file; create dangling link",
        default=False)

    parser.add_option("-u", "--update", action="store_true", dest="update",
        help="update files in query directory using .info file",
        default=False)

    (options, args) = parser.parse_args()

    logger = get_simple_logger(options.loglevel, options.verbose)

    settings = importlib.import_module(options.config)

    target_dir = options.target_dir if options.target_dir else \
        settings.target_dir

    link_dir = options.link_dir if options.link_dir else settings.link_dir
    link_dir = os.path.expanduser(link_dir)

    max_args = 3
    min_args = 2
    if options.update:
        max_args = 1
        min_args = 1

    if len(args) > max_args or len(args) < min_args:
        parser.error("incorrect number of arguments")

    query_name = args[0]

    fs_handler_class = get_class(settings.fs_handler_module_name,
        settings.fs_handler_class_name)

    fs_handler = fs_handler_class()

    new_dir = os.path.join(link_dir, query_name)

    if options.update:
        info = json.loads(fs_handler.read_manifest(new_dir))
        query_url = info[QUERY_URL]
        query_string = info[QUERY_STRING]

    else:
        if len(args) == 2:
            query_url = settings.es_url
            query_string = args[1]
        else:
            query_url = args[1]
            query_string = args[2]

    if fs_handler.exists(new_dir) and not options.update:
        error_message = 'Directory "%s" already exists' % new_dir
        logger.error(error_message)
        exit(1)

    #why?
    #query_class = get_class(settings.query_module_name,
    #    settings.query_class_name)

    #query = query_class(query_url, settings.query_fields,
    #    settings.non_disease_dir)

    dirbuild_class = get_class(settings.dirbuild_module_name,
        settings.dirbuild_class_name)

    builder = dirbuild_class(target_dir, os.path.join(link_dir, query_name))

    meta_es = ElasticSearch(query_url)

    count_query = get_count_query(query_string)
    logger.debug(count_query)

    count_result = meta_es.count(count_query, index=settings.es_index, 
        doc_type=settings.es_doc_type)

    num_results = int(count_result['count'])
    logger.debug(num_results)

    search_query = get_search_query(query_string, size=num_results)

    logger.debug(search_query)
    search_results = meta_es.search(search_query, index=settings.es_index, 
        doc_type=settings.es_doc_type)

    print json.dumps(search_results, indent=4)

    num_hits = len(search_results['hits']['hits'])

    if num_results != num_hits:
        logger.warning('Count returned %d results, '
            'while search returned %d results' % (num_results, num_hits))

    osdc_status = get_osdc_status(search_results, settings.cdb_url, settings.cdb_osdc)
    print('osdc_status')
    print(osdc_status)

    if num_hits < 1:
        loxgger.info("Query returned 0 results")
        manifest = {}
    else:
        manifest = builder.associate(search_results, osdc_status)

    print(manifest)

    if len(manifest) < 1:
        logger.info("No links to be created")

    if options.update:
        logger.info("Updating directory %s" % new_dir)
    else:
        logger.info("Making directory %s" % new_dir)
        fs_handler.mkdir(new_dir)

    num_links = 0

    for key, value in manifest.items():
        print(manifest[key])
        if 'error' in value:
            manifest[key]['linked'] = False
            continue

        if 'md5_ok' not in value:
            manifest[key]['linked'] = False
            continue

        if value['md5_ok'] == False:
            manifest[key]['linked'] = False
            continue

        target = value['target']
        link_name = value['link_name']
        exists = fs_handler.exists(target)

        if not exists:
            manifest[key]['error'] = 'not_found_linking'
            if not options.dangle:
                manifest[key]['linked'] = False
                continue

        try:
            fs_handler.symlink(target, link_name)
            manifest[key]['linked'] = True
            num_links += 1
        except Exception as e:
            manifest[key]['error'] = str(e)
            manifest[key]['linked'] = False

    summary = create_summary(query_name, query_url, query_string, options.config,
        len(manifest), num_links)

    fs_handler.write_summary(new_dir, summary)
    fs_handler.write_manifest(new_dir, json.dumps(manifest, indent=4))
  

if __name__ == "__main__":
    main()
