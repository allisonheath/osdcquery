#!/usr/bin/python

# Apache 2 License

'''Runs query and builds symlinks '''

import datetime
import importlib
import json
import os
import os.path

from optparse import OptionParser
from util import get_class
from util import get_simple_logger
from util import shared_options


QUERY_URL = "URL"
QUERY_STRING = "Query"

def create_manifest(query_name, query_url, query_string, config, num_files,
    num_links):
    ''' Format and create a manifest file that describes the results of
    the query operation.  This manifest file will be used for updating queries
    '''
    return json.dumps({
        "Query Name": query_name,
        QUERY_URL: query_url,
        QUERY_STRING: query_string,
        "Configuration Module": config,
        "Number of Files Found": num_files,
        "Number of Files Linked": num_links,
        "Date of Query": datetime.datetime.now().strftime('%b-%d-%I%M%p-%G')
    }, sort_keys=True, indent=4)

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
            query_url = settings.url
            query_string = args[1]
        else:
            query_url = args[1]
            query_string = args[2]

    if fs_handler.exists(new_dir) and not options.update:
        error_message = 'Directory "%s" already exists' % new_dir
        logger.error(error_message)
        exit(1)

    query_class = get_class(settings.query_module_name,
        settings.query_class_name)

    query = query_class(query_url, settings.query_fields,
        settings.non_disease_dir)

    dirbuild_class = get_class(settings.dirbuild_module_name,
        settings.dirbuild_class_name)

    builder = dirbuild_class(target_dir, os.path.join(link_dir, query_name))

    query_results = query.run_query(query_string)

    logger.debug(query_results)

    if len(query_results) < 1:
        print "Query returned 0 results"
        links = {}
    else:
        links = builder.associate(query_results)

    if len(links) < 1:
        print "No links to be created"

    if options.update:
        logger.info("Updating directory %s" % new_dir)
    else:
        logger.info("Making directory %s" % new_dir)
        fs_handler.mkdir(new_dir)

    num_links = 0

    for link, target in links.items():
        exists = fs_handler.exists(target)
        if not exists:
            logger.warning("File %s does not exist on disk." % target)
        if exists or options.dangle:
            logger.info("Creating link %s to target %s" % (link, target))
            fs_handler.symlink(target, link)
            num_links += 1

    manifest = create_manifest(query_name, query_url, query_string, options.config,
        len(links), num_links)

    fs_handler.write_manifest(new_dir, manifest)

if __name__ == "__main__":

    main()
