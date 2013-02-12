#!/usr/bin/python

# Apache 2 License

'''Runs query and builds symlinks '''

import datetime
import importlib
import json
import logging
import logging.handlers
import os

from optparse import OptionParser


QUERY_URL = "URL"
QUERY_STRING = "Query"

def get_class(module_name, class_name):
    ''' Return class from module and class name '''

    module = importlib.import_module(module_name)
    return getattr(module, class_name)

def create_manifest(query_name, query_url, query_string, config, num_files):
    ''' Format and create a manifest file that describes the results of
    the query operation.  This manifest file will be used for updating queries
    '''
    return json.dumps({
        "Query Name": query_name,
        QUERY_URL: query_url,
        QUERY_STRING: query_string,
        "Configuration Module": config,
        "Number of Files Linked": num_files,
        "Date of Query": datetime.datetime.now().strftime('%b-%d-%I%M%p-%G')
    }, sort_keys=True, indent=4)

def main():
    '''Runs query and builds symlinks '''

    usage = "( python -m osdcquery.osdcquery | %prog ) [options] query_name \
url query_string"

    parser = OptionParser(usage=usage)
    parser.add_option("-t", "--target_dir", dest="target_dir",
        help="target directory (where the original files are located)")

    parser.add_option("-l", "--link_dir", dest="link_dir",
        help="link directory (where to put the generated symlinks)")

    parser.add_option("-c", "--config", dest="config",
        help="config module to use standard is osdcquery.config.Tcga",
        default="osdcquery.config.Tcga")

    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
        help="display messages to standard out", default=False)

    parser.add_option("-d", "--log", dest="loglevel",
        help="python log level DEBUG, INFO ...", default="ERROR")

    parser.add_option("-i", "--dangle", dest="dangle", action="store_true",
        help="ignore nonexisting target file; create dangling link",
            default=False)

    parser.add_option("-u", "--update", action="store_true", dest="update",
        help="update files in query directory using .info file",
        default=False)

    (options, args) = parser.parse_args()

    numeric_level = getattr(logging, options.loglevel.upper(), None)

    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)

    if options.verbose and numeric_level > logging.INFO:
        numeric_level = logging.INFO

    logger = logging.getLogger('osdcquery')
    logger.setLevel(numeric_level)
    logger.addHandler(logging.StreamHandler())

    settings = importlib.import_module(options.config)

    target_dir = options.target_dir if options.target_dir else \
        settings.target_dir

    link_dir = options.link_dir if options.link_dir else settings.link_dir

    num_args = 3
    if options.update:
        num_args = 1

    if len(args) != num_args:
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
        query_url = args[1]
        query_string = args[2]

    query_class = get_class(settings.query_module_name,
        settings.query_class_name)

    query = query_class(query_url, settings.query_fields)

    dirbuild_class = get_class(settings.dirbuild_module_name,
        settings.dirbuild_class_name)

    builder = dirbuild_class(target_dir, os.path.join(link_dir, query_name))


    if fs_handler.exists(new_dir) and not options.update:
        error_message = 'Directory "%s" already exists' % new_dir
        logger.error(error_message)
        exit(1)

    if options.update:
        logger.info("Updating directory %s" % new_dir)
    else:
        logger.info("Making directory %s" % new_dir)
        fs_handler.mkdir(new_dir)

    query_results = query.run_query(query_string)

    logger.debug(query_results)

    if len(query_results) < 1:
        print "Query returned 0 results"
        links = {}
    else:
        links = builder.associate(query_results)

    if len(links) < 1:
        print "No links to be created"


    num_files = 0

    for link, target in links.items():
        exists = fs_handler.exists(target)
        if not exists:
            logger.warning("File %s does not exist on disk." % target)
        if exists or options.dangle:
            logger.info("Creating link %s to target %s" % (link, target))
            fs_handler.symlink(target, link)
            num_files += 1

    manifest = create_manifest(query_name, query_url, query_string, options.config,
        num_files)

    fs_handler.write_manifest(new_dir, manifest)

if __name__ == "__main__":

    main()
