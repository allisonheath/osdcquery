#!/usr/bin/python

# Apache 2 License

'''Runs query and builds symlinks '''

import importlib
import logging
import logging.handlers
import os

from optparse import OptionParser


def get_class(module_name, class_name):
    ''' Return class from module and class name '''

    module = importlib.import_module(module_name)
    return getattr(module, class_name)


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

    (options, args) = parser.parse_args()

    numeric_level = getattr(logging, options.loglevel.upper(), None)

    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)

    logger = logging.getLogger('osdcquery')
    logger.setLevel(numeric_level)
    logger.addHandler(logging.StreamHandler())

    settings = importlib.import_module(options.config)

    target_dir = options.target_dir if options.target_dir else \
        settings.target_dir

    link_dir = options.link_dir if options.link_dir else settings.link_dir

    if len(args) != 3:
        parser.error("incorrect number of arguments")

    query_name = args[0]
    query_url = args[1]
    query_string = args[2]

    query_class = get_class(settings.query_module_name,
        settings.query_class_name)

    query = query_class(query_url, settings.query_fields)

    dirbuild_class = get_class(settings.dirbuild_module_name,
        settings.dirbuild_class_name)

    builder = dirbuild_class(target_dir, os.path.join(link_dir, query_name))

    fs_handler_class = get_class(settings.fs_handler_module_name,
        settings.fs_handler_class_name)

    fs_handler = fs_handler_class()

    new_dir = os.path.join(link_dir, query_name)

    if options.verbose:
        print "Making directory %s" % new_dir

    query_results = query.run_query(query_string)

    logger.debug(query_results)
    
    links = builder.associate(query_results)

    fs_handler.mkdir(new_dir)

    for link, target in links.items():
        if options.verbose:
            print "Creating link %s to target %s" % (link, target)
        fs_handler.symlink(target, link)

if __name__ == "__main__":

    main()
