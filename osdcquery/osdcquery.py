#!/usr/bin/python

# Apache 2 License

'''Runs query and builds symlinks '''

import importlib
import os

from optparse import OptionParser


def get_class(module_name, class_name):
    ''' Return class from module and class name '''

    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def main():
    '''Runs query and builds symlinks '''

    usage = " ( python -m osdcquery.osdcquery |  %prog ) [options] arg"

    parser = OptionParser(usage=usage)
    parser.add_option("-t", "--target", dest="target",
        help="target directory (where the original files are located)")

    parser.add_option("-l", "--link", dest="link",
        help="link directory (where to put the generated symlinks)")

    parser.add_option("-c", "--config", dest="config",
        help="config module to use standard is osdcquery.config.Tcga",
        default="osdcquery.config.Tcga")

    (options, args) = parser.parse_args()

    settings = importlib.import_module(options.config)

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

    builder = dirbuild_class(settings.target_dir, os.path.join(
        settings.link_dir, query_name))

    fs_handler_class = get_class(settings.fs_handler_module_name,
        settings.fs_handler_class_name)

    fs_handler = fs_handler_class()

    new_dir = os.path.join(settings.link_dir, query_name)

    fs_handler.mkdir(new_dir)
    links = builder.associate(query.run_query(query_string))
    for link, target in links.items():
        fs_handler.symlink(target, link)

if __name__ == "__main__":

    main()
