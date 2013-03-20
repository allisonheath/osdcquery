#  Copyright 2013 Open Cloud Consortium
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

'''Runs query and builds symlinks '''

import importlib
import json
import os
import os.path
import requests

from optparse import OptionParser
from util import get_class
from util import get_simple_logger
from util import shared_options
from query import ESQuery, ESQueryMetadata

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
        help="update files in query directory using summary file",
        default=False)

    parser.add_option("-m", "--from_manifest", action="store_true", 
        dest="from_manifest", help="use manifest to generate symlinks",
        default=False)

    (options, args) = parser.parse_args()

    logger = get_simple_logger(options.loglevel, options.verbose)

    settings = importlib.import_module(options.config)

    target_dir = options.target_dir if options.target_dir else settings.target_dir

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

    fs_handler_class = get_class(settings.fs_handler_module_name, settings.fs_handler_class_name)

    fs_handler = fs_handler_class()

    dirbuild_class = get_class(settings.dirbuild_module_name,
            settings.dirbuild_class_name)

    top_dir = os.path.join(link_dir, query_name)

    if options.update:
        esq_meta = ESQueryMetadata(dirbuild_class, fs_handler, top_dir, target_dir)
        esq_meta.read_metadata()
        esq_meta.update_query_and_symlinks(options.dangle)
        esq_meta.write_metadata()

    else:
        if len(args) == 2:
            query_url = settings.es_url
            query_string = args[1]
        else:
            query_url = args[1]
            query_string = args[2]

        if fs_handler.exists(top_dir):
            error_message = "Directory '%s' already exists, use -u to update" % top_dir
            logger.error(error_message)
            exit(1)


        esq = ESQuery(query_url, query_string, query_name, settings.es_index, settings.es_doc_type, 
            settings.cdb_url, settings.cdb_osdc)

        results, status = esq.perform_query_with_status()

        esq_meta = ESQueryMetadata(dirbuild_class, fs_handler, top_dir, target_dir, esq)

        esq_meta.create_symlinks(options.dangle)

        esq_meta.write_metadata()
    

if __name__ == "__main__":
    main()
