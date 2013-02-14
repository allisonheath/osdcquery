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
'''Tool to list the fields of of a query interface given a url'''

import importlib

from optparse import OptionParser
from util import get_class
from util import get_simple_logger
from util import shared_options


def main():
    '''Prints field list to stdout'''

    usage = "( python -m osdcquery.fieldlist | %prog ) [options] url"

    parser = OptionParser(usage=usage)

    shared_options(parser)

    (options, args) = parser.parse_args()

    logger = get_simple_logger(options.loglevel, options.verbose)

    num_args = 1

    if len(args) != num_args:
        parser.error("incorrect number of arguments")

    url = args[0]

    settings = importlib.import_module(options.config)

    field_list_class = get_class(settings.field_module_name,
        settings.field_class_name)

    field_list = field_list_class(url)

    fields = field_list.attributes()

    for field in fields:
        print "--%s" % field,

if __name__ == "__main__":
    main()
