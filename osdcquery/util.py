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
'''Functions to prevent duplicating code in main for different tools'''

import importlib
import logging
import logging.handlers


def shared_options(parser):
    '''parser is an OptionParser object add the following options to it '''

    parser.add_option("-c", "--config", dest="config",
        help="config module to use standard is osdcquery.config.Tcga",
        default="osdcquery.config.Tcga")

    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
        help="display messages to standard out", default=False)

    parser.add_option("-d", "--log", dest="loglevel",
        help="python log level DEBUG, INFO ...", default="ERROR")

def get_simple_logger(loglevel, verbose):
    '''Return a basic logger to stdout with loglevel loglevel where loglevel
    is a string such as "DEBUG", "INFO", "WARNING"... and verbose is a boolean
    '''

    numeric_level = getattr(logging, loglevel.upper(), None)

    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)

    if verbose and numeric_level > logging.INFO:
        numeric_level = logging.INFO

    logger = logging.getLogger('osdcquery')
    logger.setLevel(numeric_level)
    sh = logging.StreamHandler()
    sh.setLevel(numeric_level)
    sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s : %(message)s'))
    logger.addHandler(sh)

    return logger

def get_class(module_name, class_name):
    ''' Return class from module and class name '''

    module = importlib.import_module(module_name)
    return getattr(module, class_name)

