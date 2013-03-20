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

'''Dir builder class for parsing TCGA style elastic search JSON '''

import os

class TcgaLinks(object):
    ''' Returns a dictionary of filepath and names with the keys being
    symlinks to create and the values being the original files to be the
    targets of the links '''

    def __init__(self, old_dir, new_dir):
        self.old_dir = old_dir
        self.new_dir = new_dir

    def associate(self, query, label='_source'):
        ''' Create files using the metadata dictionary.'''
       
        file_data = {}

        results = query.query_results
        status_results = query.status_results

        for entry in results['hits']['hits']:
            key = entry['_id']
            file_data[key] = {}
            #might want to check that this exists?
            status = status_results[key]

            for k,v in status.items():
                file_data[key][k] = v

            file_data[key]['files'] = entry[label]['files']

            file_data[key]['link_name'] = os.path.join(self.new_dir,
                entry[label]['analysis_id'])

            #need to check for valid disease_abbr?
            file_data[key]['target'] = os.path.join(self.old_dir,
                    entry[label]['disease_abbr'],
                    entry[label]['analysis_id'],
                )

        return file_data

   
