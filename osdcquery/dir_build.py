#!/usr/bin/python

# Apache 2 license

'''Dir builder class for parsing TCGA style elastic search JSON '''

import os


class TcgaDirBuild(object):
    ''' Returns a dictionary of filepath and names with the keys being
    symlinks to create and the values being the original files to be the
    targets of the links '''

    def __init__(self, old_dir, new_dir):
        self.old_dir = old_dir
        self.new_dir = new_dir

    def associate(self, es_results, osdc_status, label='_source'):
        ''' Create files using the metadata dictionary.'''

        file_data = {}

        for entry in es_results['hits']['hits']:
            key = entry['_id']
            file_data[key] = {}
            #might want to check that this exists?
            status = osdc_status[key]

            for k,v in status.items():
                file_data[key][k] = v

            file_data[key]['files'] = entry[label]['files']

            file_data[key]['link_name'] = os.path.join(self.new_dir, 
                entry[label]['analysis_id'])
            file_data[key]['target'] = os.path.join(self.old_dir,
                    entry[label]['disease_abbr'],
                    entry[label]['analysis_id'],
                )

        return file_data