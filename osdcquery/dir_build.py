#!/usr/bin/python

# Apache 2 license

'''Dir builder class for pasing TCGA style elastic search JSON '''

import os


class TcgaDirBuild(object):
    ''' Returns a dictionary of filepath and names with the keys being
    symlinks to create and the values being the original files to be the
    targets of the links '''

    def __init__(self, old_dir, new_dir):
        self.old_dir = old_dir
        self.new_dir = new_dir

    def associate(self, metadata):
        ''' Create files using the metadata dictionary.'''

        return {
            #os.path.join(self.new_dir,  entry['files'][0]['filename']):
            os.path.join(self.new_dir,  entry['analysis_id']):
            os.path.join(self.old_dir,
                entry['disease_abbr'],
                entry['analysis_id']#,
                #entry['files'][0]['filename']
            )
            for entry in metadata
        }
