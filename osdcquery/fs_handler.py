#!/usr/bin/python

# Apache 2 license

''' Probably uneeded cruft so that if needed we can have a Windows class
that will have symlink be a wrapper for Win32file.CreateSymbolicLink
'''

import os
import os.path


class UnixFsHandler(object):
    ''' Given a dictionary of link names and targets create those symlinks
    '''

    def symlink(self, target, link_name):
        ''' Wrapper for os.symlink'''
        os.symlink(target, link_name)

    def mkdir(self, name):
        ''' Wrapper for os.makedirs want to have mkdir -p'''
        os.makedirs(name)

    def exists(self, path):
        ''' Wrapper for os.path.exists'''
        return os.path.exists(path)

    def write_file(self, file, content):
        f = open(file, 'w')
        f.write(content)
        f.close()

    def read_file(self, file):
        f = open(file)
        content = f.read()
        f.close()
        return content
        