#!/usr/bin/python

# Apache 2 license

''' Probably uneeeded cruft so that if needed we can have a Windows class
that will have symlink be a wrapper for Win32file.CreateSymbolicLink
'''

import os


class UnixFsHandler(object):
    ''' Given a dictionary of link names and targets create those symlinks
    '''

    def symlink(self, target, link_name):
        ''' Wrapper for os.symlink'''
        os.symlink(target, link_name)

    def mkdir(self, name):
        ''' Wrapper for os.makedirs want to have mkdir -p'''
        os.makedirs(name)
