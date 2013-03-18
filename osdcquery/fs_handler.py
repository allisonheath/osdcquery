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

    def write_manifest(self, path, manifest):
        ''' Write json file in path consisting of string manifest'''
        f = open(os.path.join(path, "MANIFEST.json"), 'w')
        f.write(manifest)
        f.close()

    def read_manifest(self, path):
        ''' read and return json from path'''
        f = open(os.path.join(path, "MANIFEST.json"))
        manifest = f.read()
        f.close()
        return manifest

    def write_summary(self, path, summary):
        ''' Write json file in path consisting of string summary'''
        f = open(os.path.join(path, "SUMMARY.json"), 'w')
        f.write(summary)
        f.close()

    def read_summary(self, path):
        ''' read and return .info from path'''
        f = open(os.path.join(path, "SUMMARY.json"))
        summary = f.read()
        f.close()
        return summary

