#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno

import oss2
import configparser
import time

from fuse import FUSE, FuseOSError, Operations

def log(*args):
    print(*args)

class OSS(Operations):
    def __init__(self, bucket):
        self.bucket = bucket
        self.uid = os.getuid()
        self.gid = os.getgid()
        self.attrs = {
                '/': dict(type='d', lastModified=int(time.time())),
                }

    # Helpers
    # =======

    def _fileattr(self, size, lastModified):
        return dict(st_atime=lastModified, st_ctime=lastModified,
                st_gid=self.gid, st_mode=0o100644, st_mtime=lastModified,
                st_size=size, st_uid=self.uid)
    def _dirattr(self, lastModified):
        return dict(st_atime=lastModified, st_ctime=lastModified,
                st_gid=self.gid, st_mode=0o40755, st_mtime=lastModified,
                st_size=4096, st_uid=self.uid)

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        log('access', path, mode)

    def chmod(self, path, mode):
        log('chmod', path, mode)

    def chown(self, path, uid, gid):
        log('chown', path, uid, gid)

    def getattr(self, path, fh=None):
        log('getattr', path, fh)
        if path not in self.attrs:
            raise FileNotFoundError
        attr = self.attrs[path]
        if attr['type'] == 'd':
            return self._dirattr(attr['lastModified'])
        else:
            return self._fileattr(attr['size'], attr['lastModified'])

    def readdir(self, path, fh):
        length = len(path)
        if length == 1:
            prefix = ''
            offset = 1
        else:
            prefix = path[1:] + '/'
            offset = length + 1
        log('readdir', path, fh, offset, prefix)
        result = self.bucket.list_objects(prefix=prefix, delimiter='/')

        dirents = ['.', '..']
        check = set(dirents)
        for d in result.prefix_list:
            fullpath = '/'+d[:-1]
            value = fullpath[offset:]
            if len(value) > 0 and value not in check:
                check.add(value)
                dirents.append(value)
                self.attrs[fullpath] = dict(type='d', lastModified=int(time.time()))
        for f in result.object_list:
            fullpath = '/'+f.key
            value = fullpath[offset:]
            if len(value) > 0 and value not in check:
                check.add(value)
                dirents.append(value)
                self.attrs[fullpath] = dict(type='f', size=f.size, lastModified=f.last_modified)

        for r in dirents:
            yield r

    def readlink(self, path):
        log('readlink', path)
        return path

    def mknod(self, path, mode, dev):
        log('mknod', path, mode, dev)
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        log('rmdir', path)
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        log('mkdir', path, mode)
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        log('statfs', path)
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        log('unlink', path)
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        log('symlink', name, target)
        return os.symlink(name, self._full_path(target))

    def rename(self, old, new):
        log('rename', old, new)
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        log('link', target, name)
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        log('utimens', path, times)
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        log('open', path, flags)
        return 0

    def create(self, path, mode, fi=None):
        log('create', path, mode, fi)
        return 0

    def read(self, path, length, offset, fh):
        log('read', path, length, offset, fh)
        if path not in self.attrs:
            raise FileNotFoundError
        attr = self.attrs[path]
        if attr['type'] == 'd':
            raise ValueError('file needed, directory used')
        size = attr['size']
        begin = min(size, offset) - 1
        end = min(size, offset+length) - 1
        amount = min(size, length)
        return self.bucket.get_object(path[1:], byte_range=(begin, end)).read(amount)

    def write(self, path, buf, offset, fh):
        log('write', path, buf, offset, fh)
        return 0;

    def truncate(self, path, length, fh=None):
        log('truncate', path, length, fh)

    def flush(self, path, fh):
        log('flush', path, fh)

    def release(self, path, fh):
        log('release', path, fh)

    def fsync(self, path, fdatasync, fh):
        log('fsync', path, fdatasync, fh)


def getConfig(configFile):
    cp = configparser.ConfigParser()
    cp.read(configFile)
    oss = cp['aliyun-oss']
    accessKeyId = oss['access-key-id']
    accessKeySecret = oss['access-key-secret']
    endpoint = oss['endpoint']
    bucketName = oss['bucket-name']
    return accessKeyId, accessKeySecret, endpoint, bucketName

def main(configFile, mountpoint):
    accessKeyId, accessKeySecret, endpoint, bucketName = getConfig(configFile)
    bucket = oss2.Bucket(oss2.Auth(accessKeyId, accessKeySecret), endpoint, bucketName)
    FUSE(OSS(bucket), mountpoint, nothreads=True, foreground=False)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
