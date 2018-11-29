"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree. An additional grant
of patent rights can be found in the PATENTS file in the same directory.
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import six
import stat
import logging

from .error import OSCError

if six.PY2:
    import subprocess32 as subprocess
else:
    import subprocess

log = logging.getLogger(__name__)


def rm(filename, sudo=False):
    """
    Remove a file on the disk, not us os.rm because we want to add timeout to
    the command. It's possible that the os call got hang when the disk has
    some problems
    """
    cmd_args = []
    if sudo:
        cmd_args += ['sudo']
    cmd_args += ['/bin/rm', filename]
    log.debug("Executing cmd: {}".format(str(cmd_args)))
    proc = subprocess.Popen(cmd_args, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE)
    try:
        (stdout, stderr) = proc.communicate(timeout=20)
        # return True if returncode is success (0)
        return not proc.returncode
    except subprocess.TimeoutExpired:
        proc.kill()
        raise OSCError('SHELL_TIMEOUT', {'cmd': ' '.join(cmd_args)})


def sync_dir(dirname):
    """
    Calling fsync on the directory. This is for synching
    deleted files to storage devices to prevent trim stalls.
    """
    dirfd = os.open(dirname, os.O_DIRECTORY)
    os.fsync(dirfd)
    os.close(dirfd)


def is_file_readable(filepath):
    """
    Check if the file given is readable to the user we are currently running
    at
    """
    uid = os.getuid()
    euid = os.geteuid()
    gid = os.getgid()
    egid = os.getegid()

    # This is probably true most of the time, so just let os.access()
    # handle it.  Avoids potential bugs in the rest of this function.
    if uid == euid and gid == egid:
        return os.access(filepath, os.R_OK)

    st = os.stat(filepath)

    if st.st_uid == euid:
        return st.st_mode & stat.S_IRUSR != 0

    groups = os.getgroups()
    if st.st_gid == egid or st.st_gid in groups:
        return st.st_mode & stat.S_IRGRP != 0

    return st.st_mode & stat.S_IROTH != 0


def disk_partition_free(path):
    """
    For given file path, return the size of free space in bytes of the
    underlying disk

    @param path:  Full path string for which the disk space we need to get
    @type  path:  string

    @return:  free disk space in btyes
    @rtype :  int

    """
    try:
        vfs = os.statvfs(path)
        return vfs.f_bavail * vfs.f_bsize
    except Exception:
        log.exception("Exception when trying to get disk free space: ")
        raise OSCError('UNABLE_TO_GET_FREE_DISK_SPACE', {'path': path})


def disk_partition_size(path):
    """
    For given file path, return the total size in bytes of the
    underlying disk partition

    @param path:  Full path in the partition for which the size we need to get
    @type  path:  string

    @return:  total size in btyes
    @rtype :  int

    """
    try:
        vfs = os.statvfs(path)
        return vfs.f_blocks * vfs.f_bsize
    except Exception:
        log.exception("Exception when trying to get partition size: ")
        raise OSCError('UNABLE_TO_GET_PARTITION_SIZE', {'path': path})


def readable_size(nbytes):
    """
    Translate a number representing byte size into a human readable form
    @param nbytes:  number representing bytes
    @type  nbytes:  int

    @return:  readable size
    @rtype :  string
    """
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    if nbytes == 0:
        return '0 B'
    i = 0
    while nbytes >= 1024 and i < len(suffixes) - 1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])


class RangeChain(object):
    """
    A memory efficient class for memorize all the points that we've filled
    within a range containing consecutive natural values.
    Knowing that the missing points are much less than the number of filling
    points, we only store missing points and record a end point of the range
    """
    def __init__(self):
        self._stop = 0
        self._gap = []

    def extend(self, points):
        last_point = self._stop
        for current_point in points:
            # If it's consecutive then we should just extend the stop point
            if current_point != last_point + 1:
                for gap_point in range(last_point + 1, current_point):
                    self._gap.append(gap_point)

            self._stop = current_point
            last_point = current_point

    def fill(self, point):
        if point in self._gap:
            self._gap.remove(point)
        else:
            if point > self._stop:
                raise Exception(
                    "Trying to fill a value {} "
                    "beyond current covering range"
                    .format(point))
            else:
                raise Exception(
                    "Trying to fill a value {} which already exists"
                    .format(point))

    def missing_points(self):
        return self._gap
