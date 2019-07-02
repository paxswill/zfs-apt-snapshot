#!/usr/bin/env python3
"""Script that creates snapshots before upgrading packages through APT.

This script is meant to be configured as a script for the ``apt.conf`` option
``Dpkg::Pre-Install-Pkgs``. It supports up to version 3 of the argument
protocol, which is set with
``Dpkg::Tools::options::zfs-apt-snapshot::Version "3";``

The ZFS Python bindings are used if available, but the script will fall back to
the the scripting interface if it isn't. The Python APT bindings are required
(installable with the ``python-apt`` package). This script requires at least
Python 3.5.
"""

import collections
import datetime
import functools
import locale
import os.path
import subprocess
import sys

from apt.debfile import DebPackage
try:
    import libzfs_core as zfs
    import libzfs_core.exceptions
except ImportError:
    _lzc_snapshot = None
    _lzc_snap = None
    _lzc_list_snaps = None
else:
    _lzc_snapshot = getattr(zfs, "lzc_snapshot", None)
    if not zfs.is_supported(_lzc_snapshot):
        _lzc_snapshot = None
    _lzc_snap = getattr(zfs, "lzc_snap", None)
    if not zfs.is_supported(_lzc_snap):
        _lzc_snap = None
    _lzc_list_snaps = getattr(zfs, "lzc_list_snaps", None)
    if not zfs.is_supported(_lzc_list_snaps):
        _lzc_list_snaps = None


# Get the current default locale early on
default_encoding = locale.getpreferredencoding()


class SnapshotCreationError(Exception): pass


class SnapshotExists(SnapshotCreationError): pass


class ZFSListError(Exception): pass


def ensure_bytes(func):
    """Helper decorator that ensures the name argument to a function is bytes.
    """
    @functools.wraps(func)
    def inner(*args, **kwargs):
        if "name" in kwargs and isinstance(kwargs["name"], str):
            kwargs["name"] = kwargs["name"].encode(default_encoding)
        args = [a.encode(default_encoding) if isinstance(a, str) else a for a in args]
        return func(*args, **kwargs)
    return inner


# Provide fallbacks to unimplemented libzfs_core functions by shelling out
for lzc_func in (_lzc_snapshot, _lzc_snap):
    if lzc_func is not None:
        @ensure_bytes
        def create_snapshot(name):
            try:
                lzc_func([name])
            except zfs.exceptions.SnapshotExists as e:
                raise SnapshotExists() from e
        break
else:
    @ensure_bytes
    def create_snapshot(name):
        if isinstance(name, str):
            ret = subprocess.run(
                [b"zfs", b"subprocess", name],
                check=False,
                stderr=subprocess.STDOUT,
                stdout=subprocess.PIPE
            )
            if ret.returncode != 0:
                # TODO do further checking about what kind of error this is
                raise SnapshotCreationError(ret.stdout)


if _lzc_list_snaps is not None:
    @ensure_bytes
    def list_snapshots(name):
        try:
            return zfs.lzc_list_snaps(name)
        except zfs.exceptions.ZFSError as e:
            raise ZFSListError() from e
else:
    def list_snapshots(name):
        return _zfs_list(name, type_=b"snapshot")


@ensure_bytes
def _zfs_list(*names, type_=None):
    # kwargs aren't converted by ensure_bytes
    if isinstance(type_, str):
        type_ = type_.encode(default_encoding)

    valid_types = {b"snapshot", b"filesystem", b"volume", b"bookmark", b"all"}
    if type_ not in valid_types:
        raise ValueError("'{}' is not a valid type ZFS type.".format(
            type_.decode(default_encoding)
        ))

    args = [b"zfs", b"list", b"-H", b"-t", type_, b"-o", b"name", *names]
    ret = subprocess.run(
        args,
        check=False,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE
    )
    if ret.returncode != 0:
        raise ZFSListError(ret.stderr)
    else:
        # strip() the output to trim trailing newlines
        return ret.stdout.strip().split(b"\n")


def get_filesystems(*paths):
    """Return the names of the ZFS filesystems the given paths exist on."""
    return _zfs_list(*paths, type_=b"filesystem")


def directories_for_package(pkg):
    """Return a list of the directories a package is modifying."""
    directories = set()
    for path in pkg.filelist:
        if path[-1] != "/":
            dirname = os.path.dirname(path)
            if dirname[0] != "/":
                dirname = "/" + dirname
            directories.add(dirname)
    return directories


def filesystems_for_packages(pkgs):
    """Return a list of ZFS filesystems modified by the given packages."""
    all_directories = set()
    for pkg in pkgs:
        all_directories.update(directories_for_package(pkg))

    filesystems = set()
    # convert the directory list to a queue so we can put things at the end for
    # later processing
    all_directories = collections.deque(all_directories)
    while all_directories:
        d = all_directories.popleft()
        try:
            f = get_filesystems(d)
            filesystems.update(f)
        except ZFSListError:
            # If there's an error, the path probably doesn't exist yet, so find
            # the filesystem for the parent directory (and so on).
            new_d = os.path.dirname(d)
            if d != new_d:
                all_directories.append(new_d)
            else:
                raise
    return filesystems


def main(source):
    # Read the list of packages in
    pkgs = []
    while True:
        pkg_path = source.readline().strip()
        if pkg_path == "":
            break
        pkg = DebPackage(filename=pkg_path)
        pkgs.append(pkg)
    filesystems = filesystems_for_packages(pkgs)

    # Choose a name for the snapshot
    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H%M")
    snapshot_name = "zfs-apt-snap_{}".format(timestamp)
    # This mess of decode()+encode() is because there isn't a format() method
    # for bytes.
    filesystem_snapshots = [
        "{}@{}".format(
            f.decode(default_encoding),
            snapshot_name)
        .encode(default_encoding)
        for f in filesystems
    ]
    # Create the snapshots
    for snapshot in filesystem_snapshots:
        print("Creating snapshot", snapshot)
        create_snapshot(snapshot)


if __name__ == "__main__":
    main(sys.stdin)

