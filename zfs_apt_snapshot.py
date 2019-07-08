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
import enum
import functools
import locale
import operator
import os
import pathlib
import subprocess
import sys

import apt
from apt.debfile import DebPackage
# Try to use the libzfs_code library functions when possible
try:
    import libzfs_core as zfs
    import libzfs_core.exceptions
except ImportError:
    _lzc_snapshot = None
    _lzc_snap = None
    _lzc_list_snaps = None
    _lzc_get_props = None
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
    _lzc_get_props = getattr(zfs, "lzc_get_props", None)
    if not zfs.is_supported(_lzc_get_props):
        _lzc_get_props = None


# Get the current default locale early on
default_encoding = locale.getpreferredencoding()


class SnapshotCreationError(Exception): pass


class SnapshotExists(SnapshotCreationError): pass


class ZFSListError(Exception): pass


class ZFSGetPropertiesError(Exception): pass


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
                [b"zfs", b"snapshot", name],
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


if _lzc_get_props is not None:
    @ensure_bytes
    def get_dataset_props(name):
        try:
            return _lzc_get_props(name)
        except zfs.exceptions.ZFSError as e:
            raise ZFSGetPropertiesError() from e
else:
    @ensure_bytes
    def get_dataset_props(name):
        args = [
            b"zfs",
            b"get",
            b"-o",
            b"property,value",
            b"-p",
            b"-H",
            b"all",
            name,
        ]
        ret = subprocess.run(
            args,
            check=False,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE
        )
        if ret.returncode != 0:
            raise ZFSGetPropertiesError(ret.stderr)
        else:
            stdout = ret.stdout.strip()
            properties = {}
            for line in (l.strip() for l in ret.stdout.split(b"\n")):
                if line == b"":
                    # Skip blank lines (like at the end of the output).
                    continue
                name, value = line.split(b"\t")
                # convert boolean vlues to Python bools. Not converting other
                # types as blindly converting tings to ints and floats leads to
                # problems later if you're not 100% sure whjat type they're
                # supposed to be.
                if value.lower() in {b"on", b"true"}:
                    value = True
                elif value.lower() in {b"off", b"false"}:
                    value = False
                # convert the name to a python str as it's a human-readable
                # identifier
                name = name.decode(default_encoding)
                properties[name] = value
            return properties


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
    if hasattr(pkg, "filelist"):
        # apt.debfile.DebPackage
        path_strs = pkg.filelist
    elif hasattr(pkg, "installed_files"):
        # apt.Package
        path_strs = pkg.installed_files
    # Figure out the root path to add to relative paths
    first_path = path_strs[0]
    if first_path in {"./", "/.", "/"}:
        path_prefix = pathlib.PurePosixPath("/")
    paths = (
        pathlib.PurePosixPath(p)
        for p in path_strs
        # Skip root directores and empty paths
        if p not in {"./", "/.", "", "."}
    )
    for path in paths:
        # Remove any parent directories in the set, as we only want leaf
        # entries
        if not path.is_absolute():
            path = path_prefix / path
        directories.difference_update(path.parents)
        directories.add(path)
    return directories


def filesystems_for_files(files):
    """Return a list of ZFS filesystems modified by the given packages."""
    filesystems = set()
    # convert the path list to a queue so we can put things at the end for
    # later processing
    paths = collections.deque(files)
    while paths:
        path = paths.popleft()
        try:
            # We need to convert the PurePath to a string for processing by the
            # ZFS API.
            filesystem = get_filesystems(str(path))
            filesystems.update(filesystem)
        except ZFSListError:
            # If there's an error, the path probably doesn't exist yet, so find
            # the filesystem for the parent directory (and so on).
            if path.parent != path:
                paths.append(path.parent)
            else:
                raise
    return filesystems


def get_files(stream):
    """Reads the information stream and returns the directories being changed.

    This supports versions 1, 2, and 3 of the information protocol.
    """
    packages = []
    # First detect which version of the description protocol we're getting
    line = stream.readline().strip()
    # Doing this more complicated version checking to guard against a newer
    # version being sent without supporting it.
    version_prefix = "VERSION "
    if line.startswith(version_prefix):
        version = int(line[len(version_prefix):])
    else:
        version = 1
    # Check for unsupported versions
    if not (1 <= version <= 3):
        print(
            (
                "ERROR: Unsupported APT helper configuration protocol version "
                "({})!"
            ).format(version)
        )
        sys.exit(1)
    if version == 1:
        # handle the version 1 case first, it's simple
        while line != "":
            pkg = DebPackage(filename=line)
            packages.append(pkg)
            line = stream.readline().strip()
    else:
        line = stream.readline().strip()
        # The first block is the APT configuration space, which is terminated
        # with a blank line. We don't care about the info in there, so we skip
        # it
        while line != "":
            line = stream.readline().strip()
        line = stream.readline().strip()
        # Versions 2 and 3 are very similar; version 3 adds two fields for
        # architecture after both version fields. Fields are space-delimited in
        # both versions.
        if version == 2:
            # In this case, fields are:
            # package name, old version, change direction, new version, action.
            field_count = 5
        elif version == 3:
            # In this case the fields are:
            # package name, old version, old version arch, old version
            # multi-arch type, change direction, new version, new version arch,
            # new version multi-arch type, action.
            field_count = 9
        # Fields are space-delimited, in this order: package name, old version,
        # change direction, new version, action, and if version 3,
        # architecture. I recommend reading the apt.conf(5) man page for more
        # details.
        apt_cache = apt.Cache()
        while line != "":
            # I'm doing a reverse split on space to guard against possible
            # quoting in the package name.
            fields = line.rsplit(" ", maxsplit=(field_count - 1))
            # Pull out the fields we need.
            pkg_name, installed_version, *_, action = fields
            # If the package is being removed or configured, `action` is
            # `**REMOVE**` or `**CONFIGURE**` respectively. Otherwise it's the
            # path to the package file being installed.
            if action in {"**REMOVE**", "**CONFIGURE**"}:
                cached_package = apt_cache[pkg_name]
                packages.append(cached_package)
            else:
                deb_package = DebPackage(filename=action)
                packages.append(deb_package)
                if installed_version != "-":
                    # If we're upgrading from an old package, make sure to look
                    # for those files that might be removed when the old
                    # package is removed.
                    cached_package = apt_cache[pkg_name]
                    packages.append(cached_package)
            line = stream.readline().strip()

    return set(functools.reduce(
        operator.or_,
        (directories_for_package(pkg) for pkg in packages),
        set()
    ))


def main(source):
    # Read the list of packages in
    paths = get_files(source)
    filesystems = filesystems_for_files(paths)

    # Skip filesystems that have com.sun:auto-snapshot set to false
    enabled_filesystems = set()
    for fs in filesystems:
        properties = get_dataset_props(fs)
        if properties.get("com.sun:auto-snapshot", True):
            enabled_filesystems.add(fs)

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
        for f in enabled_filesystems
    ]
    # Create the snapshots
    for snapshot in filesystem_snapshots:
        print(
            "Creating ZFS snapshot '{}'".format(
                snapshot.decode(default_encoding)
            )
        )
        create_snapshot(snapshot)


if __name__ == "__main__":
    # If this environment variable is set, it's the file descriptor we're going
    # to get the info from. If it's not present, use 0 for stdin.
    input_stream = open(int(os.environ.get("APT_HOOK_INFO_FD", 0)), "r")
    main(input_stream)

