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

import argparse
import ctypes
import collections
import datetime
import enum
import functools
import locale
import logging
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
    _lzc_destroy_snaps = getattr(zfs, "lzc_get_props", None)
    if not zfs.is_supported(_lzc_destroy_snaps):
        _lzc_destroy_snaps = None


# Get the current default locale early on
default_encoding = locale.getpreferredencoding()


log = logging.getLogger("zfs_apt_snapshot")
logging.basicConfig(
    level=logging.INFO,
    format="%(filename)s: %(message)s"
)


def log_external(args):
    log.debug(
        "Running external command `%s`",
        b" ".join(args).decode(default_encoding)
    )


class APTSnapshotError(Exception):

    def __init__(self, *args, subprocess_return=None, **kwargs):
        if subprocess_return is not None:
            self.subprocess_return = subprocess_return
            args = b" ".join(self.subprocess_return.args)
            if self.subprocess_return.stderr:
                error_output = self.subprocess_return.stderr
            else:
                error_output = self.subprocess_return.stdout
            message = "Error running command `{}`: {}".format(
                args.decode(default_encoding),
                error_output
            )
            super().__init__(message, *args, **kwargs)
        else:
            super().__init__(*args, **kwargs)


class SnapshotCreationError(APTSnapshotError): pass


class SnapshotExists(SnapshotCreationError): pass


class ZFSListError(APTSnapshotError): pass


class ZFSGetPropertiesError(APTSnapshotError): pass


SNAPSHOT_PREFIX = "zfs-apt-snap"
SNAPSHOT_PREFIX_BYTES = SNAPSHOT_PREFIX.encode(default_encoding)
SNAPSHOT_TIMESTAMP_FORMAT = "%Y-%m-%dT%H%M%S"


def ensure_bytes(func):
    """Helper decorator that ensures the name argument to a function is bytes.
    """
    @functools.wraps(func)
    def inner(*args, **kwargs):
        if "name" in kwargs and isinstance(kwargs["name"], str):
            kwargs["name"] = kwargs["name"].encode(default_encoding)
        args = [
            a.encode(default_encoding) if isinstance(a, str) else a
            for a in args
        ]
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
            args = [b"zfs", b"snapshot", name],
            log_external(args)
            ret = subprocess.run(
                args,
                check=False,
                stderr=subprocess.STDOUT,
                stdout=subprocess.PIPE
            )
            if ret.returncode != 0:
                # TODO do further checking about what kind of error this is
                raise SnapshotCreationError(subprocess_return=ret)


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
        log_external(args)
        ret = subprocess.run(
            args,
            check=False,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE
        )
        if ret.returncode != 0:
            raise ZFSGetPropertiesError(subprocess_return=ret)
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


if _lzc_destroy_snaps is not None:
    @ensure_bytes
    def destroy_snapshots(*names):
        log.info(
            "Destroying snapshots:\n\t%s",
            "\n\t".join(n.decode(default_encoding) for n in names)
        )
        _lzc_destroy_snaps(names)
else:
    @ensure_bytes
    def destroy_snapshots(*names):
        log.info(
            "Destroying snapshots:\n\t%s",
            "\n\t".join(n.decode(default_encoding) for n in names)
        )
        base_args = [b"zfs", b"destroy"]
        for name in names:
            args = base_args + [name]
            log_external(args)
            ret = subprocess.run(
                args,
                check=True,
                stderr=subprocess.STDOUT,
                stdout=subprocess.PIPE
            )


@ensure_bytes
def _zfs_list(*names, type_=None, fields=(b"name",)):
    # kwargs other than name aren't converted by ensure_bytes
    if isinstance(type_, str):
        type_ = type_.encode(default_encoding)
    fields = [
        field.encode(default_encoding) if isinstance(field, str) else field
        for field in fields
    ]

    valid_types = {b"snapshot", b"filesystem", b"volume", b"bookmark", b"all"}
    if type_ not in valid_types:
        raise ValueError("'{}' is not a valid type ZFS type.".format(
            type_.decode(default_encoding)
        ))

    field_spec = b",".join(fields)
    args = [b"zfs", b"list", b"-H", b"-t", type_, b"-o", field_spec, *names]
    log.debug(
        "Running external command `%s`",
        b" ".join(args).decode(default_encoding)
    )
    ret = subprocess.run(
        args,
        check=False,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE
    )
    if ret.returncode != 0:
        raise ZFSListError(subprocess_return=ret)
    else:
        # strip() the output to trim trailing newlines
        rows = ret.stdout.strip().split(b"\n")
        if len(fields) == 1:
            # If we only have one field, just return the list of strings
            return rows
        else:
            # Otherwise, return a list of namedtuples for each row.
            # The namedtuple will have access through the names of each field
            # as well
            ListResult = collections.namedtuple(
                "ListResult",
                # names have to be str, not bytes
                [field.decode(default_encoding) for field in fields]
            )
            return [
                ListResult(*row.split(b"\t"))
                for row in rows
            ]


class MountEntry(ctypes.Structure):
    _fields_ = [
        ("mnt_fsname", ctypes.c_char_p),
        ("mnt_dir", ctypes.c_char_p),
        ("mnt_type", ctypes.c_char_p),
        ("mnt_opts", ctypes.c_char_p),
        ("mnt_freq", ctypes.c_int),
        ("mnt_passno", ctypes.c_int),
    ]


MountEntryPointer = ctypes.POINTER(MountEntry)


Filesystem = collections.namedtuple("Filesystem", ["type_", "name"])


_libc = None


def get_libc():
    global _libc
    if _libc is None:
        # For giggles, we could pass None as the library name (as per
        # dlopen(3), if the filename is NULL, the main program's handle is
        # returned), but that's not especially portable (I think), so we're
        # explicitly requesting libc
        libc_name = ctypes.util.find_library("c")
        if libc_name is None:
            log.error("ERROR: Unable to load libc.")
            sys.exit(1)
        _libc = ctypes.CDLL(libc_name)
        # Set the types for the functions we're going to be using
        _libc.setmntent.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        _libc.setmntent.restype = ctypes.c_void_p
        _libc.getmntent.argtypes = [ctypes.c_void_p]
        _libc.getmntent.restype = MountEntryPointer
        _libc.endmntent.argtypes = [ctypes.c_void_p]
        _libc.endmntent.restype = ctypes.c_int
    return _libc


def list_mounted_filesystems():
    """Return information on all currently mounted filesystems.

    :rtype: Dict[pathlib.Path: Filesystem]
    """
    libc = get_libc()
    mtab_handle = libc.setmntent(b"/etc/mtab", b"r")
    filesystems = {}
    try:
        entry_ptr = libc.getmntent(mtab_handle)
        while entry_ptr:
            entry = entry_ptr.contents
            mountpoint = pathlib.Path(entry.mnt_dir.decode(default_encoding))
            fs = Filesystem(
                entry.mnt_type.decode(default_encoding),
                entry.mnt_fsname.decode(default_encoding)
            )
            filesystems[mountpoint] = fs
            entry_ptr = libc.getmntent(mtab_handle)
        return filesystems
    finally:
        libc.endmntent(mtab_handle)


def list_zfs_volumes():
    """Map ZFS volumes to block devices.

    :returns: A mapping of device paths to the name of the ZFS volume. A single
        volume may have multiple paths to it (ex: `/dev/zd0` and
        `/dev/zvol/pool/volname`). Both paths will have entries in the returned
        mapping with identical values.
    :rtype: Dict[pathlib.Path: bytes]
    """
    # Start from /dev/zvol, as that's where volume devices live according to
    # the man page.
    volumes = {}
    zvol = pathlib.Path("/dev/zvol")
    for root, dirs, files in os.walk(zvol):
        for filename in files:
            path = pathlib.Path(root, filename)
            volume_name = str(path.relative_to(zvol))
            volumes[path] = volume_name
            if path.is_symlink():
                resolved = path.resolve()
                volumes[resolved] = volume_name
    return volumes


def get_filesystems(*paths):
    """Return the names of the ZFS filesystems the given paths exist on."""
    mounted_filesystems = list_mounted_filesystems()
    zfs_volumes = list_zfs_volumes()
    affected_datasets = set()
    for path in paths:
        path = path if isinstance(path, pathlib.Path) else pathlib.Path(path)
        parent_mountpoint = None
        for mountpoint, fs in mounted_filesystems.items():
            try:
                # This is just as a check that `path` is below `mountpoint`
                path.relative_to(mountpoint)
                if parent_mountpoint is None:
                    parent_mountpoint = mountpoint
                elif len(parent_mountpoint.parts) < len(mountpoint.parts):
                    parent_mountpoint = mountpoint
            except ValueError:
                pass
        # parent_mountpoint cannot be None, unless a relative path was given as
        # an argument (in which case all bets are off, good luck).
        assert parent_mountpoint is not None
        fs = mounted_filesystems[parent_mountpoint]
        if fs.type_ == "zfs":
            affected_datasets.add(fs.name)
        else:
            device_path = pathlib.Path(fs.name)
            if device_path in zfs_volumes:
                affected_datasets.add(zfs_volumes[device_path])
            else:
                log.warning(
                    (
                        "Skipping path '{}' as it is not on a "
                        "ZFS filesystem."
                    ).format(path)
                )
    return [
        ds.encode(default_encoding) if isinstance(ds, str) else ds
        for ds in affected_datasets
    ]


@ensure_bytes
def is_apt_snapshot(snapshot_name):
    """Function for checking if a snapshot was created by this tool."""
    if b"@" in snapshot_name:
        _, bare_snapshot_name = snapshot_name.split(b"@")
    else:
        bare_snapshot_name = snapshot_name
    return bare_snapshot_name.startswith(SNAPSHOT_PREFIX_BYTES)


def list_apt_snapshots():
    """List all snapshots that were created by this tool."""
    # No path argument to _zfs_list() means get everything (by default as of
    # 0.8.0 at least).
    all_snapshots = _zfs_list(type_="snapshot")
    apt_snapshots = filter(is_apt_snapshot, all_snapshots)
    return apt_snapshots


def directories_for_package(pkg):
    """Return a list of the directories a package is modifying."""
    directories = set()
    if hasattr(pkg, "filelist"):
        # apt.debfile.DebPackage
        log.info("Getting paths from .deb package '%s'.", pkg.pkgname)
        path_strs = pkg.filelist
    elif hasattr(pkg, "installed_files"):
        # apt.Package
        log.info("Getting paths from cached APT package '%s'.", pkg.name)
        path_strs = pkg.installed_files
    # Figure out the root path to add to relative paths
    log.debug("Paths for %s: %s", pkg, path_strs)
    first_path = path_strs[0]
    if first_path in {"./", "/.", "/"}:
        path_prefix = pathlib.PurePosixPath("/")
    paths = (
        pathlib.PurePosixPath(p)
        for p in path_strs
        # Skip root directories and empty paths
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
    filtered_files = set(files)
    for pure_path in files:
        concrete_path = pathlib.PosixPath(pure_path)
        # If a path doesn't exist yet, or doesn't point to a directory, use the
        # parent directory.
        # Change paths that don't exist yet into the closest parent that does
        while not concrete_path.exists() and not concrete_path.is_dir():
            concrete_path = concrete_path.parent
        if concrete_path != pure_path:
            filtered_files.remove(pure_path)
            filtered_files.add(concrete_path)

    log.info("Filtered %d paths down to %d", len(files), len(filtered_files))
    # convert the path list to a queue so we can put things at the end for
    # later processing
    paths = collections.deque(filtered_files)
    return get_filesystems(*paths)


def get_files(stream):
    """Reads the information stream and returns the directories being changed.

    This supports versions 1, 2, and 3 of the information protocol.
    """
    directories = set()
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
        log.error(
            (
                "ERROR: Unsupported APT helper configuration protocol version "
                "({})!"
            ).format(version)
        )
        sys.exit(1)
    log.debug("Parsing version %d of APT hook protocol.", version)
    if version == 1:
        # handle the version 1 case first, it's simple
        while line != "":
            log.debug("Hook protocol line: '%s'", line)
            pkg = DebPackage(filename=line)
            # Keep the package objects around only as long as they're needed,
            # otherwise you'll open too many files.
            directories.update(directories_for_package(pkg))
            line = stream.readline().strip()
    else:
        line = stream.readline().strip()
        # The first block is the APT configuration space, which is terminated
        # with a blank line. We don't care about the info in there, so we skip
        # it
        log.debug("Skipping APT configuration lines.")
        while line != "":
            log.debug("Hook protocol line: '%s'", line)
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
            log.debug("Hook protocol line: '%s'", line)
            # I'm doing a reverse split on space to guard against possible
            # quoting in the package name.
            fields = line.rsplit(" ", maxsplit=(field_count - 1))
            log.debug("Hook fields: %s", fields)
            # Pull out the fields we need.
            pkg_name, installed_version, *_, action = fields
            # If the package is being removed or configured, `action` is
            # `**REMOVE**` or `**CONFIGURE**` respectively. Otherwise it's the
            # path to the package file being installed.
            if action in {"**REMOVE**", "**CONFIGURE**"}:
                cached_package = apt_cache[pkg_name]
                if cached_package.is_installed:
                    # Only look at packages being reconfigured after they've
                    # been installed. If they aren't installed yet, the package
                    # from the apt cache won't have any files to list (and the
                    # package is probably being installed anyways in this run
                    # anyways).
                    directories.update(directories_for_package(cached_package))
            else:
                deb_package = DebPackage(filename=action)
                directories.update(directories_for_package(deb_package))
                if installed_version != "-":
                    # If we're upgrading from an old package, make sure to look
                    # for those files that might be removed when the old
                    # package is removed.
                    cached_package = apt_cache[pkg_name]
                    directories.update(directories_for_package(cached_package))
            line = stream.readline().strip()

    return directories


def list_old(period):
    old_snaps = []
    limit = datetime.datetime.now() - datetime.timedelta(days=period)
    # Build a list of possible formats timestamps could be in
    timestamp_formats = []

    def drop_tokens(fmt):
        timestamp_format = fmt
        while timestamp_format:
            yield timestamp_format
            timestamp_format = timestamp_format[:timestamp_format.rfind("%")]

    timestamp_format = SNAPSHOT_TIMESTAMP_FORMAT
    # Start by dropping the last token off the end
    timestamp_formats.extend(drop_tokens(SNAPSHOT_TIMESTAMP_FORMAT))
    # If there's a time separator, try without that, and try with a dash
    # instead.
    if "T" in SNAPSHOT_TIMESTAMP_FORMAT:
        timestamp_formats.extend(
            drop_tokens(SNAPSHOT_TIMESTAMP_FORMAT.replace("T", ""))
        )
        timestamp_formats.extend(
            drop_tokens(SNAPSHOT_TIMESTAMP_FORMAT.replace("T", "-"))
        )

    for snapshot in list_apt_snapshots():
        snapshot_name = snapshot.rsplit(b"@", 1)[1]
        # Add one for the underscore separating the prefix from the timestamp
        timestamp_string = snapshot_name[len(SNAPSHOT_PREFIX_BYTES) + 1:]
        log.debug("Checking timestamp '%s'", timestamp_string)
        # The argument to strptime must be a string, not bytes (and it'll
        # almost always be bytes at this point).
        if isinstance(timestamp_string, bytes):
            timestamp_string = timestamp_string.decode(default_encoding)
        snap_timestamp = None
        for timestamp_format in timestamp_formats:
            try:
                snap_timestamp = datetime.datetime.strptime(
                    timestamp_string,
                    timestamp_format
                )
                break
            except ValueError:
                pass
        else:
            log.error(
                "Unable to parse timestamp '%s'",
                timestamp_string
            )
            sys.exit(1)
        if snap_timestamp < limit:
            old_snaps.append(snapshot)
    return old_snaps


def get_config():
    parser = argparse.ArgumentParser(
        description=(
            "Create ZFS snapshots before changing packages through APT."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--ignore-auto-snapshot",
        action="store_false",
        dest="respect_auto_snapshot",
        help=(
            "Ignore the com.sun:auto-snapshot property. Otherwise, those "
            "datasets with this property set to false are not snapshotted by "
            "this script. Datasets without a value for this property are "
            "treated as if it was true."
        )
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging."
    )
    parser.add_argument(
        "--purge-old",
        action="store_true",
        dest="purge",
        help="Purge stale snapshots made by this tool."
    )
    parser.add_argument(
        "--list-old",
        action="store_false",
        dest="list_old",
        help="List stale snapshots made by this tool."
    )
    parser.add_argument(
        "--old-period",
        action="store",
        default=30,
        dest="old_period",
        help=(
            "Consider snapshots (made by this tool) older than this many days "
            "as stale."
        ),
        metavar="DAYS",
        type=int
    )
    args = parser.parse_args()
    return args


def main(source):
    args = get_config()
    if args.verbose:
        log.level = logging.DEBUG
    # Read the list of packages in
    paths = get_files(source)
    filesystems = filesystems_for_files(paths)

    if args.respect_auto_snapshot:
        # Skip filesystems that have com.sun:auto-snapshot set to false
        enabled_filesystems = set()
        for fs in filesystems:
            properties = get_dataset_props(fs)
            if properties.get("com.sun:auto-snapshot", True):
                enabled_filesystems.add(fs)
    else:
        enabled_filesystems = filesystems

    # Choose a name for the snapshot
    timestamp = datetime.datetime.utcnow().strftime(SNAPSHOT_TIMESTAMP_FORMAT)
    snapshot_name = "{}_{}".format(SNAPSHOT_PREFIX, timestamp)
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
        log.info("Creating ZFS snapshot '%s'",
                 snapshot.decode(default_encoding))
        create_snapshot(snapshot)
    # Cleanup (if needed)
    if args.list_old or args.purge:
        old_snaps = list_old(args.old_period)
    if args.list_old and old_snaps:
        # Add a blank entry at the beginning to prefix the listing so the first
        # entry is formatted like the others.
        snap_list = b"\n\t".join([b""] + old_snaps)
        message = "Old {} snapshots:{}".format(
            pathlib.Path(sys.argv[0]).name,
            snap_list.decode(default_encoding)
        )
        log.info(message)
    if args.purge and old_snaps:
        destroy_snapshots(*old_snaps)


if __name__ == "__main__":
    # If this environment variable is set, it's the file descriptor we're going
    # to get the info from. If it's not present, use 0 for stdin.
    input_stream = open(int(os.environ.get("APT_HOOK_INFO_FD", 0)), "r")
    main(input_stream)

