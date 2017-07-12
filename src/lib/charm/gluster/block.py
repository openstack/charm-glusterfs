# Copyright 2017 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import re
import subprocess
import tempfile
import typing
import uuid
from enum import Enum
from typing import List, Optional, Tuple

import pyudev
from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import log, storage_get, storage_list, ERROR
from charmhelpers.core.unitdata import kv
from charmhelpers.fetch import apt_install
from pyudev import Context
from result import Err, Ok, Result

from .shellscript import parse

config = hookenv.config


class FilesystemType(Enum):
    Btrfs = "btrfs"
    Ext2 = "ext2"
    Ext3 = "ext3"
    Ext4 = "ext4"
    Xfs = "xfs"
    Zfs = "zfs"
    Unknown = "unknown"

    def __str__(self):
        return "{}".format(self.value)


# Formats a block device at Path p with XFS
class MetadataProfile(Enum):
    Raid0 = "raid0"
    Raid1 = "raid1"
    Raid5 = "raid5"
    Raid6 = "raid6"
    Raid10 = "raid10"
    Single = "single"
    Dup = "dup"

    def __str__(self):
        return "{}".format(self.value)


class MediaType(Enum):
    SolidState = 0
    Rotational = 1
    Loopback = 2
    Unknown = 3


class Device(object):
    def __init__(self, id: Optional[uuid.UUID], name: str,
                 media_type: MediaType,
                 capacity: int, fs_type: FilesystemType) -> None:
        """
        This will be used to make intelligent decisions about setting up
        the device

        :param id:
        :param name:
        :param media_type:
        :param capacity:
        :param fs_type:
        """
        self.id = id
        self.name = name
        self.media_type = media_type
        self.capacity = capacity
        self.fs_type = fs_type

    def __repr__(self):
        return "{}".format(self.__dict__)


class BrickDevice(object):
    def __init__(self, is_block_device: bool, initialized: bool,
                 mount_path: str, dev_path: str) -> None:
        """
        A Gluster brick device.
        :param is_block_device: bool
        :param initialized: bool
        :param mount_path: str to mount path
        :param dev_path: os.path to dev path
        """
        self.is_block_device = is_block_device
        self.initialized = initialized
        self.mount_path = mount_path
        self.dev_path = dev_path

    def __eq__(self, other):
        if not isinstance(other, BrickDevice):
            return False
        typing.cast(other, BrickDevice)
        return (self.is_block_device == other.is_block_device and
                self.initialized == other.initialized and
                self.mount_path == other.mount_path and
                self.dev_path == other.dev_path)

    def __str__(self):
        return "is block device: {} initialized: {} " \
               "mount path : {} dev path: {}".format(self.is_block_device,
                                                     self.initialized,
                                                     self.mount_path,
                                                     self.dev_path)


class AsyncInit(object):
    def __init__(self, format_child: subprocess.Popen,
                 post_setup_commands: List[Tuple[str, List[str]]],
                 device: BrickDevice) -> None:
        """
        The child process needed for this device initialization
        This will be an async spawned Popen handle

        :param format_child: subprocess handle.
        :param post_setup_commands:  After formatting is complete run these
            commands to setup the filesystem ZFS needs this.
            These should prob be run in sync mode
        :param device: # The device we're initializing
        """
        self.format_child = format_child
        self.post_setup_commands = post_setup_commands
        self.device = device


class Scheduler(Enum):
    # Try to balance latency and throughput
    Cfq = "cfq"
    # Latency is most important
    Deadline = "deadline"
    # Throughput is most important
    Noop = "noop"

    def __str__(self):
        return "{}".format(self.value)


class Filesystem(object):
    def __init__(self) -> None:
        pass


class Btrfs(Filesystem):
    def __init__(self, metadata_profile: MetadataProfile, leaf_size: int,
                 node_size: int) -> None:
        """
        Btrfs filesystem.
        :param metadata_profile: MetadatProfile
        :param leaf_size: int
        :param node_size: int
        """
        super(Btrfs, self).__init__()
        self.metadata_profile = metadata_profile
        self.leaf_size = leaf_size
        self.node_size = node_size

    def format(self, brick_device: BrickDevice) -> AsyncInit:
        """
        Format a block device with a given filesystem asynchronously.
        :param brick_device: BrickDevice.
        :return: AsyncInit.  Starts formatting immediately and gives back a
        handle to access it.
        """
        device = brick_device.dev_path
        arg_list = ["mkfs.btrfs", "-m", str(self.metadata_profile),
                    "-l", self.leaf_size, "-n", str(self.node_size),
                    device]
        # Check if mkfs.btrfs is installed
        if not os.path.exists("/sbin/mkfs.btrfs"):
            log("Installing btrfs utils")
            apt_install(["btrfs-tools"])

        return AsyncInit(format_child=subprocess.Popen(arg_list),
                         post_setup_commands=[],
                         device=brick_device)


class Ext4(Filesystem):
    def __init__(self, inode_size: Optional[int],
                 reserved_blocks_percentage: int, stride: Optional[int],
                 stripe_width: Optional[int]) -> None:
        """
        Ext4 filesystem.
        :param inode_size: Optional[int]
        :param reserved_blocks_percentage: int
        :param stride: Optional[int]
        :param stripe_width: Optional[int]
        """
        super(Ext4, self).__init__()
        if inode_size is None:
            self.inode_size = 512
        else:
            self.inode_size = inode_size
        if not reserved_blocks_percentage:
            self.reserved_blocks_percentage = 0
        else:
            self.reserved_blocks_percentage = reserved_blocks_percentage
        self.stride = stride
        self.stripe_width = stripe_width

    def format(self, brick_device: BrickDevice) -> AsyncInit:
        """
        Format a block device with a given filesystem asynchronously.
        :param brick_device: BrickDevice.
        :return: AsyncInit.  Starts formatting immediately and gives back a
        handle to access it.
        """
        device = brick_device.dev_path
        arg_list = ["mkfs.ext4", "-m", str(self.reserved_blocks_percentage)]
        if self.inode_size is not None:
            arg_list.append("-I")
            arg_list.append(str(self.inode_size))

        if self.stride is not None:
            arg_list.append("-E")
            arg_list.append("stride={}".format(self.stride))

        if self.stripe_width is not None:
            arg_list.append("-E")
            arg_list.append("stripe_width={}".format(self.stripe_width))

        arg_list.append(device)

        return AsyncInit(format_child=subprocess.Popen(arg_list),
                         post_setup_commands=[],
                         device=brick_device)


class Xfs(Filesystem):
    # This is optional.  Boost knobs are on by default:
    # http:#xfs.org/index.php/XFS_FAQ#Q:
    # _I_want_to_tune_my_XFS_filesystems_for_.3Csomething.3E
    def __init__(self, block_size: Optional[int], inode_size: Optional[int],
                 stripe_size: Optional[int], stripe_width: Optional[int],
                 force: bool) -> None:
        """
        Xfs filesystem
        :param block_size:  Optional[int]
        :param inode_size:  Optional[int]
        :param stripe_size:  Optional[int]
        :param stripe_width:  Optional[int]
        :param force: bool
        """
        super(Xfs, self).__init__()
        self.block_size = block_size
        if inode_size is None:
            self.inode_size = 512
        else:
            self.inode_size = inode_size
        self.stripe_size = stripe_size
        self.stripe_width = stripe_width
        self.force = force

    def format(self, brick_device: BrickDevice) -> AsyncInit:
        """
        Format a block device with a given filesystem asynchronously.
        :param brick_device: BrickDevice.
        :return: AsyncInit.  Starts formatting immediately and gives back a
        handle to access it.
        """
        device = brick_device.dev_path
        arg_list = ["/sbin/mkfs.xfs"]
        if self.inode_size is not None:
            arg_list.append("-i")
            arg_list.append("size={}".format(self.inode_size))

        if self.force:
            arg_list.append("-f")

        if self.block_size is not None:
            block_size = self.block_size
            if not power_of_2(block_size):
                log("block_size {} is not a power of two. Rounding up to "
                    "nearest power of 2".format(block_size))
                block_size = next_power_of_two(block_size)

            arg_list.append("-b")
            arg_list.append("size={}".format(block_size))

        if self.stripe_size is not None and self.stripe_width is not None:
            arg_list.append("-d")
            arg_list.append("su={}".format(self.stripe_size))
            arg_list.append("sw={}".format(self.stripe_width))
        arg_list.append(device)

        # Check if mkfs.xfs is installed
        if not os.path.exists("/sbin/mkfs.xfs"):
            log("Installing xfs utils")
            apt_install(["xfsprogs"])

        format_handle = subprocess.Popen(arg_list)
        return AsyncInit(format_child=format_handle,
                         post_setup_commands=[],
                         device=brick_device)


class Zfs(Filesystem):
    # / The default blocksize for volumes is 8 Kbytes. Any
    # / power of 2 from 512 bytes to 128 Kbytes is valid.
    def __init__(self, block_size: Optional[int],
                 compression: Optional[bool]) -> None:
        """
        ZFS filesystem
        :param block_size: Optional[int]
        :param compression: Optional[bool]
        """
        super(Zfs, self).__init__()
        self.block_size = block_size
        # / Enable compression on the volume. Default is False
        self.compression = compression

    def format(self, brick_device: BrickDevice) -> AsyncInit:
        """
        Format a block device with a given filesystem asynchronously.
        :param brick_device: BrickDevice.
        :return: AsyncInit.  Starts formatting immediately and gives back a
        handle to access it.
        """
        device = brick_device.dev_path
        # Check if zfs is installed
        if not os.path.exists("/sbin/zfs"):
            log("Installing zfs utils")
            apt_install(["zfsutils-linux"])

        base_name = os.path.basename(device)
        # Mount at /mnt/dev_name
        post_setup_commands = []
        arg_list = ["/sbin/zpool", "create", "-f", "-m",
                    "/mnt/{}".format(base_name),
                    base_name, device]
        zpool_create = subprocess.Popen(arg_list)

        if self.block_size is not None:
            # If zpool creation is successful then we set these
            block_size = self.block_size
            log("block_size {} is not a power of two. Rounding up to nearest "
                "power of 2".format(block_size))
            block_size = next_power_of_two(block_size)
            post_setup_commands.append(("/sbin/zfs",
                                        ["set",
                                         "recordsize={}".format(block_size),
                                         base_name]))
        if self.compression is not None:
            post_setup_commands.append(("/sbin/zfs", ["set", "compression=on",
                                                      base_name]))

        post_setup_commands.append(("/sbin/zfs", ["set", "acltype=posixacl",
                                                  base_name]))
        post_setup_commands.append(
            ("/sbin/zfs", ["set", "atime=off", base_name]))
        return AsyncInit(format_child=zpool_create,
                         post_setup_commands=post_setup_commands,
                         device=brick_device)


# This assumes the device is formatted at this point
def mount_device(device: Device, mount_point: str) -> Result:
    """
    mount a device at a mount point
    :param device: Device.
    :param mount_point: str.  Place to mount to.
    :return: Result with Ok or Err
    """
    arg_list = []
    if device.id:
        arg_list.append("-U")
        arg_list.append(str(device.id))
    else:
        arg_list.append("/dev/{}".format(device.name))

    arg_list.append(mount_point)

    cmd = ["mount"]
    cmd.extend(arg_list)
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.PIPE)
        return Ok(output.decode('utf-8'))
    except subprocess.CalledProcessError as e:
        log("subprocess failed stdout: {} stderr: {} returncode: {}".format(
            e.stdout, e.stderr, e.returncode), ERROR)
        return Err(e.output)


def power_of_2(number: int) -> bool:
    """
    Check whether this number is a power of 2
    :param number: int
    :return: True or False if it is a power of 2
    """
    return ((number - 1) & number == 0) and not number == 0


def next_power_of_two(x: int) -> int:
    """
    Get the next power of 2
    :param x: int
    :return: int.  The next largest power of 2
    """
    return 2 ** (x - 1).bit_length()


def get_size(device: pyudev.Device) -> Optional[int]:
    """
    Get the size of a udev device.
    :param device: pyudev.Device
    :return: Optional[int] if the size is available.
    """
    size = device.attributes.get('size')
    if size is not None:
        return int(size) * 512
    return None


def get_uuid(device: pyudev.Device) -> Optional[uuid.UUID]:
    """
    Get the uuid of a udev device.
    :param device: pyudev.Device to check
    :return: Optional[uuid.UUID] if the UUID is available.
    """
    uuid_str = device.properties.get("ID_FS_UUID")
    if uuid_str is not None:
        return uuid.UUID(uuid_str)
    return None


def get_fs_type(device: pyudev.Device) -> Optional[FilesystemType]:
    """
    Get the filesystem type of a udev device.
    :param device: pyudev.Device to check
    :return: Optional[FilesystemType] if available
    """
    fs_type_str = device.properties.get("ID_FS_TYPE")
    if fs_type_str is not None:
        return FilesystemType(fs_type_str)
    return None


def get_media_type(device: pyudev.Device) -> MediaType:
    """
    Get the media type of a udev device.
    :param device: pyudev.Device to check
    :return: MediaType
    """
    device_sysname = device.sys_name
    loop_regex = re.compile(r"loop\d+")

    if loop_regex.match(device_sysname):
        return MediaType.Loopback

    rotation_rate = device.properties.get("ID_ATA_ROTATION_RATE_RPM")
    if rotation_rate is None:
        return MediaType.Unknown
    elif int(rotation_rate) is 0:
        return MediaType.SolidState
    else:
        return MediaType.Rotational


def is_block_device(device_path: str) -> Result:
    """
    Check if a device is a block device
    :param device_path: str path to the device to check.
    :return: Result with Ok or Err
    """
    context = Context()
    sysname = os.path.basename(device_path)
    for device in context.list_devices(subsystem='block'):
        if device.sys_name == sysname:
            return Ok(True)
    return Err("Unable to find device with name {}".format(device_path))


def get_device_info(device_path: str) -> Result:
    """
    Tries to figure out what type of device this is

    :param device_path: os.path to device.
    :return: Result with Ok or Err
    """
    context = Context()
    sysname = os.path.basename(device_path)

    for device in context.list_devices(subsystem='block'):
        if sysname == device.sys_name:
            # Ok we're a block device
            device_id = get_uuid(device)
            media_type = get_media_type(device)
            capacity = get_size(device)
            if capacity is None:
                capacity = 0
            fs_type = get_fs_type(device)
            return Ok(Device(id=device_id, name=sysname,
                             media_type=media_type, capacity=capacity,
                             fs_type=fs_type))
    return Err("Unable to find device with name {}".format(device_path))


def device_initialized(brick_path: str) -> bool:
    """
    Given a dev device path this will check to see if the device
    has been formatted and mounted.

    :param brick_path: os.path to the device.
    """
    log("Connecting to unitdata storage")
    unit_storage = kv()
    log("Getting unit_info")
    unit_info = unit_storage.get(brick_path)
    log("{} initialized: {}".format(brick_path, unit_info))
    if not unit_info:
        return False
    else:
        return True


def scan_devices(devices: List[str]) -> Result:
    """
    Check a list of devices and convert to a list of BrickDevice
    :param devices: List[str] of devices to check
    :return: Result with Ok or Err
    """
    brick_devices = []
    for brick in devices:
        device_path = os.path.join(brick)
        # Translate to mount location
        brick_filename = os.path.basename(device_path)
        log("Checking if {} is a block device".format(device_path))
        block_device = is_block_device(device_path)
        if block_device.is_err():
            log("Skipping invalid block device: {}".format(device_path))
            continue
        log("Checking if {} is initialized".format(device_path))
        initialized = False
        if device_initialized(device_path):
            initialized = True
        mount_path = os.path.join(os.sep, "mnt", brick_filename)
        # All devices start at initialized is False
        brick_devices.append(BrickDevice(
            is_block_device=block_device.value,
            initialized=initialized,
            dev_path=device_path,
            mount_path=mount_path))
    return Ok(brick_devices)


def set_elevator(device_path: str,
                 elevator: Scheduler) -> Result:
    """
    Set the default elevator for a device
    :param device_path: os.path to device
    :param elevator: Scheduler
    :return: Result with Ok or Err
    """
    log("Setting io scheduler for {} to {}".format(device_path, elevator))
    device_name = os.path.basename(device_path)
    f = open("/etc/rc.local", "r")
    elevator_cmd = "echo {scheduler} > /sys/block/{device}/queue/" \
                   "scheduler".format(scheduler=elevator, device=device_name)

    script = parse(f)
    if script.is_ok():
        for line in script.value.commands:
            if device_name in line:
                line = elevator_cmd
    f = open("/etc/rc.local", "w", encoding="utf-8")
    bytes_written = script.value.write(f)
    if bytes_written.is_ok():
        return Ok(bytes_written.value)
    else:
        return Err(bytes_written.value)


def weekly_defrag(mount: str, fs_type: FilesystemType, interval: str) -> \
        Result:
    """
    Setup a weekly defrag of a mount point. Filesystems tend to fragment over
    time and this helps keep Gluster's mount bricks fast.
    :param mount: str to mount point location of the brick
    :param fs_type: FilesystemType.  Some FS types don't have defrag
    :param interval: str.  How often to defrag in crontab format.
    :return: Result with Ok or Err.
    """
    log("Scheduling weekly defrag for {}".format(mount))
    crontab = os.path.join(os.sep, "etc", "cron.weekly", "defrag-gluster")
    defrag_command = ""
    if fs_type is FilesystemType.Ext4:
        defrag_command = "e4defrag"
    elif fs_type is FilesystemType.Btrfs:
        defrag_command = "btrfs filesystem defragment -r"
    elif fs_type is FilesystemType.Xfs:
        defrag_command = "xfs_fsr"

    job = "{interval} {cmd} {path}".format(
        interval=interval,
        cmd=defrag_command,
        path=mount)

    existing_crontab = []
    if os.path.exists(crontab):
        try:
            with open(crontab, 'r') as f:
                buff = f.readlines()
                existing_crontab = list(filter(None, buff))
        except IOError as e:
            return Err(e.strerror)

    existing_job_position = [i for i, x in enumerate(existing_crontab) if
                             mount in x]
    # If we found an existing job we remove the old and insert the new job
    if existing_job_position:
        existing_crontab.remove(existing_job_position[0])

    existing_crontab.append(job)

    # Write back out and use a temporary file.
    try:
        fd, name = tempfile.mkstemp(dir=os.path.dirname(crontab), text=True)
        out = os.fdopen(fd, 'w')
        written_bytes = out.write("\n".join(existing_crontab))
        written_bytes += out.write("\n")
        out.close()
        os.rename(name, 'root')
        return Ok(written_bytes)
    except IOError as e:
        return Err(e.strerror)


def get_manual_bricks() -> Result:
    """
    Get the list of bricks from the config.yaml
    :return: Result with Ok or Err
    """
    log("Gathering list of manually specified brick devices")
    brick_list = []
    manual_config_brick_devices = config("brick_devices")
    for brick in manual_config_brick_devices.split(" "):
        if brick is not None:
            brick_list.append(brick)
    log("List of manual storage brick devices: {}".format(brick_list))
    bricks = scan_devices(brick_list)
    if bricks.is_err():
        return Err(bricks.value)
    return Ok(bricks.value)


def get_juju_bricks() -> Result:
    """
    Get the list of bricks from juju storage.
    :return: Result with Ok or Err
    """
    log("Gathering list of juju storage brick devices")
    # Get juju storage devices
    brick_list = []
    juju_config_brick_devices = storage_list()
    for brick in juju_config_brick_devices:
        if brick is None:
            continue
        s = storage_get("location", brick)
        if s is not None:
            brick_list.append(s.strip())

    log("List of juju storage brick devices: {}".format(brick_list))
    bricks = scan_devices(brick_list)
    if bricks.is_err():
        return Err(bricks.value)
    return Ok(bricks.value)
