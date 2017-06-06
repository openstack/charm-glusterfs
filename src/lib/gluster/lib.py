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

import copy
import os
import subprocess
import time
from enum import Enum
from typing import List, Optional, Dict

from charmhelpers.contrib.storage.linux.ceph import filesystem_mounted
from charmhelpers.core.hookenv import (ERROR, log, INFO, config,
                                       status_set)
from charmhelpers.core.host import umount, add_to_updatedb_prunepath
from charmhelpers.core.unitdata import kv
from result import Err, Ok, Result

from .block import (FilesystemType, Scheduler, get_device_info,
                    BrickDevice, Zfs, mount_device, weekly_defrag,
                    set_elevator, get_juju_bricks, MetadataProfile,
                    Xfs, Btrfs, Ext4, get_manual_bricks)
from .fstab import FsEntry, FsTab
from .peer import Peer, peer_status, State
from .volume import Brick, Volume


class FailoverDomain(Enum):
    """

    """
    Host = 'host'
    Rack = 'rack'
    Row = 'row'
    DataCenter = 'datacenter'
    Room = 'room'


class Status(Enum):
    """
    Need more expressive return values so we can wait on peers
    """
    Created = 0
    WaitForMorePeers = 1
    InvalidConfig = 2
    FailedToCreate = 3
    FailedToStart = 4
    Expanded = 5


def brick_and_server_product(peers: Dict[str, Dict],
                             failover: FailoverDomain = FailoverDomain.Host) \
        -> List[Brick]:
    """
    {
      'glusterfs-0': {
          'address': '192.168.10.1',
          'bricks': ['/mnt/vdb1', '/mnt/vdb2'],
          'location': ['host', 'rack-a', 'row-a', 'datacenter-1']
      },
      'glusterfs-1': {
          'address': '192.168.10.2',
          'bricks': ['/mnt/vdb1', '/mnt/vdb2', '/mnt/vdb3'],
          'location': ['host', 'rack-a', 'row-b', 'datacenter-1']
      },
    }
    Produce a list of Brick's that can be sent to a gluster cli volume
    creation command.  Tries to take into account failover domain.  Defaults
    to host level failover if none is found.
    :param peers: A list of peers to match up against brick paths
    :param paths: A list of brick mount paths to match up against peers
    :param failover: FailoverDomaon to use
    :return: List[Brick].  Returns a list of Brick's that can be sent in
    order to the gluster cli and create a volume with the correct failover
    domain and replicas.
    """
    _peers = copy.deepcopy(peers)
    product = []
    while all(len(_peers[i]['bricks']) > 0 for i in _peers.keys()):
        for k in _peers.keys():
            host = _peers[k]
            log("host: {}".format(host))
            bricks = host['bricks']
            log("bricks: {}".format(bricks))
            brick = Brick(
                peer=Peer(uuid=None,
                          hostname=host['address'],
                          status=None),
                path=bricks[0],
                is_arbiter=False,
                brick_uuid=None)
            del bricks[0]
            product.append(brick)
    return product


def check_for_new_devices() -> Result:
    """
    Scan for new hard drives to format and turn into a GlusterFS brick
    :return:
    """
    log("Checking for new devices", INFO)
    log("Checking for ephemeral unmount")
    ephemeral_unmount()
    brick_devices = []
    # Get user configured storage devices
    manual_brick_devices = get_manual_bricks()
    if manual_brick_devices.is_err():
        return Err(manual_brick_devices.value)
    brick_devices.extend(manual_brick_devices.value)

    # Get the juju storage block devices
    juju_config_brick_devices = get_juju_bricks()
    if juju_config_brick_devices.is_err():
        return Err(juju_config_brick_devices.value)
    brick_devices.extend(juju_config_brick_devices.value)

    log("storage devices: {}".format(brick_devices))

    format_handles = []
    brick_paths = []
    # Format all drives in parallel
    for device in brick_devices:
        if not device.initialized:
            log("Calling initialize_storage for {}".format(device.dev_path))
            # Spawn all format commands in the background
            handle = initialize_storage(device=device)
            if handle.is_err():
                log("initialize storage for {} failed with err: {}".format(
                    device, handle.value))
                return Err(Status.FailedToCreate)
            format_handles.append(handle.value)
        else:
            # The device is already initialized, lets add it to our
            # usable paths list
            log("{} is already initialized".format(device.dev_path))
            brick_paths.append(device.mount_path)
    # Wait for all children to finish formatting their drives
    for handle in format_handles:
        log("format_handle: {}".format(handle))
        output_result = handle.format_child.wait()
        if output_result is 0:
            # success
            # 1. Run any post setup commands if needed
            finish_initialization(handle.device.dev_path)
            brick_paths.append(handle.device.mount_path)
        else:
            # Failed
            log("Device {} formatting failed with error: {}. Skipping".format(
                handle.device.dev_path, output_result), ERROR)
    log("Usable brick paths: {}".format(brick_paths))
    return Ok(brick_paths)


def ephemeral_unmount() -> Result:
    """
    Unmount amazon ephemeral mount points.
    :return: Result with Ok or Err depending on the outcome of unmount.
    """
    mountpoint = config("ephemeral_unmount")
    if mountpoint is None:
        return Ok(())
    # Remove the entry from the fstab if it's set
    fstab = FsTab(os.path.join(os.sep, "etc", "fstab"))
    log("Removing ephemeral mount from fstab")
    fstab.remove_entry_by_mountpoint(mountpoint)

    if filesystem_mounted(mountpoint):
        result = umount(mountpoint=mountpoint)
        if not result:
            return Err("unmount of {} failed".format(mountpoint))
        # Unmounted Ok
        log("{} unmounted".format(mountpoint))
        return Ok(())
    # Not mounted
    return Ok(())


def find_new_peers(peers: Dict[str, Dict], volume_info: Volume) -> \
        Dict[str, Dict]:
    """
    Checks two lists of peers to see if any new ones not already serving
    a brick have joined.
    :param peers: List[Peer].  List of peers to check.
    :param volume_info: Volume. Existing volume info
    :return: List[Peer] with any peers not serving a brick that can now
    be used.
    """
    new_peers = {}
    for peer in peers:
        # If this peer is already in the volume, skip it
        existing_peer = any(
            brick.peer.hostname == peers[peer]['address'] for brick in
            volume_info.bricks)
        if not existing_peer:
            # Try to match up by hostname
            new_peers[peer] = peers[peer]
    return new_peers


def finish_initialization(device_path: str) -> Result:
    """
    Once devices have been formatted this is called to run fstab entry setup,
    updatedb exclusion, weekly defrags, etc.
    :param device_path:  os.path to device
    :return: Result with Ok or Err
    """
    filesystem_type = FilesystemType(config("filesystem_type"))
    defrag_interval = config("defragmentation_interval")
    disk_elevator = config("disk_elevator")
    scheduler = Scheduler(disk_elevator)
    mount_path = os.path.join(os.sep, 'mnt', os.path.basename(device_path))
    unit_storage = kv()
    device_info = get_device_info(device_path)
    if device_info.is_err():
        return Err(device_info.value)
    log("device_info: {}".format(device_info.value), INFO)

    # Zfs automatically handles mounting the device
    if filesystem_type is not Zfs:
        log("Mounting block device {} at {}".format(device_path, mount_path),
            INFO)
        status_set(workload_state="maintenance",
                   message="Mounting block device {} at {}".format(
                       device_path, mount_path))

        if not os.path.exists(mount_path):
            log("Creating mount directory: {}".format(mount_path), INFO)
            os.makedirs(mount_path)

        mount_result = mount_device(device_info.value, mount_path)
        if mount_result.is_err():
            log("mount failed {}".format(mount_result.value), ERROR)
        status_set(workload_state="maintenance", message="")
        fstab_entry = FsEntry(
            fs_spec="UUID={}".format(device_info.value.id),
            mountpoint=mount_path,
            vfs_type=device_info.value.fs_type,
            mount_options=["noatime", "inode64"],
            dump=False,
            fsck_order=2)
        log("Adding {} to fstab".format(fstab_entry))
        fstab = FsTab(os.path.join("/etc/fstab"))
        fstab.add_entry(fstab_entry)
    unit_storage.set(device_path, True)
    # Actually save the data.  unit_storage.set does not save the value
    unit_storage.flush()
    log("Removing mount path from updatedb {}".format(mount_path), INFO)
    add_to_updatedb_prunepath(mount_path)
    weekly_defrag(mount_path, filesystem_type, defrag_interval)
    set_elevator(device_path, scheduler)
    return Ok(())


def get_brick_list(peers: Dict[str, Dict], volume: Optional[Volume]) -> Result:
    """
    This function will take into account the replication level and
    try its hardest to produce a list of bricks that satisfy this:
    1. Are not already in the volume
    2. Sufficient hosts to satisfy replication level
    3. Stripped across the hosts
    If insufficient hosts exist to satisfy this replication level this will
    return no new bricks to add
    Default to 3 replicas if the parsing fails

    :param peers:
    :param volume:
    :return:
    """
    # brick_devices = []
    replica_config = config("replication_level")
    replicas = 3
    try:
        replicas = int(replica_config)
    except ValueError:
        # Use default
        pass

    if volume is None:
        log("Volume is none")
        # number of bricks % replicas == 0 then we're ok to proceed
        if len(peers) < replicas:
            # Not enough peers to replicate across
            log("Not enough peers to satisfy the replication level for the Gluster \
                        volume.  Waiting for more peers to join.")
            return Err(Status.WaitForMorePeers)
        elif len(peers) == replicas:
            # Case 1: A perfect marriage of peers and number of replicas
            log("Number of peers and number of replicas match")
            log("{}".format(peers))
            return Ok(brick_and_server_product(peers))
        else:
            # Case 2: We have a mismatch of replicas and hosts
            # Take as many as we can and leave the rest for a later time
            count = len(peers) - (len(peers) % replicas)
            new_peers = copy.deepcopy(peers)

            # Drop these peers off the end of the list
            to_remove = list(new_peers.keys())[count:]
            for key in to_remove:
                del new_peers[key]
            log("Too many new peers.  Dropping {} peers off the list".format(
                count))
            return Ok(brick_and_server_product(new_peers))

    else:
        # Existing volume.  Build a differential list.
        log("Existing volume.  Building differential brick list {} {}".format(
            peers, volume))
        new_peers = find_new_peers(peers, volume)

        if len(new_peers) < replicas:
            log("New peers found are less than needed by the replica count")
            return Err(Status.WaitForMorePeers)
        elif len(new_peers) == replicas:
            log("New peers and number of replicas match")
            return Ok(brick_and_server_product(new_peers))
        else:
            count = len(new_peers) - (len(new_peers) % replicas)
            # Drop these peers off the end of the list
            log("Too many new peers.  Dropping {} peers off the list".format(
                count))
            new_peers = copy.deepcopy(peers)

            # Drop these peers off the end of the list
            to_remove = list(new_peers.keys())[count:]
            for key in to_remove:
                del new_peers[key]
            return Ok(brick_and_server_product(new_peers))


def initialize_storage(device: BrickDevice) -> Result:
    """
    Format and mount block devices to ready them for consumption by Gluster
    Return an Initialization struct

    :param device: BrickDevice. The device to format.
    :return: Result with Ok or Err.
    """
    filesystem_type = FilesystemType(config("filesystem_type"))
    log("filesystem_type selected: {}".format(filesystem_type))
    # Custom params
    stripe_width = config("raid_stripe_width")
    stripe_size = config("raid_stripe_size")
    inode_size = config("inode_size")

    # Format with the default XFS unless told otherwise
    if filesystem_type is Xfs:
        log("Formatting block device with XFS: {}".format(device.dev_path),
            INFO)
        status_set(workload_state="maintenance",
                   message="Formatting block device with XFS: {}".format(
                       device.dev_path))
        xfs = Xfs(
            block_size=None,
            force=True,
            inode_size=inode_size,
            stripe_size=stripe_size,
            stripe_width=stripe_width,
        )
        return Ok(xfs.format(brick_device=device))
    elif filesystem_type is Ext4:
        log("Formatting block device with Ext4: {}".format(device.dev_path),
            INFO)
        status_set(workload_state="maintenance",
                   message="Formatting block device with Ext4: {}".format(
                       device.dev_path))

        ext4 = Ext4(
            inode_size=inode_size,
            reserved_blocks_percentage=0,
            stride=stripe_size,
            stripe_width=stripe_width,
        )
        return Ok(ext4.format(brick_device=device))

    elif filesystem_type is Btrfs:
        log("Formatting block device with Btrfs: {}".format(device.dev_path),
            INFO)
        status_set(workload_state="maintenance",
                   message="Formatting block device with Btrfs: {}".format(
                       device.dev_path))

        btrfs = Btrfs(
            leaf_size=0,
            node_size=0,
            metadata_profile=MetadataProfile.Single)
        return Ok(btrfs.format(brick_device=device))
    elif filesystem_type is Zfs:
        log("Formatting block device with ZFS: {:}".format(device.dev_path),
            INFO)
        status_set(workload_state="maintenance",
                   message="Formatting block device with ZFS: {:}".format(
                       device.dev_path))
        zfs = Zfs(
            compression=None,
            block_size=None,
        )
        return Ok(zfs.format(brick_device=device))
    else:
        log("Formatting block device with XFS: {}".format(device.dev_path),
            INFO)
        status_set(workload_state="maintenance",
                   message="Formatting block device with XFS: {}".format(
                       device.dev_path))

        xfs = Xfs(
            block_size=None,
            force=True,
            inode_size=inode_size,
            stripe_width=stripe_width,
            stripe_size=stripe_size)
        return Ok(xfs.format(brick_device=device))


def run_command(command: str, arg_list: List[str], script_mode: bool) -> \
        str:
    """
    :param command:  str. The command to run.
    :param arg_list: List[str].  The argument list
    :param script_mode: .  Should the command be run in script mode.
    :return: str. This returns stdout
    :raises: subprocess.CalledProcessError in the event of a failure
    """
    cmd = [command]
    if script_mode:
        cmd.append("--mode=script")
    for arg in arg_list:
        cmd.append(arg)
    try:
        return subprocess.check_output(cmd, stderr=subprocess.PIPE).decode(
            'utf-8')
    except subprocess.CalledProcessError as e:
        log("subprocess failed stdout: {} stderr: {} returncode: {}".format(
            e.stdout, e.stderr, e.returncode), ERROR)
        raise


def translate_to_bytes(value: str) -> float:
    """
    This is a helper function to convert values such as 1PB into a bytes.

    :param value: str. Size representation to be parsed
    :return: float. Value in bytes
    """
    k = 1024

    sizes = [
        "KB",
        "MB",
        "GB",
        "TB",
        "PB"
    ]

    if value.endswith("Bytes"):
        return float(value.rstrip("Bytes"))
    else:
        for power, size in enumerate(sizes, 1):
            if value.endswith(size):
                return float(value.rstrip(size)) * (k ** power)
        raise ValueError("Cannot translate value")


def peers_are_ready(peer_list: List[Peer]) -> bool:
    """
    Checks to see if all peers are ready.  Peers go through a number of states
    before they are ready to be added to a volume.
    :param peer_list: Result with a List[Peer]
    :return: True or False if all peers are ready
    """
    log("Checking if peers are ready")
    return all(peer.status == State.Connected for peer in peer_list)


def wait_for_peers() -> Result:
    """
    HDD's are so slow that sometimes the peers take long to join the cluster.
    This will loop and wait for them ie spinlock

    :return: Result with Err if waited too long for the peers to become ready.
    """
    log("Waiting for all peers to enter the Peer in Cluster status")
    status_set(workload_state="maintenance",
               message="Waiting for all peers to enter the "
                       "\"Peer in Cluster status\"")
    iterations = 0
    while not peers_are_ready(peer_status()):
        time.sleep(1)
        iterations += 1
        if iterations > 600:
            return Err("Gluster peers failed to connect after 10 minutes")
    return Ok(())
