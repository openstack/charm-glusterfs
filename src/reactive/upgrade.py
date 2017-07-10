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
import json
import os
import random
import time
import uuid
from typing import Optional

import apt
import apt_pkg
from charm.gluster import peer, volume
from charm.gluster.apt import get_candidate_package_version
from charmhelpers.cli import hookenv
from charmhelpers.core.hookenv import config, log, status_set, ERROR
from charmhelpers.core.host import service_start, service_stop
from charmhelpers.fetch import apt_install, add_source, apt_update
from gluster.cli.parsers import GlusterCmdOutputParseError
from result import Err, Ok, Result


def get_glusterfs_version() -> str:
    """
    Get the current glusterfs version that is installed
    :return: Result.  Ok(str) or Err(str)
    """
    try:
        cache = apt.Cache()
        version_str = cache['glusterfs-server'].installed.version
        return version_str
    except KeyError:
        raise


def get_local_uuid() -> Result:
    """
    File looks like this:
    UUID=30602134-698f-4e53-8503-163e175aea85
    operating-version=30800

    :return: Result with Ok or Err.
    """
    with open("/var/lib/glusterd/glusterd.info", "r") as f:
        lines = f.readlines()
        for line in lines:
            if "UUID" in line:
                parts = line.split("=")
                gluster_uuid = uuid.UUID(parts[1].strip())
                return Ok(gluster_uuid)
    return Err("Unable to find UUID")


def roll_cluster(new_version: str) -> Result:
    """
    Edge cases:
    1. Previous node dies on upgrade, can we retry
    This is tricky to get right so here's what we're going to do.
    :param new_version: str of the version to upgrade to
    There's 2 possible cases: Either I'm first in line or not.
    If I'm not first in line I'll wait a random time between 5-30 seconds
    and test to see if the previous peer is upgraded yet.

    :param new_version: str.  new version to upgrade to
    :return: Result with Ok or Err.
    """
    log("roll_cluster called with {}".format(new_version))
    volume_name = config("volume_name")
    my_uuid = get_local_uuid()
    if my_uuid.is_err():
        return Err(my_uuid.value)

    # volume_name always has a default
    try:
        volume_bricks = volume.volume_info(volume_name)
        peer_list = volume_bricks.value.bricks.peers

        log("peer_list: {}".format(peer_list))

        # Sort by UUID
        peer_list.sort()
        # We find our position by UUID
        position = [i for i, x in enumerate(peer_list) if x == my_uuid.value]
        if len(position) == 0:
            return Err("Unable to determine upgrade position")
        log("upgrade position: {}".format(position))

        if position[0] == 0:
            # I'm first!  Roll
            # First set a key to inform others I'm about to roll
            lock_and_roll(my_uuid.value, new_version)
        else:
            # Check if the previous node has finished
            status_set(workload_state="waiting",
                       message="Waiting on {} to finish upgrading".format(
                           peer_list[position[0] - 1]))
            wait_on_previous_node(peer_list[position[0] - 1], new_version)
            lock_and_roll(my_uuid.value, new_version)
    except GlusterCmdOutputParseError as e:
        return Err(e)
    return Ok(())


def upgrade_peer(new_version: str) -> Result:
    """
    Upgrade a specific peer
    :param new_version: str.  new version to upgrade to
    :return: Result with Ok or Err.
    """
    from .main import update_status

    current_version = get_glusterfs_version()
    status_set(workload_state="maintenance", message="Upgrading peer")
    log("Current ceph version is {}".format(current_version))
    log("Upgrading to: {}".format(new_version))

    service_stop("glusterfs-server")
    apt_install(["glusterfs-server", "glusterfs-common", "glusterfs-client"])
    service_start("glusterfs-server")
    update_status()
    return Ok(())


def lock_and_roll(my_uuid: uuid.UUID, version: str) -> Result:
    """
    Lock and prevent others from upgrading and upgrade this particular peer
    :param my_uuid: uuid.UUID of the peer to upgrade
    :param version: str.  Version to upgrade to
    :return: Result with Ok or Err
    """
    start_timestamp = time.time()

    log("gluster_key_set {}_{}_start {}".format(my_uuid, version,
                                                start_timestamp))
    gluster_key_set("{}_{}_start".format(my_uuid, version), start_timestamp)
    log("Rolling")

    # This should be quick
    upgrade_peer(version)
    log("Done")

    stop_timestamp = time.time()
    # Set a key to inform others I am finished
    log("gluster_key_set {}_{}_done {}".format(my_uuid, version,
                                               stop_timestamp))
    gluster_key_set("{}_{}_done".format(my_uuid, version), stop_timestamp)

    return Ok(())


def gluster_key_get(key: str) -> Optional[float]:
    """
    Get an upgrade key from the gluster local mount
    :param key: str.  Name of key to get
    :return: Optional[float] with a timestamp
    """
    upgrade_key = os.path.join(os.sep, "mnt", "glusterfs", ".upgrade", key)
    if not os.path.exists(upgrade_key):
        return None

    try:
        with open(upgrade_key, "r") as f:
            s = f.readlines()
            log("gluster_key_get read {} bytes".format(len(s)))
            try:
                decoded = json.loads(s)
                return float(decoded)
            except ValueError:
                log("Failed to decode json file in "
                    "gluster_key_get(): {}".format(s))
                return None
    except IOError as e:
        log("gluster_key_get failed to read file /mnt/glusterfs/.upgraded/.{} "
            "Error: {}".format(key, e.strerror))
        return None


def gluster_key_set(key: str, timestamp: float) -> Result:
    """
    Set a key and a timestamp on the local glusterfs mount
    :param key: str. Name of the key
    :param timestamp: float.  Timestamp
    :return: Result with Ok or Err
    """
    p = os.path.join(os.sep, "mnt", "glusterfs", ".upgrade")
    if os.path.exists(p):
        os.makedirs(p)

    try:
        with open(os.path.join(p, key), "w") as file:
            encoded = json.dumps(timestamp)
            file.write(encoded)
            return Ok(())
    except IOError as e:
        return Err(e.strerror)


def gluster_key_exists(key: str) -> bool:
    location = "/mnt/glusterfs/.upgrade/{}".format(key)
    return os.path.exists(location)


def wait_on_previous_node(previous_node: peer.Peer, version: str) -> Result:
    """
    Wait on a previous node to finish upgrading
    :param previous_node: peer.Peer to wait on
    :param version: str.  Version we're upgrading to
    :return: Result with Ok or Err
    """
    log("Previous node is: {}".format(previous_node))
    previous_node_finished = gluster_key_exists(
        "{}_{}_done".format(previous_node.uuid, version))

    while not previous_node_finished:
        log("{} is not finished. Waiting".format(previous_node.uuid))
        # Has this node been trying to upgrade for longer than
        # 10 minutes
        # If so then move on and consider that node dead.

        # NOTE: This assumes the clusters clocks are somewhat accurate
        # If the hosts clock is really far off it may cause it to skip
        # the previous node even though it shouldn't.
        current_timestamp = time.time()

        previous_node_start_time = gluster_key_get("{}_{}_start".format(
            previous_node.uuid, version))
        if previous_node_start_time is not None:
            if float(current_timestamp - 600) > previous_node_start_time:
                # Previous node is probably dead.  Lets move on
                if previous_node_start_time is not None:
                    log("Waited 10 mins on node {}. "
                        "current time: {} > "
                        "previous node start time: {} "
                        "Moving on".format(previous_node.uuid,
                                           (current_timestamp - 600),
                                           previous_node_start_time))
                    return Ok(())
            else:
                # I have to wait.  Sleep a random amount of time and then
                # check if I can lock,upgrade and roll.
                wait_time = random.randrange(5, 30)
                log("waiting for {} seconds".format(wait_time))
                time.sleep(wait_time)
                previous_node_finished = gluster_key_exists(
                    "{}_{}_done".format(previous_node.uuid, version))
        else:
            # TODO: There is no previous start time.  What should we do?
            return Ok(())


def check_for_upgrade() -> Result:
    """
    If the config has changed this will initiated a rolling upgrade

    :return:
    """
    config = hookenv.config()
    if not config.changed("source"):
        # No upgrade requested
        log("No upgrade requested")
        return Ok(())

    log("Getting current_version")
    current_version = get_glusterfs_version()

    log("Adding new source line")
    source = config["source"]
    if not source:
        # No upgrade requested
        log("Source not set.  Cannot continue with upgrade")
        return Ok(())
    add_source(source)
    log("Calling apt update")
    apt_update()

    log("Getting proposed_version")
    apt_pkg.init_system()
    proposed_version = get_candidate_package_version("glusterfs-server")
    if proposed_version.is_err():
        return Err(proposed_version.value)
    version_compare = apt_pkg.version_compare(a=proposed_version.value,
                                              b=current_version)

    # Using semantic versioning if the new version is greater
    # than we allow the upgrade
    if version_compare > 0:
        log("current_version: {}".format(current_version))
        log("new_version: {}".format(proposed_version.value))
        log("{} to {} is a valid upgrade path.  Proceeding.".format(
            current_version, proposed_version.value))
        return roll_cluster(proposed_version.value)
    else:
        # Log a helpful error message
        log("Invalid upgrade path from {} to {}. The new version needs to be \
                            greater than the old version".format(
            current_version, proposed_version.value), ERROR)
        return Ok(())
