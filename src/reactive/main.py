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
import subprocess
from typing import Optional, Dict

from charm.gluster.lib import (check_for_new_devices, run_command, Status,
                               get_brick_list, wait_for_peers)
# from .ctdb import VirtualIp
# from .nfs_relation_joined import nfs_relation_joined
from charm.gluster.peer import peer_probe, Peer
from charm.gluster.volume import (Transport, volume_create_arbiter,
                                  get_local_bricks, Volume,
                                  GlusterOption, SplitBrainPolicy, Toggle,
                                  volume_create_distributed,
                                  volume_create_striped,
                                  volume_create_replicated,
                                  volume_create_striped_replicated,
                                  volume_add_brick, volume_create_erasure,
                                  VolumeType,
                                  volume_enable_bitrot, volume_list,
                                  volume_set_options,
                                  volume_remove_brick, volume_info)
from charmhelpers.contrib.storage.linux.ceph import filesystem_mounted
from charmhelpers.core import hookenv, sysctl
from charmhelpers.core.hookenv import (application_version_set, relation_id)
from charmhelpers.core.hookenv import (config, ERROR, INFO, is_leader,
                                       log, status_set, DEBUG, unit_public_ip)
from charmhelpers.core.host import add_to_updatedb_prunepath
from charmhelpers.fetch import apt_update, add_source, apt_install
from charms.reactive import when, when_not, set_state, remove_state
from gluster.cli import GlusterCmdException
from gluster.cli.parsers import GlusterCmdOutputParseError
from gluster.cli.volume import start
from result import Err, Ok, Result

# from .brick_detached import brick_detached
# from .fuse_relation_joined import fuse_relation_joined
# from .metrics import collect_metrics
# from .server_removed import server_removed
from .upgrade import check_for_upgrade, get_glusterfs_version

"""
#TODO: Deferred
def get_cluster_networks() -> Result: # -> Result<Vec<ctdb.VirtualIp>, str>:
    # Return all the virtual ip networks that will be used
    cluster_networks = []#: Vec<ctdb.VirtualIp> = Vec.new()
    config_value = config["virtual_ip_addresses"]
    if config_value is None:
        config_value = cluster_networks
    virtual_ips = config_value.split(" ")
    for vip in virtual_ips:
        if len(vip) is 0:
            continue
        network = ctdb.ipnetwork_from_str(vip)
        interface = ctdb.get_interface_for_address(network)
            # .ok_or("Failed to find interface for network {}".format(network))
        cluster_networks.append(VirtualIp(cidr=network,interface=interface))
    return Ok(cluster_networks)
"""


@when_not("installed")
def install():
    add_source(config('source'), config('key'))
    apt_update(fatal=True)
    apt_install(
        packages=["ctdb", "nfs-common", "glusterfs-server", "glusterfs-common",
                  "glusterfs-client"], fatal=True)
    set_state("installed")


# @when_file_changed('config.yaml')
def config_changed() -> None:
    """

    :return:
    """
    r = check_for_new_devices()
    if r.is_err():
        log("Checking for new devices failed with error: {".format(r.value),
            ERROR)
    r = check_for_sysctl()
    if r.is_err():
        log("Setting sysctl's failed with error: {".format(r.value), ERROR)
    # If fails we fail the hook
    check_for_upgrade()
    return


@when('server.bricks.available')
@when_not("volume.created")
def initialize_volume(peer) -> None:
    """
    Possibly create a new volume
    :param peer:
    """
    """
    get_peer_info:
    {
      'glusterfs-0': {
          'address': '192.168.10.1',
          'bricks': ['/mnt/vdb1', '/mnt/vdb2']
      },
      'glusterfs-1': {
          'address': '192.168.10.2',
          'bricks': ['/mnt/vdb1', '/mnt/vdb2', '/mnt/vdb3']
      },
    }
    """
    if is_leader():
        log("I am the leader: {}".format(relation_id()))
        log("peer map: {}".format(peer.get_peer_info()))
        vol_name = config("volume_name")
        try:
            vol_info = volume_info(vol_name)
            if not vol_info:
                log("Creating volume {}".format(vol_name), INFO)
                status_set(workload_state="maintenance",
                           message="Creating volume {}".format(vol_name))
                create_result = create_gluster_volume(vol_name,
                                                      peer.get_peer_info())
                if create_result.is_ok():
                    if create_result.value == Status.Created:
                        set_state("volume.created")
                else:
                    log("Volume creation failed with error: {}".format(
                        create_result.value))
        except GlusterCmdException as e:
            log("Volume info command failed: {}".format(e))
            return
            # setup_ctdb()
            # setup_samba(volume_name)
        return
    else:
        log("Deferring to the leader for volume modification")


def create_gluster_volume(volume_name: str,
                          peers: Dict[str, Dict]) -> Result:
    """
    Create a new gluster volume with a name and a list of peers
    :param volume_name: str.  Name of the volume to create
    :param peers: List[Peer].  List of the peers to use in this volume
    :return:
    """
    create_vol = create_volume(peers, None)
    if create_vol.is_ok():
        if create_vol.value == Status.Created:
            log("Create volume succeeded.", INFO)
            status_set(workload_state="maintenance",
                       message="Create volume succeeded")
            start_gluster_volume(volume_name)
            # Poke the other peers to update their status
            set_state("volume.started")
            return Ok(Status.Created)
        elif create_vol.value == Status.WaitForMorePeers:
            log("Waiting for all peers to enter the Peer in Cluster status")
            status_set(workload_state="maintenance",
                       message="Waiting for all peers to enter "
                               "the \"Peer in Cluster status\"")
            return Ok(Status.WaitForMorePeers)
        else:
            # Status is failed
            # What should I return here
            return Ok(())
    else:
        log("Create volume failed with output: {}".format(create_vol.value),
            ERROR)
        status_set(workload_state="blocked",
                   message="Create volume failed.  Please check "
                           "juju debug-log.")
        return Err(create_vol.value)


def create_volume(peers: Dict[str, Dict],
                  volume_info: Optional[Volume]) -> Result:
    """
        Create a new volume if enough peers are available
        :param peers:
        :param volume_info:
        :return:
    """
    cluster_type_config = config("cluster_type")
    cluster_type = VolumeType(cluster_type_config.lower())
    volume_name = config("volume_name")
    replicas = int(config("replication_level"))
    extra = int(config("extra_level"))
    # Make sure all peers are in the cluster
    # spin lock
    wait_for_peers()

    # Build the brick list
    log("get_brick_list: {}".format(peers))
    brick_list = get_brick_list(peers, volume_info)
    if brick_list.is_err():
        if brick_list.value is Status.WaitForMorePeers:
            log("Waiting for more peers", INFO)
            status_set(workload_state="maintenance",
                       message="Waiting for more peers")
            return Ok(Status.WaitForMorePeers)
        elif brick_list.value is Status.InvalidConfig:
            return Err(brick_list.value)
        else:
            # Some other error
            return Err("Unknown error in create volume: {}".format(
                brick_list.value))

    log("Got brick list: {}".format(brick_list.value))
    log("Creating volume of type {} with brick list {}".format(
        cluster_type, [str(b) for b in brick_list.value]), INFO)

    result = None
    if cluster_type is VolumeType.Distribute:
        result = volume_create_distributed(
            vol=volume_name,
            transport=Transport.Tcp,
            bricks=brick_list.value,
            force=True)
    elif cluster_type is VolumeType.Stripe:
        result = volume_create_striped(
            vol=volume_name,
            stripe_count=replicas,
            transport=Transport.Tcp,
            bricks=brick_list.value,
            force=True)
    elif cluster_type is VolumeType.Replicate:
        result = volume_create_replicated(
            vol=volume_name,
            replica_count=replicas,
            transport=Transport.Tcp,
            bricks=brick_list.value,
            force=True)
    elif cluster_type is VolumeType.Arbiter:
        result = volume_create_arbiter(volume_name,
                                       replica_count=replicas,
                                       arbiter_count=extra,
                                       transport=Transport.Tcp,
                                       bricks=brick_list.value,
                                       force=True)
    elif cluster_type is VolumeType.StripedAndReplicate:
        result = volume_create_striped_replicated(volume_name,
                                                  stripe_count=extra,
                                                  replica_count=replicas,
                                                  transport=Transport.Tcp,
                                                  bricks=brick_list.value,
                                                  force=True)
    elif cluster_type is VolumeType.Disperse:
        result = volume_create_erasure(vol=volume_name,
                                       disperse_count=replicas,
                                       redundancy_count=extra,
                                       transport=Transport.Tcp,
                                       bricks=brick_list.value,
                                       force=True)
    elif cluster_type is VolumeType.DistributedAndStripe:
        result = volume_create_striped(vol=volume_name,
                                       stripe_count=replicas,
                                       transport=Transport.Tcp,
                                       bricks=brick_list.value, force=True)
    elif cluster_type is VolumeType.DistributedAndReplicate:
        result = volume_create_replicated(vol=volume_name,
                                          replica_count=replicas,
                                          transport=Transport.Tcp,
                                          bricks=brick_list.value, force=True)
    elif cluster_type is VolumeType.DistributedAndStripedAndReplicate:
        result = volume_create_striped_replicated(vol=volume_name,
                                                  stripe_count=extra,
                                                  replica_count=replicas,
                                                  transport=Transport.Tcp,
                                                  bricks=brick_list.value,
                                                  force=True)
    elif cluster_type is VolumeType.DistributedAndDisperse:
        result = volume_create_erasure(
            vol=volume_name,
            disperse_count=extra,
            redundancy_count=None,
            transport=Transport.Tcp,
            bricks=brick_list.value,
            force=True)
    # Check our result
    if result.is_err():
        log("Failed to create volume: {}".format(result.value), ERROR)
        return Err(Status.FailedToCreate)
    # Everything is good
    return Ok(Status.Created)


@when('server.bricks.available')
@when('volume.created')
def check_for_expansion(peer) -> None:
    """
    Possibly expand an existing volume
    :param peer:
    """
    if is_leader():
        log("I am the leader: {}".format(relation_id()))
        vol_name = config("volume_name")
        try:
            vol_info = volume_info(vol_name)
            if vol_info:
                log("Expanding volume {}".format(vol_name), INFO)
                status_set(workload_state="maintenance",
                           message="Expanding volume {}".format(vol_name))
                expand_vol = expand_volume(peer.get_peer_info(), vol_info[0])
                if expand_vol.is_ok():
                    if expand_vol.value == Status.Expanded:
                        log("Expand volume succeeded.", INFO)
                        status_set(workload_state="active",
                                   message="Expand volume succeeded.")
                        # Poke the other peers to update their status
                        remove_state("volume.needs.expansion")
                        return
                    else:
                        # Ensure the cluster is mounted
                        # setup_ctdb()
                        # setup_samba(volume_name)
                        return
                log("Expand volume failed with output: {}".format(
                    expand_vol.value), ERROR)
                status_set(workload_state="blocked",
                           message="Expand volume failed.  Please check juju "
                                   "debug-log.")
                return
        except GlusterCmdException as e:
            log("Volume info command failed: {}".format(e))
            return


def expand_volume(peers: Dict[str, Dict],
                  vol_info: Optional[Volume]) -> Result:
    """
    Expands the volume by X servers+bricks
    Adds bricks and then runs a rebalance
    :param peers:
    :param vol_info:
    :return:
    """
    volume_name = config("volume_name")
    # Are there new peers
    log("Checking for new peers to expand the volume named {}".format(
        volume_name))
    # Build the brick list
    brick_list = get_brick_list(peers, vol_info)
    if brick_list.is_ok():
        if brick_list.value:
            log("Expanding volume with brick list: {}".format(
                [str(b) for b in brick_list.value]), INFO)
            try:
                volume_add_brick(volume_name, brick_list.value, True)
                return Ok(Status.Expanded)
            except GlusterCmdException as e:
                return Err("Adding brick to volume failed: {}".format(e))
        return Ok(Status.InvalidConfig)
    else:
        if brick_list.value is Status.WaitForMorePeers:
            log("Waiting for more peers", INFO)
            return Ok(Status.WaitForMorePeers)
        elif brick_list.value is Status.InvalidConfig:
            return Err(brick_list.value)
        else:
            # Some other error
            return Err(
                "Unknown error in expand volume: {}".format(brick_list.value))


"""
# TODO: Deferred
# Add all the peers in the gluster cluster to the ctdb cluster

def setup_ctdb() -> Result:
    if config["virtual_ip_addresses"] is None:
        # virtual_ip_addresses isn't set.  Skip setting ctdb up
        return Ok(())

    log("setting up ctdb")
    peers = peer_list()
    log("Got ctdb peer list: {}".format(peers))
    cluster_addresses: Vec<IpAddr> = []
    for peer in peers:
        address = IpAddr.from_str(peer.hostname).map_err(|e| e)
        cluster_addresses.append(address)
    log("writing /etc/default/ctdb")
    ctdb_conf = File.create("/etc/default/ctdb").map_err(|e| e)
    ctdb.render_ctdb_configuration(ctdb_conf).map_err(|e| e)
    cluster_networks = get_cluster_networks()
    log("writing /etc/ctdb/public_addresses")
    public_addresses =
        File.create("/etc/ctdb/public_addresses").map_err(|e| e)
    ctdb.render_ctdb_public_addresses(public_addresses, cluster_networks)
        .map_err(|e| e)

    log("writing /etc/ctdb/nodes")
    cluster_nodes = File.create("/etc/ctdb/nodes").map_err(|e| e)
    ctdb.render_ctdb_cluster_nodes(cluster_nodes, cluster_addresses)
        .map_err(|e| e)

    # Start the ctdb service
    log("Starting ctdb")
    apt.service_start("ctdb")

    return Ok(())
"""


def shrink_volume(peer: Peer, vol_info: Optional[Volume]):
    """
    Shrink a volume.  This needs to be done in replica set so it's a bit
    tricky to get right.
    :param peer: Peer to remove
    :param vol_info: Optional[Volume]
    """
    volume_name = config("volume_name")
    log("Shrinking volume named  {}".format(volume_name), INFO)
    peers = [peer]

    # Build the brick list
    brick_list = get_brick_list(peers, vol_info)
    if brick_list.is_ok():
        log("Shrinking volume with brick list: {}".format(
            [str(b) for b in brick_list.value]), INFO)
        return volume_remove_brick(volume_name, brick_list.value, True)
    else:
        if brick_list.value == Status.WaitForMorePeers:
            log("Waiting for more peers", INFO)
            return Ok(0)
        elif brick_list.value == Status.InvalidConfig:
            return Err(brick_list.value)
        else:
            # Some other error
            return Err("Unknown error in shrink volume: {}".format(
                brick_list.value))


@when('volume.started')
@when_not("volume.configured")
def set_volume_options() -> None:
    """
    Set any options needed on the volume.
    :return:
    """
    if is_leader():
        status_set(workload_state="maintenance",
                   message="Setting volume options")
        volume_name = config('volume_name')
        settings = [
            # Starting in gluster 3.8 NFS is disabled in favor of ganesha.
            # I'd like to stick with the legacy version a bit longer.
            GlusterOption(option=GlusterOption.NfsDisable, value=Toggle.Off),
            GlusterOption(option=GlusterOption.DiagnosticsLatencyMeasurement,
                          value=Toggle.On),
            GlusterOption(option=GlusterOption.DiagnosticsCountFopHits,
                          value=Toggle.On),
            # Dump FOP stats every 5 seconds.
            # NOTE: On slow main drives this can severely impact them
            GlusterOption(option=GlusterOption.DiagnosticsFopSampleInterval,
                          value=5),
            GlusterOption(option=GlusterOption.DiagnosticsStatsDumpInterval,
                          value=30),
            # 1HR DNS timeout
            GlusterOption(option=GlusterOption.DiagnosticsStatsDnscacheTtlSec,
                          value=3600),
            # Set parallel-readdir on.  This has a very nice performance
            # benefit as the number of bricks/directories grows
            GlusterOption(option=GlusterOption.PerformanceParallelReadDir,
                          value=Toggle.On),
            GlusterOption(option=GlusterOption.PerformanceReadDirAhead,
                          value=Toggle.On),
            # Start with 20MB and go from there
            GlusterOption(
                option=GlusterOption.PerformanceReadDirAheadCacheLimit,
                value=1024 * 1024 * 20)]

        # Set the split brain policy if requested
        splitbrain_policy = config("splitbrain_policy")
        if splitbrain_policy:
            # config.yaml has a default here.  Should always have a value
            policy = SplitBrainPolicy(splitbrain_policy)
            if policy:
                log("Setting split brain policy to: {}".format(
                    splitbrain_policy), DEBUG)
                settings.append(
                    GlusterOption(option=GlusterOption.FavoriteChildPolicy,
                                  value=policy))
        # Set all the volume options
        option_set_result = volume_set_options(volume_name, settings)

        # The has a default.  Should be safe
        bitrot_config = bool(config("bitrot_detection"))
        if bitrot_config:
            log("Enabling bitrot detection", DEBUG)
            status_set(workload_state="maintenance",
                       message="Enabling bitrot detection.")
            try:
                volume_enable_bitrot(volume_name)
            except GlusterCmdException as e:
                log("Enabling bitrot failed with error: {}".format(e), ERROR)
        # Tell reactive we're all set here
        status_set(workload_state="active",
                   message="")
        if option_set_result.is_err():
            log("Setting volume options failed with error(s): {}".format(
                option_set_result.value), ERROR)
        set_state("volume.configured")
    # Display the status of the volume on the juju cli
    update_status()


def start_gluster_volume(volume_name: str) -> None:
    """
    Startup the gluster volume
    :param volume_name: str.  volume name to start
    :return: Result
    """
    try:
        start(volume_name, False)
        log("Starting volume succeeded.", INFO)
        status_set(workload_state="active",
                   message="Starting volume succeeded.")
        return Ok(())
    except GlusterCmdException as e:
        log("Start volume failed with output: {}".format(e), ERROR)
        status_set(workload_state="blocked",
                   message="Start volume failed. Please check juju debug-log.")


def check_for_sysctl() -> Result:
    """
    Check to see if there's sysctl changes that need to be applied
    :return: Result
    """
    config = hookenv.config()
    if config.changed("sysctl"):
        config_path = os.path.join(os.sep, "etc", "sysctl.d",
                                   "50-gluster-charm.conf")
        sysctl_dict = config["sysctl"]
        if sysctl_dict is not None:
            sysctl.create(sysctl_dict, config_path)
    return Ok(())


@when('server.connected')
def server_connected(peer) -> None:
    """
    The peer.available state is set when there are one or more peer units
    that have joined.
    :return:
    """
    update_status()
    bricks = check_for_new_devices()
    if bricks.is_ok():
        log('Reporting my bricks {} to the leader'.format(bricks.value))
        peer.set_bricks(bricks=bricks.value)
    if not is_leader():
        log('Reporting my public address {} to the leader'.format(
            unit_public_ip()))
        peer.set_address(address_type='public', address=unit_public_ip())
        return

    # I am the leader
    log('Leader probing peers')
    probed_units = []
    try:
        p = hookenv.leader_get('probed-units')
        if p:
            probed_units = json.loads(p)
    except json.decoder.JSONDecodeError as e:
        log("json decoder failed for {}: {}".format(e.doc, e.msg))

    log("probed_units: {}".format(probed_units))
    peer_info = peer.get_peer_info()
    for unit in peer_info:
        if unit in probed_units:
            continue
        address = peer_info[unit]['address']
        log('probing host {} at {}'.format(unit, address))
        status_set('maintenance', 'Probing peer {}'.format(unit))
        try:
            peer_probe(address)
            probed_units.append(unit)
        except (GlusterCmdException, GlusterCmdOutputParseError):
            log('Error probing host {}: {}'.format(unit, address), ERROR)
            continue
        log('successfully probed {}: {}'.format(unit, address), DEBUG)
    settings = {'probed-units': json.dumps(probed_units)}
    hookenv.leader_set(settings)
    status_set('maintenance', '')


"""
def resolve_first_vip_to_dns() -> Result:
    cluster_networks = get_cluster_networks()
    if cluster_networks.is_ok():
    match cluster_networks.first() {
        Some(cluster_network) => {
            match cluster_network.cidr {
                IpNetwork.V4(ref v4_network) => {
                    # Resolve the ipv4 address back to a dns string
                    Ok(address_name(.std.net.IpAddr.V4(v4_network.ip())))
                }
                IpNetwork.V6(ref v6_network) => {
                    # Resolve the ipv6 address back to a dns string
                    Ok(address_name(.std.net.IpAddr.V6(v6_network.ip())))
        None => {
            # No vips were set
            return Err("virtual_ip_addresses has no addresses set")
"""


@when('installed')
@when_not('glusterfs.mounted')
def mount_cluster() -> None:
    """
    Mount the cluster at /mnt/glusterfs using fuse

    :return: Result.  Ok or Err depending on the outcome of mount
    """
    log("Checking if cluster mount needed")
    volume_name = config('volume_name')
    volumes = volume_list()
    if not os.path.exists("/mnt/glusterfs"):
        os.makedirs("/mnt/glusterfs")
    if not filesystem_mounted("/mnt/glusterfs"):
        if volume_name in volumes:
            arg_list = ["-t", "glusterfs", "localhost:/{}".format(volume_name),
                        "/mnt/glusterfs"]
            try:
                run_command(command="mount", arg_list=arg_list,
                            script_mode=False)
                log("Removing /mnt/glusterfs from updatedb", INFO)
                add_to_updatedb_prunepath("/mnt/glusterfs")
                set_state("glusterfs.mounted")
                update_status()
                return
            except subprocess.CalledProcessError as e:
                log("mount failed with error: "
                    "stdout: {} stderr: {}".format(e.stdout, e.stderr))
                return


def update_status() -> None:
    """
    Update the juju status information

    :return: Result with Ok or Err.
    """
    try:
        version = get_glusterfs_version()
        application_version_set("{}".format(version))
    except KeyError:
        log("glusterfs-server not installed yet.  Cannot discover version",
            DEBUG)
        return
    volume_name = config("volume_name")

    local_bricks = get_local_bricks(volume_name)
    if local_bricks.is_ok():
        status_set(workload_state="active",
                   message="Unit is ready ({} bricks)".format(
                       len(local_bricks.value)))
        return
    else:
        status_set(workload_state="blocked",
                   message="No bricks found")
        return
