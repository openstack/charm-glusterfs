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
import subprocess
import typing
import uuid
import xml.etree.ElementTree as etree
from enum import Enum
from ipaddress import ip_address
from typing import Dict, List, Optional

from charmhelpers.contrib.openstack.utils import get_host_ip
from charmhelpers.core.hookenv import log, ERROR, unit_get
from gluster.cli import (bitrot, bricks, GlusterCmdException, quota, rebalance,
                         volume)
from gluster.cli.parsers import GlusterCmdOutputParseError
from result import Err, Ok, Result

from .peer import Peer
from ..utils.utils import check_return_code


class AccessMode(Enum):
    ReadOnly = "read-only"
    ReadWrite = "read-write"

    def __str__(self):
        return self.value

    @staticmethod
    def from_str(s: str) -> Optional['AccessMode']:
        """
        Parse a SelfHealAlgorithm from a str
        :param s: str. The string to parse
        :return: AccessMode
        """
        if s == "read-only":
            return AccessMode.ReadOnly
        elif s == "read-write":
            return AccessMode.ReadWrite
        else:
            return None


class SelfHealAlgorithm(Enum):
    Full = "full"
    Diff = "diff"
    Reset = "reset"

    def __str__(self):
        return self.value

    @staticmethod
    def from_str(s: str) -> Optional['SelfHealAlgorithm']:
        """
        Parse a SelfHealAlgorithm from a str
        :param s: str. The string to parse
        :return: SelfHealAlgorithm
        """
        if s == "full":
            return SelfHealAlgorithm.Full
        elif s == "diff":
            return SelfHealAlgorithm.Diff
        elif s == "reset":
            return SelfHealAlgorithm.Reset
        else:
            return None


class SplitBrainPolicy(Enum):
    Ctime = "ctime"
    Disable = "none"
    Majority = "majority"
    Mtime = "mtime"
    Size = "size"

    def __str__(self):
        return self.value

    @staticmethod
    def from_str(s: str) -> Optional['SplitBrainPolicy']:
        """
        Parse a SelfHealAlgorithm from a str
        :param s: str. The string to parse
        :return: SplitBrainPolicy
        """
        if s == "ctime":
            return SplitBrainPolicy.Ctime
        elif s == "none":
            return SplitBrainPolicy.Disable
        elif s == "majority":
            return SplitBrainPolicy.Majority
        elif s == "mtime":
            return SplitBrainPolicy.Mtime
        elif s == "size":
            return SplitBrainPolicy.Size
        else:
            return None


class Toggle(Enum):
    On = True
    Off = False

    def __str__(self):
        if self.value:
            return "On"
        else:
            return "Off"

    @staticmethod
    def from_str(s: str) -> Optional['Toggle']:
        """
        Parse a Toggle from a str
        :param s: str. The string to parse
        :return: Toggle
        """
        s = s.lower()
        if s == "on":
            return Toggle.On
        elif s == "off":
            return Toggle.Off
        elif s == "true":
            return Toggle.On
        elif s == "false":
            return Toggle.Off
        else:
            return None


class GlusterOption(object):
    # Valid IP address which includes wild card patterns including *,
    # such as 192.168.1.*
    AuthAllow = "auth.allow"
    # Valid IP address which includes wild card patterns including *,
    # such as 192.168.2.*
    AuthReject = "auth.reject"
    # Specifies the duration for the lock state to be maintained on the
    # client after a network disconnection in seconds
    # Range: 10-1800
    ClientGraceTimeout = "client.grace-timeout"
    # Specifies the maximum number of blocks per file on which self-heal
    # would happen simultaneously.
    # Range: 0-1025
    ClusterSelfHealWindowSize = "cluster.self-heal-window-size"
    # enable/disable client.ssl flag in the volume
    ClientSsl = "client.ssl"
    # Specifies the type of self-heal. If you set the option as "full", the
    # entire file is copied from source to destinations. If the option is set
    # to "diff" the file blocks
    # that are not in sync are copied to destinations.
    ClusterDataSelfHealAlgorithm = "cluster.data-self-heal-algorithm"
    # Percentage of required minimum free disk space
    DiagnosticsFopSampleBufSize = "diagnostics.fop-sample-buf-size"
    ClusterMinFreeDisk = "cluster.min-free-disk"
    # Specifies the size of the stripe unit that will be read from or written
    # to in bytes
    ClusterStripeBlockSize = "cluster.stripe-block-size"
    # Allows you to turn-off proactive self-heal on replicated
    ClusterSelfHealDaemon = "cluster.self-heal-daemon"
    # This option makes sure the data/metadata is durable across abrupt
    # shutdown of the brick.
    ClusterEnsureDurability = "cluster.ensure-durability"
    # The log-level of the bricks.
    DiagnosticsBrickLogLevel = "diagnostics.brick-log-level"
    # The log-level of the clients.
    DiagnosticsClientLogLevel = "diagnostics.client-log-level"
    # Interval in which we want to collect FOP latency samples.  2 means
    # collect a sample every 2nd FOP.
    DiagnosticsFopSampleInterval = "diagnostics.fop-sample-interval"
    # The maximum size of our FOP sampling ring buffer. Default: 65535
    # Enable the File Operation count translator
    DiagnosticsCountFopHits = "diagnostics.count-fop-hits"
    # Interval (in seconds) at which to auto-dump statistics. Zero disables
    # automatic dumping.
    DiagnosticsStatsDumpInterval = "diagnostics.stats-dump-interval"
    # The interval after wish a cached DNS entry will be re-validated.
    # Default: 24 hrs
    DiagnosticsStatsDnscacheTtlSec = "diagnostics.stats-dnscache-ttl-sec"
    # Statistics related to the latency of each operation would be tracked.
    DiagnosticsLatencyMeasurement = "diagnostics.latency-measurement"
    # Statistics related to file-operations would be tracked.
    DiagnosticsDumpFdStats = "diagnostics.dump-fd-stats"
    # Enables automatic resolution of split brain issues
    FavoriteChildPolicy = "cluster.favorite-child-policy"
    # Enables you to mount the entire volume as read-only for all the clients
    # (including NFS clients) accessing it.
    FeaturesReadOnly = "features.read-only"
    # Enables self-healing of locks when the network disconnects.
    FeaturesLockHeal = "features.lock-heal"
    # For performance reasons, quota caches the directory sizes on client.
    # You can set timeout indicating the maximum duration of directory sizes
    # in cache, from the time they are
    # populated, during which they are considered valid
    FeaturesQuotaTimeout = "features.quota-timeout"
    # Automatically sync the changes in the filesystem from Master to Slave.
    GeoReplicationIndexing = "geo-replication.indexing"
    # The time frame after which the operation has to be declared as dead,
    # if the server does not respond for a particular operation.
    NetworkFrameTimeout = "network.frame-timeout"
    # For 32-bit nfs clients or applications that do not support 64-bit inode
    # numbers or large files, use this option from the CLI to make Gluster NFS
    # return 32-bit inode numbers instead of 64-bit inode numbers.
    NfsEnableIno32 = "nfs.enable-ino32"
    # Set the access type for the specified sub-volume.
    NfsVolumeAccess = "nfs.volume-access"
    # If there is an UNSTABLE write from the client, STABLE flag will be
    # returned to force the client to not send a COMMIT request. In some
    # environments, combined with a replicated GlusterFS setup, this option
    # can improve write performance. This flag allows users to trust Gluster
    # replication logic to sync data to the disks and recover when required.
    # COMMIT requests if received will be handled in a default manner by
    # fsyncing. STABLE writes are still handled in a sync manner.
    NfsTrustedWrite = "nfs.trusted-write"
    # All writes and COMMIT requests are treated as async. This implies that
    # no write requests
    # are guaranteed to be on server disks when the write reply is received
    # at the NFS client.
    # Trusted sync includes trusted-write behavior.
    NfsTrustedSync = "nfs.trust-sync"
    # This option can be used to export specified comma separated
    # subdirectories in the volume.
    # The path must be an absolute path. Along with path allowed list of
    # IPs/hostname can be
    # associated with each subdirectory. If provided connection will allowed
    # only from these IPs.
    # Format: \<dir>[(hostspec[hostspec...])][,...]. Where hostspec can be an
    # IP address,
    # hostname or an IP range in CIDR notation. Note: Care must be taken
    # while configuring
    # this option as invalid entries and/or unreachable DNS servers can
    # introduce unwanted
    # delay in all the mount calls.
    NfsExportDir = "nfs.export-dir"
    # Enable/Disable exporting entire volumes, instead if used in conjunction
    # with
    # nfs3.export-dir, can allow setting up only subdirectories as exports.
    NfsExportVolumes = "nfs.export-volumes"
    # Enable/Disable the AUTH_UNIX authentication type. This option is
    # enabled by default for
    # better interoperability. However, you can disable it if required.
    NfsRpcAuthUnix = "nfs.rpc-auth-unix"
    # Enable/Disable the AUTH_NULL authentication type. It is not recommended
    # to change the default value for this option.
    NfsRpcAuthNull = "nfs.rpc-auth-null"
    # Allow client connections from unprivileged ports. By default only
    # privileged ports are
    # allowed. This is a global setting in case insecure ports are to be
    # enabled for all exports using a single option.
    NfsPortsInsecure = "nfs.ports-insecure"
    # Turn-off name lookup for incoming client connections using this option.
    # In some setups,
    # the name server can take too long to reply to DNS queries resulting in
    # timeouts of mount
    # requests. Use this option to turn off name lookups during address
    # authentication. Note,
    NfsAddrNamelookup = "nfs.addr-namelookup"
    # For systems that need to run multiple NFS servers, you need to prevent
    # more than one from
    # registering with portmap service. Use this option to turn off portmap
    # registration for Gluster NFS.
    NfsRegisterWithPortmap = "nfs.register-with-portmap"
    # Turn-off volume being exported by NFS
    NfsDisable = "nfs.disable"
    # Size of the per-file write-behind buffer.Size of the per-file
    # write-behind buffer.
    PerformanceWriteBehindWindowSize = "performance.write-behind-window-size"
    # The number of threads in IO threads translator.
    PerformanceIoThreadCount = "performance.io-thread-count"
    # If this option is set ON, instructs write-behind translator to perform
    # flush in background, by returning success (or any errors, if any
    # of previous writes were failed)
    # to application even before flush is sent to backend filesystem.
    PerformanceFlushBehind = "performance.flush-behind"
    # Sets the maximum file size cached by the io-cache translator. Can use the
    # normal size
    # descriptors of KB, MB, GB,TB or PB (for example, 6GB). Maximum size u64.
    PerformanceCacheMaxFileSize = "performance.cache-max-file-size"
    # Sets the minimum file size cached by the io-cache translator. Values same
    # as "max" above
    PerformanceCacheMinFileSize = "performance.cache-min-file-size"
    # The cached data for a file will be retained till 'cache-refresh-timeout'
    # seconds, after which data re-validation is performed.
    PerformanceCacheRefreshTimeout = "performance.cache-refresh-timeout"
    # Size of the read cache in bytes
    PerformanceCacheSize = "performance.cache-size"
    # enable/disable readdir-ahead translator in the volume
    PerformanceReadDirAhead = "performance.readdir-ahead"
    # If this option is enabled, the readdir operation is performed parallely
    # on all the bricks,
    # thus improving the performance of readdir. Note that the performance
    # improvement is higher
    # in large clusters
    PerformanceParallelReadDir = "performance.parallel-readdir"
    # maximum size of cache consumed by readdir-ahead xlator. This value is
    # global and total
    # memory consumption by readdir-ahead is capped by this value, irrespective
    # of the
    # number/size of directories cached
    PerformanceReadDirAheadCacheLimit = "performance.rda-cache-limit"
    # Allow client connections from unprivileged ports. By default only
    # privileged ports are
    # allowed. This is a global setting in case insecure ports are to be
    # enabled for all exports using a single option.
    ServerAllowInsecure = "server.allow-insecure"
    # Specifies the duration for the lock state to be maintained on the server
    # after a network disconnection.
    ServerGraceTimeout = "server.grace-timeout"
    # enable/disable server.ssl flag in the volume
    ServerSsl = "server.ssl"
    # Location of the state dump file.
    ServerStatedumpPath = "server.statedump-path"
    SslAllow = "auth.ssl-allow"
    SslCertificateDepth = "ssl.certificate-depth"
    SslCipherList = "ssl.cipher-list"
    # Number of seconds between health-checks done on the filesystem that
    # is used for the
    # brick(s). Defaults to 30 seconds, set to 0 to disable.
    StorageHealthCheckInterval = "storage.health-check-interval"

    def __init__(self, option, value):
        self.option = option
        self.value = value

    @staticmethod
    def from_str(s: str, value):
        if s == "auth.allow":
            return GlusterOption(option=GlusterOption.AuthAllow, value=value)
        elif s == "auth.reject":
            return GlusterOption(option=GlusterOption.AuthReject, value=value)
        elif s == "auth.ssl-allow":
            return GlusterOption(option=GlusterOption.SslAllow, value=value)
        elif s == "client.ssl":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.ClientSsl, value=t)
        elif s == "cluster.favorite-child-policy":
            policy = SplitBrainPolicy.from_str(value)
            return GlusterOption(option=GlusterOption.FavoriteChildPolicy,
                                 value=policy)
        elif s == "client.grace-timeout":
            i = int(value)
            return GlusterOption(option=GlusterOption.ClientGraceTimeout,
                                 value=i)
        elif s == "cluster.self-heal-window-size":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.ClusterSelfHealWindowSize,
                value=i)
        elif s == "cluster.data-self-heal-algorithm":
            s = SelfHealAlgorithm.from_str(value)
            return GlusterOption(
                option=GlusterOption.ClusterDataSelfHealAlgorithm, value=s)
        elif s == "cluster.min-free-disk":
            i = int(value)
            return GlusterOption(option=GlusterOption.ClusterMinFreeDisk,
                                 value=i)
        elif s == "cluster.stripe-block-size":
            i = int(value)
            return GlusterOption(option=GlusterOption.ClusterStripeBlockSize,
                                 value=i)
        elif s == "cluster.self-heal-daemon":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.ClusterSelfHealDaemon,
                                 value=t)
        elif s == "cluster.ensure-durability":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.ClusterEnsureDurability,
                                 value=t)
        elif s == "diagnostics.brick-log-level":
            return GlusterOption(option=GlusterOption.DiagnosticsBrickLogLevel,
                                 value=value)
        elif s == "diagnostics.client-log-level":
            return GlusterOption(
                option=GlusterOption.DiagnosticsClientLogLevel,
                value=value)
        elif s == "diagnostics.latency-measurement":
            t = Toggle.from_str(value)
            return GlusterOption(
                option=GlusterOption.DiagnosticsLatencyMeasurement, value=t)
        elif s == "diagnostics.count-fop-hits":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.DiagnosticsCountFopHits,
                                 value=t)
        elif s == "diagnostics.stats-dump-interval":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.DiagnosticsStatsDumpInterval, value=i)
        elif s == "diagnostics.fop-sample-buf-size":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.DiagnosticsFopSampleBufSize,
                value=i)
        elif s == "diagnostics.fop-sample-interval":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.DiagnosticsFopSampleInterval, value=i)
        elif s == "diagnostics.stats-dnscache-ttl-sec":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.DiagnosticsStatsDnscacheTtlSec, value=i)
        elif s == "diagnostics.dump-fd-stats":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.DiagnosticsDumpFdStats,
                                 value=t)
        elif s == "features.read-only":
            t = Toggle.from_str(value)
            return GlusterOption(
                option=GlusterOption.FeaturesReadOnly, value=t)
        elif s == "features.lock-heal":
            t = Toggle.from_str(value)
            return GlusterOption(
                option=GlusterOption.FeaturesLockHeal, value=t)
        elif s == "features.quota-timeout":
            i = int(value)
            return GlusterOption(option=GlusterOption.FeaturesQuotaTimeout,
                                 value=i)
        elif s == "geo-replication.indexing":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.GeoReplicationIndexing,
                                 value=t)
        elif s == "network.frame-timeout":
            i = int(value)
            return GlusterOption(option=GlusterOption.NetworkFrameTimeout,
                                 value=i)
        elif s == "nfs.enable-ino32":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsEnableIno32, value=t)
        elif s == "nfs.volume-access":
            s = AccessMode.from_str(value)
            return GlusterOption(option=GlusterOption.NfsVolumeAccess, value=s)
        elif s == "nfs.trusted-write":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsTrustedWrite, value=t)
        elif s == "nfs.trusted-sync":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsTrustedSync, value=t)
        elif s == "nfs.export-dir":
            return GlusterOption(
                option=GlusterOption.NfsExportDir, value=value)
        elif s == "nfs.export-volumes":
            t = Toggle.from_str(value)
            return GlusterOption(
                option=GlusterOption.NfsExportVolumes, value=t)
        elif s == "nfs.rpc-auth-unix":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsRpcAuthUnix, value=t)
        elif s == "nfs.rpc-auth-null":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsRpcAuthNull, value=t)
        elif s == "nfs.ports-insecure":
            t = Toggle.from_str(value)
            return GlusterOption(
                option=GlusterOption.NfsPortsInsecure, value=t)
        elif s == "nfs.addr-namelookup":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsAddrNamelookup,
                                 value=t)
        elif s == "nfs.register-with-portmap":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsRegisterWithPortmap,
                                 value=t)
        elif s == "nfs.disable":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsDisable, value=t)
        elif s == "performance.write-behind-window-size":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.PerformanceWriteBehindWindowSize, value=i)
        elif s == "performance.io-thread-count":
            i = int(value)
            return GlusterOption(option=GlusterOption.PerformanceIoThreadCount,
                                 value=i)
        elif s == "performance.flush-behind":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.PerformanceFlushBehind,
                                 value=t)
        elif s == "performance.cache-max-file-size":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.PerformanceCacheMaxFileSize,
                value=i)
        elif s == "performance.cache-min-file-size":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.PerformanceCacheMinFileSize,
                value=i)
        elif s == "performance.cache-refresh-timeout":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.PerformanceCacheRefreshTimeout, value=i)
        elif s == "performance.cache-size":
            i = int(value)
            return GlusterOption(option=GlusterOption.PerformanceCacheSize,
                                 value=i)
        elif s == "performance.readdir-ahead":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.PerformanceReadDirAhead,
                                 value=t)
        elif s == "performance.parallel-readdir":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.PerformanceReadDirAhead,
                                 value=t)
        elif s == "performance.readdir-cache-limit":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.PerformanceReadDirAheadCacheLimit,
                value=i)
        elif s == "server.ssl":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.ServerSsl, value=t)
        elif s == "server.allow-insecure":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.ServerAllowInsecure,
                                 value=t)
        elif s == "server.grace-timeout":
            i = int(value)
            return GlusterOption(option=GlusterOption.ServerGraceTimeout,
                                 value=i)
        elif s == "server.statedump-path":
            return GlusterOption(option=GlusterOption.ServerStatedumpPath,
                                 value=value)
        elif s == "ssl.certificate-depth":
            i = int(value)
            return GlusterOption(option=GlusterOption.SslCertificateDepth,
                                 value=i)
        elif s == "ssl.cipher-list":
            return GlusterOption(GlusterOption.SslCipherList, value=value)
        elif s == "storage.health-check-interval":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.StorageHealthCheckInterval,
                value=i)
        else:
            raise ValueError


class ScrubAggression(Enum):
    Aggressive = "aggressive"
    Lazy = "lazy"
    Normal = "normal"

    def __str__(self):
        return "scrub-throttle"

    @staticmethod
    def from_str(s: str) -> Optional['ScrubAggression']:
        """
        Parse a ScrubAggression from a str
        :param s: str.  The string to parse
        :return:  ScrubAggression
        """
        if s == "aggressive":
            return ScrubAggression.Aggressive
        elif s == "lazy":
            return ScrubAggression.Lazy
        elif s == "normal":
            return ScrubAggression.Normal
        else:
            return None


class ScrubControl(Enum):
    Pause = "pause"
    Resume = "resume"
    Status = "status"
    OnDemand = "ondemand"

    def __str__(self):
        return "scrub"

    @staticmethod
    def from_str(s: str) -> Optional['ScrubControl']:
        """
        Parse a ScrubControl from a string
        :param s: str.  The string to parse
        :return:  ScrubControl
        """
        if s == "pause":
            return ScrubControl.Pause
        elif s == "resume":
            return ScrubControl.Resume
        elif s == "status":
            return ScrubControl.Status
        elif s == "ondemand":
            return ScrubControl.OnDemand
        else:
            return None


class ScrubSchedule(Enum):
    Hourly = "hourly"
    Daily = "daily"
    Weekly = "weekly"
    BiWeekly = "biweekly"
    Monthly = "monthly"

    @staticmethod
    def from_str(s: str) -> Optional['ScrubSchedule']:
        """
        Parse a ScrubSchedule from a str
        :param s: str.  The string to parse
        :return: ScrubSchedule
        """
        if s == "hourly":
            return ScrubSchedule.Hourly
        elif s == "daily":
            return ScrubSchedule.Daily
        elif s == "weekly":
            return ScrubSchedule.Weekly
        elif s == "biweekly":
            return ScrubSchedule.BiWeekly
        elif s == "monthly":
            return ScrubSchedule.Monthly
        else:
            return None

    def __str__(self):
        return "scrub-frequency"


class BitrotOption(object):
    ScrubThrottle = ScrubAggression
    ScrubFrequency = ScrubSchedule
    Scrub = ScrubControl

    def __init__(self, option):
        self.option = option

    def __str__(self):
        return "{}".format(self.option)


# A Gluster Brick consists of a Peer and a path to the mount point
class Brick(object):
    def __init__(self, brick_uuid: Optional[uuid.UUID], peer: Peer, path,
                 is_arbiter: bool) -> None:
        """
        A Gluster brick
        :param brick_uuid: uuid.  Uuid of the host this brick is located on
        :param peer: Peer.  Optional information about the Peer this brick
          is located on.
        :param path: String.  The filesystem path the brick is located at
        :param is_arbiter:  bool.  Whether this brick is an arbiter or not
        """
        self.uuid = brick_uuid
        self.peer = peer
        self.path = path
        self.is_arbiter = is_arbiter

    # Returns a String representation of the selected enum variant.
    def __str__(self):
        if self.peer is not None:
            return "{}:{}".format(self.peer.hostname, self.path)
        else:
            return self.path

    def __eq__(self, other):
        if not isinstance(other, Brick):
            return False
        typing.cast(other, Brick)
        return (self.uuid == other.uuid and
                self.peer == other.peer and
                self.path == other.path and
                self.is_arbiter == other.is_arbiter)


class Quota(object):
    def __init__(self, path: str, hard_limit: int, soft_limit: int,
                 soft_limit_percentage: str,
                 used: int, avail: int, soft_limit_exceeded: str,
                 hard_limit_exceeded: str) -> None:
        """
        A Quota can be used set limits on the pool usage.
        All limits are set in bytes.
        :param path: String. Filesystem path of the quota
        :param hard_limit: int. Hard byte limit
        :param soft_limit: int. Soft byte limit
        :param soft_limit_percentage: int. Soft limit percentage
        :param used: int.  Amount of bytes used of the quota amount
        :param avail: int.  Amount of bytes left of the quota amount
        :param soft_limit_exceeded: str.  Soft limit has been exceeded
        :param hard_limit_exceeded: str.  Hard limit has been exceeded.
        """
        self.path = path
        self.hard_limit = int(hard_limit)
        self.soft_limit = int(soft_limit)
        self.soft_limit_percentage = soft_limit_percentage
        self.used = int(used)
        self.avail = int(avail)
        if soft_limit_exceeded == "No":
            self.soft_limit_exceeded = False
        else:
            self.soft_limit_exceeded = True
        if hard_limit_exceeded == "No":
            self.hard_limit_exceeded = False
        else:
            self.hard_limit_exceeded = True

    def __eq__(self, other):
        return (self.path == other.path and
                self.hard_limit == other.hard_limit and
                self.soft_limit == other.soft_limit and
                self.soft_limit_percentage == other.soft_limit_percentage and
                self.used == other.used and self.avail == other.avail and
                self.soft_limit_exceeded == other.soft_limit_exceeded and
                self.hard_limit_exceeded == other.hard_limit_exceeded)

    def __str__(self):
        return "path:{path} hard limit:{hard_limit} " \
               "soft limit percentage: {soft_limit_percentage} " \
               "soft limit: {soft_limit} used: {used} " \
               "available: {avail} soft limit exceeded: {soft_exceeded} " \
               "hard limit exceeded: {hard_exceeded}" \
               "".format(path=self.path, hard_limit=self.hard_limit,
                         soft_limit_percentage=self.soft_limit_percentage,
                         soft_limit=self.soft_limit,
                         used=self.used,
                         avail=self.avail,
                         soft_exceeded=self.soft_limit_exceeded,
                         hard_exceeded=self.hard_limit_exceeded)

    @staticmethod
    def from_xml(element: etree.Element) -> 'Quota':
        path = None
        hard_limit = None
        soft_limit_percent = None
        soft_limit = None
        used_space = None
        avail_space = None
        soft_limit_exceeded = None
        hard_limit_exceeded = None
        for limit_info in element:
            if limit_info.tag == 'path':
                path = limit_info.text
            elif limit_info.tag == 'hard_limit':
                hard_limit = int(limit_info.text)
            elif limit_info.tag == 'soft_limit_percent':
                soft_limit_percent = limit_info.text
            elif limit_info.tag == 'soft_limit_value':
                soft_limit = int(limit_info.text)
            elif limit_info.tag == 'used_space':
                used_space = int(limit_info.text)
            elif limit_info.tag == 'avail_space':
                avail_space = int(limit_info.text)
            elif limit_info.tag == 'sl_exceeded':
                soft_limit_exceeded = limit_info.text
            elif limit_info.tag == 'hl_exceeded':
                hard_limit_exceeded = limit_info.text
        return Quota(path=path, hard_limit=hard_limit,
                     soft_limit=soft_limit,
                     soft_limit_percentage=soft_limit_percent,
                     used=used_space, avail=avail_space,
                     soft_limit_exceeded=soft_limit_exceeded,
                     hard_limit_exceeded=hard_limit_exceeded)


class BrickStatus(object):
    def __init__(self, brick: Brick, tcp_port: Optional[int],
                 rdma_port: Optional[int],
                 online: bool, pid: int) -> None:
        """
        brick: Brick,
        tcp_port: u16.  The tcp port
        rdma_port: u16. The rdma port
        online: bool. Whether the Brick is online or not
        pid: u16.  The process id of the Brick
        """
        self.brick = brick
        self.tcp_port = tcp_port
        self.rdma_port = rdma_port
        self.online = online
        self.pid = pid

    def __eq__(self, other):
        return self.brick.peer == other.brick.peer

    def __str__(self):
        return "BrickStatus {} tcp port: {} rdma port: {} " \
               "online: {} pid: ".format(self.brick,
                                         self.tcp_port,
                                         self.rdma_port,
                                         self.online,
                                         self.pid)


class Transport(Enum):
    """
        An enum to select the transport method Gluster should import
        for the Volume
    """
    Tcp = "tcp"
    TcpAndRdma = "tcp,rdma"
    Rdma = "rdma"

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(transport):
        if transport == "tcp":
            return Transport.Tcp
        elif transport == "tcp,rdma":
            return Transport.TcpAndRdma
        elif transport == "rdma":
            return Transport.Rdma
        elif transport == "0":
            return Transport.Tcp
        else:
            return None


class VolumeTranslator(Enum):
    Arbiter = "arbiter"
    Disperse = "disperse"
    Replica = "replica"
    Redundancy = "redundancy"
    Stripe = "stripe"

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(translator):
        if translator == "arbiter":
            return VolumeTranslator.Arbiter
        elif translator == "disperse":
            return VolumeTranslator.Disperse
        elif translator == "replica":
            return VolumeTranslator.Replica
        elif translator == "redundancy":
            return VolumeTranslator.Redundancy
        elif translator == "stripe":
            return VolumeTranslator.Stripe
        else:
            return None


# These are all the different Volume types that are possible in Gluster
# Note: Tier is not represented here becaimport I'm waiting for it to become
# more stable
# For more information about these types see: [Gluster Volume]
# (https:#gluster.readthedocs.
# org/en/latest/Administrator%20Guide/Setting%20Up%20Volumes/)
class VolumeType(Enum):
    Arbiter = "arbiter"
    Distribute = "distribute"
    Stripe = "stripe"
    Replicate = "replicate"
    StripedAndReplicate = "stripd-replicate"
    Disperse = "disperse"
    # Tier,
    DistributedAndStripe = "distributed-stripe"
    DistributedAndReplicate = "distributed-replicate"
    DistributedAndStripedAndReplicate = "distributed-striped-replicate"
    DistributedAndDisperse = "distributed-disperse"

    def __str__(self):
        return "{}".format(self.value)

    # Returns a enum variant of the given String
    @staticmethod
    def from_str(vol_type: str):
        if vol_type == "Arbiter":
            return VolumeType.Arbiter
        elif vol_type == "Distribute":
            return VolumeType.Distribute
        elif vol_type == "Stripe":
            return VolumeType.Stripe,
        elif vol_type == "Replicate":
            return VolumeType.Replicate
        elif vol_type == "Striped-Replicate":
            return VolumeType.StripedAndReplicate
        elif vol_type == "Disperse":
            return VolumeType.Disperse
            # TODO: Waiting for this to become stable
            # VolumeType::Tier => "Tier",
        elif vol_type == "Distributed-Stripe":
            return VolumeType.DistributedAndStripe
        elif vol_type == "Distributed-Replicate":
            return VolumeType.DistributedAndReplicate
        elif vol_type == "Distributed-Striped-Replicate":
            return VolumeType.DistributedAndStripedAndReplicate
        elif vol_type == "Distrubted-Disperse":
            return VolumeType.DistributedAndDisperse
        else:
            return None


class Volume(object):
    """
    A volume is a logical collection of bricks. Most of the gluster management
    operations happen on the volume.
    """

    def __init__(self, name: str, vol_type: VolumeType, vol_id: uuid.UUID,
                 status: str,
                 snapshot_count: int,
                 dist_count: int,
                 stripe_count: int,
                 replica_count: int,
                 arbiter_count: int,
                 disperse_count: int,
                 redundancy_count: int,
                 transport: Transport,
                 bricks: List[Brick],
                 options: Dict[str, str]) -> None:
        """
        :param name: String.  Name of the volume
        :param vol_type: VolumeType.
        :param vol_id: uuid
        :param status: String.  Status of the volume
        :param transport: Transport.  Transport protocol
        :param bricks: list.  List of Brick
        :param options: dict.  String:String mapping of volume options
        """
        self.name = name
        self.vol_type = vol_type
        self.vol_id = vol_id
        self.status = status
        self.snapshot_count = snapshot_count
        self.dist_count = dist_count
        self.stripe_count = stripe_count
        self.replica_count = replica_count
        self.arbiter_count = arbiter_count
        self.disperse_count = disperse_count
        self.redundancy_count = redundancy_count
        self.transport = transport
        self.bricks = bricks
        self.options = options

    def __str__(self):
        return self.__dict__

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


def volume_list() -> List[str]:
    """
    # Lists all available volume names.
    # # Failures
    # Will return None if the Volume list command failed or if volume could not
    # be transformed
    # into a String from utf8
    """
    try:
        l = volume.vollist()
        return l
    except AttributeError:
        return []


def volume_info(vol_name: str) -> List[Volume]:
    """
    Returns a Volume with all available information on the volume
    volume: String.  The volume to gather info about
    :return: List[Volume].  The volume information
    :raises: GlusterError if the command fails to run
    """
    try:
        info = volume.info(vol_name)
        if len(info) > 0:
            v = info[0]
            brick_list = []
            for brick in v['bricks']:
                ip_addr = brick['name'].split(':')[0]
                path = brick['name'].split(':')[1]
                brick_list.append(
                    Brick(
                        brick_uuid=uuid.UUID(brick['uuid']),
                        peer=Peer(uuid=None,
                                  hostname=ip_addr,
                                  status=None),
                        path=path,
                        # Not enough info to answer this next field
                        is_arbiter=False))
            return [Volume(
                name=v['name'],
                vol_id=uuid.UUID(v['uuid']),
                vol_type=VolumeType.from_str(v['type']),
                status=v['status'],
                snapshot_count=None,
                dist_count=v['distribute'],
                stripe_count=v['stripe'],
                replica_count=v['replica'],
                arbiter_count=None,
                disperse_count=None,
                redundancy_count=None,
                transport=Transport.from_str(v['transport']),
                bricks=brick_list,
                options=v['options'],
            )]
        else:
            return []
    except GlusterCmdOutputParseError:
        raise


def quota_list(vol_name: str) -> Result:
    """
    Return a list of quotas on the volume if any
    Enable bitrot detection and remediation on the volume
    volume: String.  The volume to operate on.
    :return: List of Quota's on the volume
    :raises: GlusterError if the command fails to run
    """
    cmd = ["gluster", "volume", "quota", vol_name, "list", "--xml"]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.PIPE)
        quota_list_result = parse_quota_list(output.decode('utf-8'))
        return quota_list_result
    except subprocess.CalledProcessError as e:
        log("subprocess failed stdout: {} stderr: {} returncode: {}".format(
            e.stdout, e.stderr, e.returncode), ERROR)
        return Err("Volume quota list command failed with error: {}".format(
            e.stderr))


def parse_quota_list(output_xml: str) -> Result:
    """
    Return a list of quotas on the volume if any
    :param output_xml:
    """
    tree = etree.fromstring(output_xml)
    result = check_return_code(tree)
    if result.is_err():
        return Err(result.value)

    xml_quotas_list = tree.findall('./volQuota/limit')
    quotas = [Quota.from_xml(node) for node in xml_quotas_list]
    return Ok(quotas)


def volume_enable_bitrot(vol_name: str) -> None:
    """
    Enable bitrot detection and remediation on the volume
    volume: String.  The volume to operate on.
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    try:
        bitrot.enable(vol_name)
    except GlusterCmdException:
        raise


def volume_disable_bitrot(vol_name: str) -> None:
    """
    Disable bitrot detection and remediation on the volume
    volume: String.  The volume to operate on.
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    try:
        bitrot.disable(vol_name)
    except GlusterCmdException:
        raise


def volume_set_bitrot_option(vol_name: str, setting: BitrotOption) -> None:
    """
    Set a bitrot option on the vol_name
    vol_name: String.  The vol_name to operate on.
    setting: BitrotOption.  The option to set on the bitrot daemon
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    try:
        if setting == BitrotOption.ScrubThrottle:
            bitrot.scrub_throttle(volname=vol_name, throttle_type=str(setting))
        elif setting == BitrotOption.ScrubFrequency:
            bitrot.scrub_frequency(volname=vol_name, freq=str(setting))
        elif setting == BitrotOption.Scrub.Pause:
            bitrot.scrub_pause(volname=vol_name)
        elif setting == BitrotOption.Scrub.Resume:
            bitrot.scrub_resume(volname=vol_name)
        elif setting == BitrotOption.Scrub.Status:
            bitrot.scrub_status(volname=vol_name)
    except GlusterCmdException:
        raise


def volume_enable_quotas(vol_name: str) -> None:
    """
    Enable quotas on the volume
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    try:
        quota.enable(volname=vol_name)
    except GlusterCmdException:
        raise


def volume_quotas_enabled(vol_name: str) -> Result:
    """
     Check if quotas are already enabled on a vol_name
    :return: bool.  True/False if quotas are enabled
    :raises: GlusterError if the command fails to run
    """
    vol_info = volume_info(vol_name)
    for vol in vol_info:
        if vol.name == vol_name:
            quota = vol.options["features.quota"]
            if quota is None or quota == "false":
                return Ok(False)
            elif quota == "on":
                return Ok(True)
            else:
                # No idea what this is
                return Err(
                    "Unknown features.quota setting: {}. Cannot discern "
                    "if quota is enabled or not".format(quota))
    return Err(
        "Unknown vol_name: {}. Failed to get quota information".format(
            vol_name))


def volume_disable_quotas(vol_name: str) -> None:
    """
    Disable quotas on the vol_name
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    try:
        quota.disable(volname=vol_name)
    except GlusterCmdException:
        raise


def volume_remove_quota(vol_name: str, path: str) -> None:
    """
    Removes a size quota to the vol_name and path.
    path: String.  Path of the directory to remove a quota on
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    try:
        quota.remove_path(volname=vol_name, path=path)
    except GlusterCmdException:
        raise


def volume_add_quota(vol_name: str, path: str, size: int) -> None:
    """
    Adds a size quota to the volume and path.
    volume: String Volume to add a quota to
    path: String.  Path of the directory to apply a quota on
    size: int. Size in bytes of the quota to apply
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    try:
        quota.limit_usage(volname=vol_name, path=path, size=size)
    except GlusterCmdException:
        raise


def ok_to_remove(vol_name: str, brick: Brick) -> Result:
    """
    Based on the replicas or erasure bits that are still available in the
    volume this will return
    True or False as to whether you can remove a Brick. This should be called
    before volume_remove_brick()
    volume: String. Volume to check if the brick is ok to remove
    brick: Brick. Brick to check
    :param vol_name: str.  Volume to check
    :param brick: Brick.  Brick to check if it is ok to remove
    :return: bool.  True/False if the Brick is safe to remove from the volume
    """
    # TODO: This command doesn't give me enough information to make a decision
    volume.status_detail(volname=vol_name)
    # The redundancy requirement is needed here.
    # The code needs to understand what
    # volume type it's operating on.
    return Ok(True)


#  def volume_shrink_replicated(volume: str,
# replica_count: usize,
# bricks: Vec<Brick>,
# force) -> Result<i32,String>
# volume remove-brick <VOLNAME> [replica <COUNT>] <BRICK> ...
# <start|stop|status|c
# ommit|force> - remove brick from volume <VOLNAME>
#
#


def volume_status(vol_name: str) -> List[Dict]:
    """
        Query the status of the volume given.
        :return: list.  List of BrickStatus
        :raise: Raises GlusterError on exception
    """
    try:
        return volume.status_detail(vol_name)
    except GlusterCmdException:
        raise


# def volume_shrink_replicated(volume: str,
# replica_count: usize,
# bricks: Vec<Brick>,
# force) -> Result<i32,String>
# volume remove-brick <VOLNAME> [replica <COUNT>] <BRICK> ...
# <start|stop|status|c
# ommit|force> - remove brick from volume <VOLNAME>
#
#


def volume_remove_brick(volume: str, brick_list: List[Brick],
                        force: bool) -> None:
    """
    This will remove bricks from the volume
    :param volume: String of the volume to remove bricks from.
    :param brick_list:  list.  List of bricks to remove from the volume
    :param force:  bool.  Force remove brick
    :return: int.  Negative number on error
    """

    if len(brick_list) == 0:
        return Err("The brick list is empty.  Not removing brick")

    bricks.remove_start(volume, brick_list, force=force)


def volume_add_brick(volume: str, brick_list: List[Brick],
                     force: bool) -> None:
    """
    volume add-brick <VOLNAME> [<stripe|replica> <COUNT>]
    <NEW-BRICK> ... [force] - add brick to volume <VOLNAME>
    This adds a new brick to the volume
    :param volume: String of the volume to add bricks to.
    :param brick_list:  list.  List of bricks to add to the volume
    :param force:  bool.  Force add brick
    :return: Result.  Ok or Err
    """

    if not brick_list:
        raise ValueError("The brick list is empty.  Not expanding volume")
    try:
        bricks.add(volume, brick_list, force=force)
    except GlusterCmdException:
        raise


def volume_start(vol_name: str, force: bool) -> None:
    # Should I check the volume exists first?
    """
    Once a volume is created it needs to be started.  This starts the volume
    :param vol_name: String of the volume to start.
    :param force:  bool.  Force start
    :return: Result.  Ok or Err
    """
    try:
        volume.start(vol_name, force)
    except GlusterCmdException:
        raise


def volume_stop(vol_name: str, force: bool) -> None:
    """
    This stops a running volume
    :param vol_name:  String of the volume to stop
    :param force:  bool. Force stop.
    :return: Result.  Ok or Err
    """
    try:
        volume.stop(vol_name, force)
    except GlusterCmdException:
        raise


def volume_delete(vol_name: str) -> None:
    """
    This deletes a stopped volume
    :param vol_name:  String of the volume name to delete
    :return: Result.  Ok or Err
    """
    try:
        volume.delete(vol_name)
    except GlusterCmdException:
        raise


def volume_rebalance(vol_name: str) -> None:
    """
    # This function doesn't do anything yet.  It is a place holder because
    # volume_rebalance is a long running command and I haven't decided how to
    # poll for completion yet
    # Usage: volume rebalance <VOLNAME> fix-layout start | start
    # [force]|stop|status
    :param vol_name: str.  The name of the volume to start rebalancing
    :return: Result.  Ok or Err
    """
    try:
        rebalance.start(vol_name)
    except GlusterCmdException:
        raise


def vol_set(vol_name: str, options: Dict[str, str]) -> None:
    """
    :param vol_name: String. Volume name to set the option on
    :param options: GlusterOption
    :return: Result.  Return code and output of cmd
    """
    try:
        volume.optset(volname=vol_name, opts=options)
    except GlusterCmdException as e:
        log("volume.optsetfailed: {}".format(e), ERROR)
        raise


def volume_set_options(volume: str, settings: List[GlusterOption]) -> Result:
    """
    Set an option on the volume
    :param volume: String. Volume name to set the option on
    :param settings: list of GlusterOption
    """
    # # Failures
    # Will return GlusterError if the command fails to run
    error_list = []
    options = {}
    for setting in settings:
        options[setting.option] = str(setting.value)
    try:
        vol_set(volume, options)
    except GlusterCmdException as e:
        error_list.append(e)

    if len(error_list) > 0:
        return Err("\n".join(error_list))
    return Ok()


def volume_create_replicated(vol: str, replica_count: int,
                             transport: Transport, bricks: List[Brick],
                             force: bool) -> Result:
    """
    This creates a new replicated volume
    :param replica_count:
    :param transport:
    :param bricks:
    :param force:
    :param vol: String. Volume name to set the option on
    :return: Result.  If Ok() stdout is returned.  Err returns stderr
    """
    try:
        out = volume.create(volname=vol,
                            volbricks=[str(b) for b in bricks],
                            replica=replica_count, transport=str(transport),
                            force=force)
        return Ok(out)
    except GlusterCmdException as e:
        return Err(e)


def volume_create_arbiter(vol: str, replica_count: int, arbiter_count: int,
                          transport: Transport,
                          bricks: List[Brick], force: bool) -> Result:
    """
    The arbiter volume is special subset of replica volumes that is aimed at
    preventing split-brains and providing the same consistency guarantees
    as a normal replica 3 volume without consuming 3x space.
    :param vol: The volume name to create
    :param replica_count:
    :param arbiter_count:
    :param transport:
    :param bricks:
    :param force:
    :return: Result.  If Ok() stdout is returned.  Err returns stderr
    """
    try:
        out = volume.create(volname=vol, volbricks=[str(b) for b in bricks],
                            replica=replica_count,
                            arbiter=arbiter_count, force=force,
                            transport=str(transport))
        return Ok(out)
    except GlusterCmdException as e:
        return Err(e)


def volume_create_striped(vol: str, stripe_count: int, transport: Transport,
                          bricks: List[Brick], force: bool) -> Result:
    """
    This creates a new striped volume
    :param vol:
    :param stripe_count:
    :param transport:
    :param bricks:
    :param force:
    :return: Result.  If Ok() stdout is returned.  Err returns stderr
    """
    try:
        out = volume.create(volname=vol, stripe=stripe_count,
                            volbricks=[str(b) for b in bricks],
                            transport=str(transport), force=force)
        return Ok(out)
    except GlusterCmdException as e:
        return Err(e)


def volume_create_striped_replicated(vol: str, stripe_count: int,
                                     replica_count: int,
                                     transport: Transport, bricks: List[Brick],
                                     force: bool) -> Result:
    """
    This creates a new striped and replicated volume
    :param vol:
    :param stripe_count:
    :param replica_count:
    :param transport:
    :param bricks:
    :param force:
    :return: Result.  If Ok() stdout is returned.  Err returns stderr
    """
    try:
        out = volume.create(volname=vol, stripe=stripe_count,
                            volbricks=[str(b) for b in bricks],
                            replica=replica_count, transport=str(transport),
                            force=force)
        return Ok(out)
    except GlusterCmdException as e:
        return Err(e)


def volume_create_distributed(vol: str, transport: Transport,
                              bricks: List[Brick], force: bool) -> Result:
    """
    This creates a new distributed volume
    :param vol:
    :param transport:
    :param bricks:
    :param force:
    :return: Result.  If Ok() stdout is returned.  Err returns stderr
    """
    try:
        out = volume.create(volname=vol,
                            volbricks=[str(b) for b in bricks],
                            transport=str(transport),
                            force=force)
        return Ok(out)
    except GlusterCmdException as e:
        return Err(e)


def volume_create_erasure(vol: str, disperse_count: int,
                          redundancy_count: Optional[int],
                          transport: Transport,
                          bricks, force: bool) -> Result:
    """
    This creates a new erasure coded volume
    :param vol: String
    :param disperse_count: int
    :param redundancy_count: int
    :param transport: Transport
    :param bricks: list of Brick
    :param force: bool
    :return: Result.  If Ok() stdout is returned.  Err returns stderr
    """
    try:
        out = volume.create(volname=vol,
                            disperse=disperse_count,
                            redundancy=redundancy_count,
                            volbricks=[str(b) for b in bricks],
                            transport=str(transport),
                            force=force)
        return Ok(out)
    except GlusterCmdException as e:
        return Err(e)


def get_local_ip() -> Result:
    """
    Returns the local IPAddr address associated with this server
    # Failures
    Returns a GlusterError representing any failure that may have happened
    while trying to
    query this information.
    """
    ip_addr = get_host_ip(unit_get('private-address'))
    try:
        parsed = ip_address(address=ip_addr)
        return Ok(parsed)  # Resolves a str hostname into a ip address.
    except ValueError:
        return Err("failed to parse ip address: {}".format(ip_addr))


def get_local_bricks(volume: str) -> Result:
    """
        Return all bricks that are being served locally in the volume
        volume: Name of the volume to get local bricks for
    """
    try:
        vol_info = volume_info(volume)
        local_ip = get_local_ip()
        if local_ip.is_err():
            return Err(local_ip.value)
        local_brick_list = []
        for vol in vol_info:
            for brick in vol.bricks:
                if ip_address(brick.peer.hostname) == local_ip.value:
                    local_brick_list.append(brick)
        return Ok(local_brick_list)
    except GlusterCmdOutputParseError:
        raise
