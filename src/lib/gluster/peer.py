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
import uuid
from enum import Enum
from typing import Optional, List

from charmhelpers.core.hookenv import log
from gluster.cli import peer as gpeer, GlusterCmdException
from gluster.cli.parsers import GlusterCmdOutputParseError

from ..utils.utils import resolve_to_ip


# A enum representing the possible States that a Peer can be in
class State(Enum):
    Connected = "connected"
    Disconnected = "disconnected"
    Unknown = ""
    EstablishingConnection = "establishing connection"
    ProbeSentToPeer = "probe sent to peer"
    ProbeReceivedFromPeer = "probe received from peer"
    PeerInCluster = "peer in cluster"
    AcceptedPeerRequest = "accepted peer in cluster"
    SentAndReceivedPeerRequest = "sent and received peer request"
    PeerRejected = "peer rejected"
    PeerDetachInProgress = "peer detach in progress"
    ConnectedToPeer = "connected to peer"
    PeerIsConnectedAndAccepted = "peer is connected and accepted"
    InvalidState = "invalid state"

    def __str__(self) -> str:
        return "{}".format(self.value)

    @staticmethod
    def from_str(string: str):
        """Parses the string to return the appropriate State instance.
        The python3 enum class already has some attempt to find the correct
        object when the State class is constructed with a value, but may
        not be obvious what's going on. Parsing a string allows us to
        create a more rich version of data stored in the enum (e.g. a tuple)
        but also allows our own custom parsing.
        :param string: the string to parse
        :return State: the corresponding State object
        :raises ValueError: if the string cannot parse to a State object.
        """
        if string:
            for state in State:
                if state.value.lower() == string.lower():
                    return state

        raise ValueError("Unable to find State for string: {}".format(string))

    """
    @staticmethod
    def from_str(s: str):
        s = s.lower()
        if s == 'connected':
            return State.Connected
        elif s == 'disconnected':
            return State.Disconnected
        elif s == 'establishing connection':
            return State.EstablishingConnection
        elif s == 'probe sent to peer':
            return State.ProbeSentToPeer
        elif s == 'probe received from peer':
            return State.ProbeReceivedFromPeer
        elif s == 'peer in cluster':
            return State.PeerInCluster
        elif s == 'accepted peer in cluster':
            return State.AcceptedPeerRequest
        elif s == "sent and received peer request":
            return State.SentAndReceivedPeerRequest
        elif s == "peer rejected":
            return State.PeerRejected
        elif s == "peer detach in progress":
            return State.PeerDetachInProgress
        elif s == "connected to peer":
            return State.ConnectedToPeer
        elif s == "peer is connected and accepted":
            return State.PeerIsConnectedAndAccepted
        elif s == "invalid state":
            return State.InvalidState
        else:
            return None
    """


class Peer(object):
    def __init__(self, uuid: uuid.UUID, hostname: str,
                 status: Optional[State]) -> None:
        """
        A Gluster Peer.  A Peer is roughly equivalent to a server in Gluster.
        :param uuid: uuid.UUID. Unique identifier of this peer
        :param hostname: str. ip address of the peer
        :param status:  Optional[State] current State of the peer
        """
        self.uuid = uuid
        self.hostname = hostname
        self.status = status

    def __eq__(self, other):
        return self.uuid == other.uuid

    def __str__(self):
        return "UUID: {}  Hostname: {} Status: {}".format(
            self.uuid,
            self.hostname,
            self.status)


def get_peer(hostname: str) -> Optional[Peer]:
    """
    This will query the Gluster peer list and return a Peer class for the peer
    :param hostname: str.  ip address of the peer to get
    :return Peer or None in case of not found
    """
    peer_pool = peer_list()

    for node in peer_pool:
        if node.hostname == hostname:
            return node
    return None


def peer_status() -> List[Peer]:
    """
    Runs gluster peer status and returns the status of all the peers
    in the cluster
    Returns GlusterError if the command failed to run
    :return: List of Peers
    """
    try:
        status = gpeer.status()
        peers = []
        for peer in status:
            p = Peer(uuid=uuid.UUID(peer['uuid']),
                     status=State.from_str(peer['connected']),
                     hostname=peer['hostname'])
            peers.append(p)
        return peers
    except GlusterCmdOutputParseError:
        raise


def peer_list() -> List[Peer]:
    """
    List all peers including localhost
    Runs gluster pool list and returns a List[Peer] representing all the peers
    in the cluster
    This also returns information for the localhost as a Peer. peer_status()
    does not
    # Failures
    Returns GlusterError if the command failed to run
    """
    try:
        parsed_peers = []
        pool_list = gpeer.pool()

        for value in pool_list:
            ip_addr = resolve_to_ip(value['hostname'])
            if ip_addr.is_err():
                log("Failed to resolve {} to ip address, skipping peer".format(
                    value['hostname']))
                continue
            parsed_peers.append(
                Peer(
                    hostname=ip_addr.value,
                    uuid=uuid.UUID(value['uuid']),
                    status=State.from_str(value['connected'])))
        return parsed_peers
    except GlusterCmdOutputParseError:
        raise


def peer_probe(hostname: str) -> None:
    """
    Probe a peer and prevent double probing
    Adds a new peer to the cluster by hostname or ip address
    :param hostname: String.  Add a host to the cluster
    :return:
    """
    try:
        current_peers = peer_list()
        for current_peer in current_peers:
            if current_peer.hostname == hostname:
                # Bail instead of double probing
                return
    except GlusterCmdOutputParseError:
        raise
    try:
        gpeer.probe(hostname)
    except GlusterCmdException:
        raise


def peer_remove(hostname: str) -> None:
    """
    Removes a peer from the cluster by hostname or ip address
    :param hostname: String.  Hostname to remove from the cluster
    :return:
    """
    try:
        gpeer.detach(hostname)
    except GlusterCmdException:
        raise
