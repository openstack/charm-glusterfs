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

import unittest
import uuid
from ipaddress import ip_address

import mock
from lib.charm.gluster import peer
from lib.charm.gluster.peer import Peer, State


class Test(unittest.TestCase):
    @mock.patch('lib.charm.gluster.peer.peer_list')
    def testGetPeer(self, _peer_list):
        existing_peers = [
            peer.Peer(
                uuid=uuid.UUID("663bbc5b-c9b4-4a02-8b56-85e05e1b01c8"),
                hostname=ip_address("172.31.12.7"),
                status=peer.State.PeerInCluster),
            peer.Peer(
                uuid=uuid.UUID("15af92ad-ae64-4aba-89db-73730f2ca6ec"),
                hostname=ip_address("172.31.21.242"),
                status=peer.State.PeerInCluster)
        ]
        _peer_list.return_value = existing_peers
        result = peer.get_peer(hostname=ip_address('172.31.21.242'))
        self.assertIs(result, existing_peers[1])

    @mock.patch('lib.charm.gluster.peer.gpeer.pool')
    def testPeerList(self, _peer_pool):
        # Ignore parse_peer_list.  We test that above
        peer.peer_list()
        # _run_command.assert_called_with(command="gluster",
        #                                arg_list=["pool", "list", "--xml"],
        #                                script_mode=False)

    @mock.patch('lib.charm.gluster.peer.peer_list')
    @mock.patch('lib.charm.gluster.peer.gpeer.probe')
    def testPeerProbe(self, _peer_probe, _peer_list):
        _peer_list.return_value = [
            Peer(hostname="172.31.18.192",
                 uuid=uuid.UUID('832e2e64-24c7-4f05-baf5-42431fd801e2'),
                 status=State.Connected),
            Peer(hostname="localhost",
                 uuid=uuid.UUID('d16f8c77-a0c5-4c31-a8eb-0cfbf7d7d1a5'),
                 status=State.Connected)]
        # Probe a new hostname that's not currently in the cluster
        peer.peer_probe(hostname='172.31.18.194')
        _peer_probe.assert_called_with('172.31.18.194')


if __name__ == "__main__":
    unittest.main()
