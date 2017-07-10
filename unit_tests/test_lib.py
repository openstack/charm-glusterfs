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

import mock
from lib.charm.gluster import lib
from lib.charm.gluster.peer import Peer, State
from lib.charm.gluster.volume import Brick, Volume, VolumeType, Transport


class Test(unittest.TestCase):
    @mock.patch('lib.charm.gluster.lib.log')
    def testPeersAreNotReady(self, _log):
        peer_list = [
            Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc1'),
                 hostname="host-{}".format(
                     uuid.UUID('8fd64553-8925-41f5-b64a-1ba4d359c73b')),
                 status=State.PeerInCluster),
            Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc2'),
                 hostname="host-{}".format(
                     uuid.UUID('8fd64553-8925-41f5-b64a-1ba4d359c73c')),
                 status=State.AcceptedPeerRequest),
        ]
        result = lib.peers_are_ready(peer_list)
        self.assertFalse(result)

    @mock.patch('lib.charm.gluster.lib.log')
    def testPeersAreReady(self, _log):
        peer_list = [
            Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc1'),
                 hostname="host-{}".format(
                     uuid.UUID('8fd64553-8925-41f5-b64a-1ba4d359c73b')),
                 status=State.Connected),
            Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc2'),
                 hostname="host-{}".format(
                     uuid.UUID('8fd64553-8925-41f5-b64a-1ba4d359c73c')),
                 status=State.Connected),
        ]
        result = lib.peers_are_ready(peer_list)
        self.assertTrue(result)

    def testFindNewPeers(self):
        peer1 = Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc1'),
                     hostname="192.168.10.2",
                     status=State.PeerInCluster)
        peer2 = Peer(uuid=uuid.UUID('3da2c343-7c67-499d-a6bb-68591cc72bc2'),
                     hostname="192.168.10.3",
                     status=State.AcceptedPeerRequest)

        # glusterfs-0 and glusterfs-1 are in the cluster but only glusterfs-0
        # is actually serving a brick. find_new_peers should
        # return glusterfs-1 as a new peer
        peers = {
            "glusterfs-0": {
                "address": peer1.hostname,
                "bricks": ["/mnt/brick1"]
            },
            "glusterfs-1": {
                "address": peer2.hostname,
                "bricks": []
            }}
        existing_brick = Brick(peer=peer1,
                               brick_uuid=uuid.UUID(
                                   '3da2c343-7c67-499d-a6bb-68591cc72bc1'),
                               path="/mnt/brick1",
                               is_arbiter=False)
        volume_info = Volume(name="test",
                             vol_type=VolumeType.Replicate,
                             vol_id=uuid.uuid4(),
                             status="online", bricks=[existing_brick],
                             arbiter_count=0, disperse_count=0, dist_count=0,
                             replica_count=3, redundancy_count=0,
                             stripe_count=0, transport=Transport.Tcp,
                             snapshot_count=0, options={})
        new_peers = lib.find_new_peers(peers=peers, volume_info=volume_info)
        self.assertDictEqual(new_peers,
                             {"glusterfs-1": {
                                 "address": "192.168.10.3",
                                 "bricks": []}}
                             )

    def testProduct(self):
        peer1 = Peer(uuid=None,
                     hostname="server1",
                     status=None)
        peer2 = Peer(uuid=None,
                     hostname="server2",
                     status=None)
        expected = [
            Brick(peer=peer1,
                  brick_uuid=None,
                  path="/mnt/brick1",
                  is_arbiter=False),
            Brick(peer=peer2,
                  brick_uuid=None,
                  path="/mnt/brick1",
                  is_arbiter=False),
            Brick(peer=peer1,
                  brick_uuid=None,
                  path="/mnt/brick2",
                  is_arbiter=False),
            Brick(peer=peer2,
                  brick_uuid=None,
                  path="/mnt/brick2",
                  is_arbiter=False)
        ]
        peers = {
            "glusterfs-0": {
                "address": "192.168.10.2",
                "bricks": ["/mnt/brick1", "/mnt/brick2"]
            },
            "glusterfs-1": {
                "address": "192.168.10.3",
                "bricks": ["/mnt/brick1", "/mnt/brick2"]
            }}
        result = lib.brick_and_server_product(peers=peers)
        self.assertListEqual(result, expected)

    class TestTranslateToBytes(unittest.TestCase):
        def setUp(self):
            self.tests = {
                "1TB": 1099511627776.0,
                "8.2KB": 8396.8,
                "2Bytes": 2.0
            }

        def test(self):
            for test, correct in self.tests.items():
                self.assertEqual(lib.translate_to_bytes(test), correct)

        def tearDown(self):
            pass

    if __name__ == "__main__":
        unittest.main()
