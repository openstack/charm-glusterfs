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
from lib.charm.gluster import peer, volume

# mock_apt = mock.MagicMock()
# sys.modules['apt'] = mock_apt
# mock_apt.apt_pkg = mock.MagicMock()

peer_1 = peer.Peer(
    uuid=uuid.UUID("39bdbbd6-5271-4c23-b405-cc0b67741ebc"),
    hostname="172.20.21.231", status=None)
peer_2 = peer.Peer(
    uuid=uuid.UUID("a51b28e8-6f06-4563-9a5f-48f3f31a6713"),
    hostname="172.20.21.232", status=None)
peer_3 = peer.Peer(
    uuid=uuid.UUID("57dd0230-50d9-452a-be8b-8f9dd9fe0264"),
    hostname="172.20.21.233", status=None)

brick_list = [
    volume.Brick(
        brick_uuid=uuid.UUID("12d4bd98-e102-4174-b99a-ef76f849474e"),
        peer=peer_1,
        path="/mnt/sdb",
        is_arbiter=False),
    volume.Brick(
        brick_uuid=uuid.UUID("a563d73c-ef3c-47c6-b50d-ddc800ef5dae"),
        peer=peer_2,
        path="/mnt/sdb",
        is_arbiter=False),
    volume.Brick(
        brick_uuid=uuid.UUID("cc4a3f0a-f152-4e40-ab01-598f53eb83f9"),
        peer=peer_3,
        path="/mnt/sdb", is_arbiter=False)
]


class Test(unittest.TestCase):
    def testGetLocalBricks(self):
        pass

    def testOkToRemove(self):
        pass

    @mock.patch("lib.charm.gluster.volume.unit_get")
    @mock.patch("lib.charm.gluster.volume.get_host_ip")
    def testGetLocalIp(self, _get_host_ip, _unit_get):
        _unit_get.return_value = "192.168.1.6"
        _get_host_ip.return_value = "192.168.1.6"
        result = volume.get_local_ip()
        self.assertTrue(result.is_ok())
        self.assertTrue(result.value == ip_address("192.168.1.6"))

    def testParseQuotaList(self):
        expected_quotas = [
            volume.Quota(path="/", hard_limit=10240, soft_limit=8192,
                         soft_limit_percentage="80%", used=0, avail=10240,
                         soft_limit_exceeded="No", hard_limit_exceeded="No"),
            volume.Quota(path="/test2", hard_limit=10240, soft_limit=8192,
                         soft_limit_percentage="80%", used=0, avail=10240,
                         soft_limit_exceeded="No", hard_limit_exceeded="No"),
        ]
        with open('unit_tests/quota_list.xml', 'r') as xml_output:
            lines = xml_output.readlines()
            result = volume.parse_quota_list("".join(lines))
            self.assertTrue(result.is_ok())
            self.assertTrue(len(result.value) == 2)
            for quota in result.value:
                self.assertTrue(quota in expected_quotas)

    def testVolumeAddBrick(self):
        pass

    @mock.patch('lib.charm.gluster.volume.volume.create')
    def testVolumeCreateArbiter(self, _volume_create):
        volume.volume_create_arbiter(vol="test", replica_count=3,
                                     arbiter_count=1,
                                     transport=volume.Transport.Tcp,
                                     bricks=brick_list, force=False)
        _volume_create.assert_called_with(
            volname='test', replica=3, arbiter=1, transport='tcp',
            volbricks=[str(b) for b in brick_list], force=False)

    @mock.patch('lib.charm.gluster.volume.volume.create')
    def testVolumeCreateDistributed(self, _volume_create):
        volume.volume_create_distributed(vol="test",
                                         transport=volume.Transport.Tcp,
                                         bricks=brick_list, force=False)
        _volume_create.assert_called_with(volname="test", transport='tcp',
                                          volbricks=[str(b) for b in
                                                     brick_list], force=False)

    @mock.patch('lib.charm.gluster.volume.volume.create')
    def testVolumeCreateErasure(self, _volume_create):
        volume.volume_create_erasure(vol="test", disperse_count=1,
                                     redundancy_count=3,
                                     transport=volume.Transport.Tcp,
                                     bricks=brick_list, force=False)
        _volume_create.assert_called_with(
            volname='test', disperse=1, redundancy=3, transport='tcp',
            volbricks=[str(b) for b in brick_list], force=False)

    @mock.patch('lib.charm.gluster.volume.volume.create')
    def testVolumeCreateReplicated(self, _volume_create):
        volume.volume_create_replicated(vol="test", replica_count=3,
                                        transport=volume.Transport.Tcp,
                                        bricks=brick_list, force=False)
        _volume_create.assert_called_with(
            volname='test', replica=3, transport='tcp',
            volbricks=[str(b) for b in brick_list], force=False)

    @mock.patch('lib.charm.gluster.volume.volume.create')
    def testVolumeCreateStriped(self, _volume_create):
        volume.volume_create_striped(vol="test", stripe_count=3,
                                     transport=volume.Transport.Tcp,
                                     bricks=[str(b) for b in brick_list],
                                     force=False)
        _volume_create.assert_called_with(
            volname='test', stripe=3, transport='tcp',
            volbricks=[str(b) for b in brick_list], force=False)

    @mock.patch('lib.charm.gluster.volume.volume.create')
    def testVolumeCreateStripedReplicated(self, _volume_create):
        volume.volume_create_striped_replicated(vol="test", stripe_count=1,
                                                replica_count=3,
                                                transport=volume.Transport.Tcp,
                                                bricks=brick_list, force=False)
        _volume_create.assert_called_with(
            volname='test', stripe=1, replica=3,
            transport='tcp', volbricks=[str(b) for b in brick_list],
            force=False)

    def testVolumeSetBitrotOption(self):
        pass

    def testVolumeSetOptions(self):
        pass


if __name__ == "__main__":
    unittest.main()
