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

import mock
from result import Ok

from lib.charm.gluster import block


class Test(unittest.TestCase):
    def testGetDeviceInfo(self):
        pass

    @mock.patch('lib.charm.gluster.block.scan_devices')
    @mock.patch('lib.charm.gluster.block.storage_get')
    @mock.patch('lib.charm.gluster.block.storage_list')
    @mock.patch('lib.charm.gluster.block.log')
    def testGetJujuBricks(self, _log, _storage_list, _storage_get,
                          _scan_devices):
        _storage_list.return_value = ['data/0', 'data/1', 'data/2']
        _storage_get.side_effect = lambda x, y: "/dev/{}".format(
            y.split('/')[1])
        _scan_devices.return_value = Ok(["/dev/0", "/dev/1", "/dev/2"])
        bricks = block.get_juju_bricks()
        self.assertTrue(bricks.is_ok())
        self.assertListEqual(["/dev/0", "/dev/1", "/dev/2"], bricks.value)

    @mock.patch('lib.charm.gluster.block.scan_devices')
    @mock.patch('lib.charm.gluster.block.config')
    @mock.patch('lib.charm.gluster.block.log')
    def testGetManualBricks(self, _log, _config, _scan_devices):
        _config.return_value = "/dev/sda /dev/sdb /dev/sdc"
        _scan_devices.return_value = Ok(["/dev/sda", "/dev/sdb", "/dev/sdc"])
        bricks = block.get_manual_bricks()
        self.assertTrue(bricks.is_ok())
        self.assertListEqual(["/dev/sda", "/dev/sdb", "/dev/sdc"],
                             bricks.value)

    def testSetElevator(self):
        pass

    @mock.patch('lib.charm.gluster.block.is_block_device')
    @mock.patch('lib.charm.gluster.block.device_initialized')
    @mock.patch('lib.charm.gluster.block.log')
    def testScanDevices(self, _log, _is_block_device, _device_initialized):
        expected = [
            block.BrickDevice(is_block_device=True, initialized=True,
                              mount_path="/mnt/sda", dev_path="/dev/sda"),
            block.BrickDevice(is_block_device=True, initialized=True,
                              mount_path="/mnt/sdb", dev_path="/dev/sdb"),
            block.BrickDevice(is_block_device=True, initialized=True,
                              mount_path="/mnt/sdc", dev_path="/dev/sdc")
        ]
        _is_block_device.return_value = Ok(True)
        _device_initialized.return_value = Ok(True)
        result = block.scan_devices(["/dev/sda", "/dev/sdb", "/dev/sdc"])
        self.assertTrue(result.is_ok())
        self.assertListEqual(expected, result.value)

        # @mock.patch('lib.charm.gluster.block.log')
        # def testWeeklyDefrag(self, _log):
        #    block.weekly_defrag(mount="/mnt/sda",
        #                        fs_type=block.FilesystemType.Xfs,
        #                        interval="daily")
        #    pass


if __name__ == "__main__":
    unittest.main()
