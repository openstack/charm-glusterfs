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
import unittest

from lib.charm.gluster.fstab import FsEntry, FsTab
from mock import patch
from result import Ok


class Test(unittest.TestCase):
    @patch.object(FsTab, 'save_fstab')
    def testAddEntry(self, _save_fstab):
        _save_fstab.return_value = Ok(())
        fstab = FsTab(os.path.join("unit_tests", "fstab"))
        result = fstab.add_entry(FsEntry(
            fs_spec="/dev/test",
            mountpoint="/mnt/test",
            vfs_type="xfs",
            mount_options=["defaults"],
            dump=False,
            fsck_order=2
        ))
        self.assertTrue(result.is_ok())

    def testParser(self):
        expected_results = [
            FsEntry(
                fs_spec="/dev/mapper/xubuntu--vg--ssd-root",
                mountpoint=os.path.join(os.sep),
                vfs_type="ext4",
                mount_options=["noatime", "errors=remount-ro"],
                dump=False,
                fsck_order=1),
            FsEntry(
                fs_spec="UUID=378f3c86-b21a-4172-832d-e2b3d4bc7511",
                mountpoint=os.path.join(os.sep, "boot"),
                vfs_type="ext2",
                mount_options=["defaults"],
                dump=False,
                fsck_order=2),
            FsEntry(
                fs_spec="/dev/mapper/xubuntu--vg--ssd-swap_1",
                mountpoint="none",
                vfs_type="swap",
                mount_options=["sw"],
                dump=False,
                fsck_order=0),
            FsEntry(
                fs_spec="UUID=be8a49b9-91a3-48df-b91b-20a0b409ba0f",
                mountpoint=os.path.join(os.sep, "mnt", "raid"),
                vfs_type="ext4",
                mount_options=["errors=remount-ro", "user"],
                dump=False,
                fsck_order=1)
        ]
        with open('unit_tests/fstab', 'r') as f:
            fstab = FsTab(os.path.join(os.sep, "fake"))
            results = fstab.parse_entries(f)
            for result in results.value:
                self.assertTrue(result in expected_results)


if __name__ == "__main__":
    unittest.main()
