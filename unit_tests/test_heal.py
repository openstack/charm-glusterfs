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
from unittest.mock import MagicMock

import mock
from lib.gluster import heal


# mock_apt = MagicMock()
# sys.modules['apt'] = mock_apt
# mock_apt.apt_pkg = MagicMock()


class Test(unittest.TestCase):
    @mock.patch('os.listdir')
    def testGetHealCount(self, _listdir):
        _listdir.return_value = ['xattrop_one', 'healme', 'andme']
        brick = MagicMock(path='/export/brick1/')
        count = heal.get_self_heal_count(brick)
        self.assertEqual(2, count, "Expected 2 objects to need healing")


if __name__ == "__main__":
    unittest.main()
