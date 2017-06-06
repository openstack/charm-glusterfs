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

import sys
import unittest

import mock
from result import Ok

from lib.gluster.volume import Quota
from reactive import actions

mock_apt = mock.MagicMock()
sys.modules['apt'] = mock_apt
mock_apt.apt_pkg = mock.MagicMock()


class Test(unittest.TestCase):
    @mock.patch('reactive.actions.quota_list')
    @mock.patch('reactive.actions.volume_quotas_enabled')
    @mock.patch('reactive.actions.action_get')
    @mock.patch('reactive.actions.action_set')
    def testListVolQuotas(self, _action_set, _action_get,
                          _volume_quotas_enabled, _quota_list):
        _quota_list.return_value = Ok(
            [Quota(path="/test1",
                   used=10,
                   avail=90,
                   hard_limit=90,
                   soft_limit=80,
                   hard_limit_exceeded=False,
                   soft_limit_exceeded=False,
                   soft_limit_percentage="80%")])
        _volume_quotas_enabled.return_value = Ok(True)
        _action_get.return_value = "test"
        actions.list_volume_quotas()
        _action_set.assert_called_with(
            {"quotas": "path:/test1 limit:90 used:10"})

    def testSetVolOptions(self):
        pass


if __name__ == "__main__":
    unittest.main()
