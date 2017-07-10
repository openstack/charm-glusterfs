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
from .volume import Brick


def get_self_heal_count(brick: Brick) -> int:
    """
    Find the self heal count for a given brick.

    :param brick: the brick to probe for the self heal count.
    :return int: the number of files that need healing
    """
    brick_path = "{}/.glusterfs/indices/xattrop".format(brick.path)

    # The gfids which need healing are those files which do not start
    # with 'xattrop'.
    count = 0
    for f in os.listdir(brick_path):
        if not f.startswith('xattrop'):
            count += 1

    return count
