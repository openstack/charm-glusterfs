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
from charmhelpers.core.hookenv import add_metric
import os.path


def collect_metrics():
    """
    Gather metrics about gluster mount and log them to juju metrics
    :rtype: object
    """
    p = os.path.join(os.sep, "mnt", "glusterfs")
    mount_stats = os.statvfs(p)
    # block size * total blocks
    total_space = mount_stats.f_blocks * mount_stats.f_bsize
    free_space = mount_stats.f_bfree * mount_stats.f_bsize
    # capsize only operates on i64 values
    used_space = total_space - free_space
    gb_used = used_space / 1024 / 1024 / 1024

    # log!(format!("Collecting metric gb-used {}", gb_used), Info)
    add_metric("gb-used", "{}".format(gb_used))
