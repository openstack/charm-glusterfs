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
from charmhelpers.core.hookenv import ERROR, log, relation_set, unit_public_ip

from lib.gluster.volume import volume_list


def fuse_relation_joined():
    # Fuse clients only need one ip address and they can discover the rest
    """

    """
    public_addr = unit_public_ip()
    volumes = volume_list()
    if volumes.is_err():
        log("volume list is empty.  Unable to complete fuse relation", ERROR)
        return
    data = {"gluster-public-address": public_addr,
            "volumes": " ".join(volumes.value)}
    relation_set(relation_settings=data)
