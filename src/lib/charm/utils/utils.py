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

from ipaddress import ip_address
import xml.etree.ElementTree as etree

from charmhelpers.contrib.openstack.utils import get_host_ip
from result import Err, Ok, Result

__author__ = 'Chris Holcombe <chris.holcombe@canonical.com>'


def check_return_code(tree: etree.Element) -> Result:
    """
    Helper function to make processing xml easier.  This checks
    to see if gluster returned an error code
    :param tree: xml tree
    :return: Result with Ok or Err
    """
    return_code = 0
    err_string = ""
    for child in tree:
        if child.tag == 'opRet':
            return_code = int(child.text)
        elif child.tag == 'opErrstr':
            err_string = child.text

    if return_code != 0:
        return Err(err_string)
    return Ok()


def resolve_to_ip(address: str) -> Result:
    """
    Resolves an dns address to an ip address.  Relies on dig
    :param address: String.  Hostname to resolve to an ip address
    :return: result
    """
    ip_addr = get_host_ip(hostname=address)
    try:
        parsed = ip_address(address=ip_addr)
        return Ok(parsed)
    except ValueError:
        return Err("failed to parse ip address: {}".format(ip_addr))
