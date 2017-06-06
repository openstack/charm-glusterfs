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
import apt
from result import Err, Ok, Result


def get_candidate_package_version(package_name: str) -> Result:
    """
    Ask apt-cache for the new candidate package that is available
    :param package_name: The package to check for an upgrade
    :return: Ok with the new candidate version or Err in case the candidate
        was not found
    """
    cache = apt.Cache()
    try:
        version = cache[package_name].candidate.version
        return Ok(version)
    except KeyError:
        return Err("Unable to find candidate upgrade package for: {}".format(
            package_name))
