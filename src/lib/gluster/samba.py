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
import io
import os
from io import TextIOBase

from charms.reactive import when_file_changed, when_not, set_state
from charmhelpers.core.hookenv import config, log, status_set
from charmhelpers.core.host import service_start
from charmhelpers.fetch import apt_install


def render_samba_configuration(f: TextIOBase, volume_name: str) -> int:
    """
    Write the samba configuration file out to disk

    :param f: TextIOBase handle to the sambe config file
    :param volume_name: str
    :return: int of bytes written
    """
    bytes_written = 0
    bytes_written += f.write("[{}]\n".format(volume_name))
    bytes_written += f.write(b"path = /mnt/glusterfs\n"
                             b"read only = no\n"
                             b"guest ok = yes\n"
                             b"kernel share modes = no\n"
                             b"kernel oplocks = no\n"
                             b"map archive = no\n"
                             b"map hidden = no\n"
                             b"map read only = no\n"
                             b"map system = no\n"
                             b"store dos attributes = yes\n")
    return bytes_written


@when_file_changed('/etc/samba/smb.conf')
def samba_config_changed() -> bool:
    """
    Checks whether a samba config file has changed or not.
    :param volume_name: str.
    :return: True or False
    """
    volume_name = config("volume_name")
    samba_path = os.path.join(os.sep, 'etc', 'samba', 'smb.conf')
    if os.path.exists(samba_path):
        # Lets check if the smb.conf matches what we're going to write.
        # If so then it was already setup and there's nothing to do
        with open(samba_path) as existing_config:
            old_config = existing_config.readlines()
            new_config = io.StringIO()
            render_samba_configuration(new_config, volume_name)
            if "".join(new_config) == "".join(old_config):
                # configs are identical
                return False
            else:
                return True
    # Config doesn't exist.
    return True


@when_not('samba.installed')
def setup_samba():
    """
    Installs and starts up samba
    :param volume_name: str. Gluster volume to start samba on
    """
    volume_name = config("volume_name")
    cifs_config = config("cifs")
    if cifs_config is None:
        # Samba isn't enabled
        return
    if not samba_config_changed(volume_name):
        # log!("Samba is already setup.  Not reinstalling")
        return
    status_set("Maintenance", "Installing Samba")
    apt_install(["samba"])
    status_set("Maintenance", "Configuring Samba")
    with open(os.path.join(os.sep, 'etc', 'samba', 'smb.conf')) as samba_conf:
        bytes_written = render_samba_configuration(samba_conf, volume_name)
        log("Wrote {} bytes to /etc/samba/smb.conf", bytes_written)
        log("Starting Samba service")
        status_set("Maintenance", "Starting Samba")
        service_start("smbd")
        set_state('samba.installed')
