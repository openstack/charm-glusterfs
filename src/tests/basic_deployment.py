#!/usr/bin/env python
#
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

import amulet
from charmhelpers.contrib.openstack.amulet.deployment import (
    OpenStackAmuletDeployment
)
from charmhelpers.contrib.openstack.amulet.utils import (
    OpenStackAmuletUtils,
    DEBUG,
)

# Use DEBUG to turn on debug logging
u = OpenStackAmuletUtils(DEBUG)


class GlusterFSBasicDeployment(OpenStackAmuletDeployment):
    """Amulet tests on a basic glusterfs deployment."""

    def __init__(self, series, openstack=None, source=None, stable=False):
        """Deploy the entire test environment."""
        super(GlusterFSBasicDeployment, self).__init__(series, openstack,
                                                       source, stable)
        self._add_services()
        self._configure_services()
        self._deploy()

        u.log.info('Waiting on extended status checks...')
        self._auto_wait_for_status(exclude_services=[])

        self.d.sentry.wait()
        self._initialize_tests()

    def _add_services(self):
        """Add services

           Add the services that we're testing, where glusterfs is local,
           and the rest of the service are from lp branches that are
           compatible with the local charm (e.g. stable or next).
           """
        super(GlusterFSBasicDeployment, self)._add_services(
            this_service={'name': 'glusterfs', 'units': 3},
            no_origin=['glusterfs'], other_services=[])

    def _configure_services(self):
        """Configure all of the services."""
        configs = {
            'glusterfs': {
                'volume_name': 'test',
                'brick_devices': '/dev/vdb',
                'ephemeral_unmount': '/mnt',
            },
        }
        super(GlusterFSBasicDeployment, self)._configure_services(configs)

    def _initialize_tests(self):
        """Perform final initialization before tests get run."""
        # Access the sentries for inspecting service units
        self.gluster0_sentry = self.d.sentry['glusterfs'][0]
        self.gluster1_sentry = self.d.sentry['glusterfs'][1]
        self.gluster2_sentry = self.d.sentry['glusterfs'][2]

        u.log.debug('openstack release val: {}'.format(
            self._get_openstack_release()))
        u.log.debug('openstack release str: {}'.format(
            self._get_openstack_release_string()))

    def test_100_gluster_processes(self):
        """Verify that the expected service processes are running
        on each gluster unit."""

        # Process name and quantity of processes to expect on each unit
        gluster_processes = {
            'glusterd': 1,
            'glusterfsd': 1,
        }

        # Units with process names and PID quantities expected
        expected_processes = {
            self.gluster0_sentry: gluster_processes,
            self.gluster1_sentry: gluster_processes,
            self.gluster2_sentry: gluster_processes
        }

        actual_pids = u.get_unit_process_ids(expected_processes)
        ret = u.validate_unit_process_ids(expected_processes, actual_pids)
        if ret:
            amulet.raise_status(amulet.FAIL, msg=ret)

    def test_102_services(self):
        """Verify the expected services are running on the corresponding
           service units."""
        u.log.debug('Checking system services on units...')

        glusterfs_svcs = ['glusterfs-server']

        service_names = {
            self.gluster0_sentry: glusterfs_svcs,
        }

        ret = u.validate_services_by_name(service_names)
        if ret:
            amulet.raise_status(amulet.FAIL, msg=ret)

        u.log.debug('OK')

    def test_400_gluster_cmds_exit_zero(self):
        """Check basic functionality of gluster cli commands against
        one gluster unit."""
        sentry_units = [
            self.gluster0_sentry,
        ]
        commands = [
            'sudo gluster vol status test',
            'sudo gluster vol info test',
        ]
        ret = u.check_commands_on_units(commands, sentry_units)
        if ret:
            amulet.raise_status(amulet.FAIL, msg=ret)
