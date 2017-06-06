#!/usr/bin/python3
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
import sys

from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import action_get, action_fail, action_set
from lib.gluster.volume import (quota_list,
                                BitrotOption, ScrubAggression, ScrubSchedule,
                                ScrubControl, GlusterOption,
                                volume_add_quota,
                                volume_disable_bitrot, volume_enable_bitrot,
                                volume_enable_quotas, volume_quotas_enabled,
                                volume_rebalance, volume_remove_quota,
                                volume_set_bitrot_option, volume_set_options)


def rebalance_volume():
    """
    Start a rebalance volume operation
    """
    vol = action_get("volume")
    if not vol:
        action_fail("volume not specified")
    output = volume_rebalance(vol)
    if output.is_err():
        action_fail(
            "volume rebalance failed with error: {}".format(output.value))


def enable_bitrot_scan():
    """
    Enable bitrot scan
    """
    vol = action_get("volume")
    if not vol:
        action_fail("volume not specified")
    output = volume_enable_bitrot(vol)
    if output.is_err():
        action_fail("enable bitrot failed with error: {}".format(output.value))


def disable_bitrot_scan():
    """
    Disable bitrot scan
    """
    vol = action_get("volume")
    if not vol:
        action_fail("volume not specified")
    output = volume_disable_bitrot(vol)
    if output.is_err():
        action_fail("enable disable failed with error: {}".format(
            output.value))


def pause_bitrot_scan():
    """
    Pause bitrot scan
    """
    vol = action_get("volume")
    option = BitrotOption.Scrub(ScrubControl.Pause)
    output = volume_set_bitrot_option(vol, option)
    if output.is_err():
        action_fail(
            "pause bitrot scan failed with error: {}".format(output.value))


def resume_bitrot_scan():
    """
    Resume bitrot scan
    """
    vol = action_get("volume")
    option = BitrotOption.Scrub(ScrubControl.Resume)
    output = volume_set_bitrot_option(vol, option)
    if output.is_err():
        action_fail(
            "resume bitrot scan failed with error: {}".format(option.value))


def set_bitrot_scan_frequency():
    """
    Set the bitrot scan frequency
    """
    vol = action_get("volume")
    frequency = action_get("frequency")
    option = ScrubSchedule.from_str(frequency)
    output = volume_set_bitrot_option(vol, BitrotOption.ScrubFrequency(option))
    if output.is_err():
        action_fail("set bitrot scan frequency failed with error: {}".format(
            output.value))


def set_bitrot_throttle():
    """
    Set how aggressive bitrot scanning should be
    """
    vol = action_get("volume")
    throttle = action_get("throttle")
    option = ScrubAggression.from_str(throttle)
    output = volume_set_bitrot_option(vol, BitrotOption.ScrubThrottle(option))
    if output.is_err():
        action_fail(
            "set bitrot throttle failed with error: {}".format(output.value))


def enable_volume_quota():
    """
    Enable quotas on the volume
    """
    # Gather our action parameters
    volume = action_get("volume")
    usage_limit = action_get("usage-limit")
    parsed_usage_limit = int(usage_limit)
    path = action_get("path")
    # Turn quotas on if not already enabled
    quotas_enabled = volume_quotas_enabled(volume)
    if quotas_enabled.is_err():
        action_fail("Enable quota failed: {}".format(quotas_enabled.value))
    if not quotas_enabled.value:
        output = volume_enable_quotas(volume)
        if output.is_err():
            action_fail("Enable quotas failed: {}".format(output.value))

    output = volume_add_quota(volume, path, parsed_usage_limit)
    if output.is_err():
        action_fail("Add quota failed: {}".format(output.value))


def disable_volume_quota():
    """
    Disable quotas on the volume
    """
    volume = action_get("volume")
    path = action_get("path")
    quotas_enabled = volume_quotas_enabled(volume)
    if quotas_enabled.is_err():
        action_fail("Disable quota failed: {}".format(quotas_enabled.value))
    if quotas_enabled.value:
        output = volume_remove_quota(volume, path)
        if output.is_err():
            # Notify the user of the failure and then return the error
            # up the stack
            action_fail(
                "remove quota failed with error: {}".format(output.value))


def list_volume_quotas():
    """
    List quotas on the volume
    """
    volume = action_get("volume")
    quotas_enabled = volume_quotas_enabled(volume)
    if quotas_enabled.is_err():
        action_fail("List quota failed: {}".format(quotas_enabled.value))
    if quotas_enabled.value:
        quotas = quota_list(volume)
        if quotas.is_err():
            action_fail(
                "Failed to get volume quotas: {}".format(quotas.value))
        quota_strings = []
        for quota in quotas.value:
            quota_string = "path:{} limit:{} used:{}".format(
                quota.path,
                quota.hard_limit,
                quota.used)
            quota_strings.append(quota_string)
        action_set({"quotas": "\n".join(quota_strings)})


def set_volume_options():
    """
    Set one or more options on the volume at once
    """
    volume = action_get("volume")

    # Gather all of the action parameters up at once.  We don't know what
    # the user wants to change.
    options = action_get()
    settings = []
    for (key, value) in options:
        if key != "volume":
            settings.append(GlusterOption(key, value))
        else:
            volume = value

    volume_set_options(volume, settings)


# Actions to function mapping, to allow for illegal python action names that
# can map to a python function.
ACTIONS = {
    "create-volume-quota": enable_volume_quota,
    "delete-volume-quota": disable_volume_quota,
    "disable-bitrot-scan": disable_bitrot_scan,
    "enable-bitrot-scan": enable_bitrot_scan,
    "list-volume-quotas": list_volume_quotas,
    "pause-bitrot-scan": pause_bitrot_scan,
    "rebalance-volume": rebalance_volume,
    "resume-bitrot-scan": resume_bitrot_scan,
    "set-bitrot-scan-frequency": set_bitrot_scan_frequency,
    "set-bitrot-throttle": set_bitrot_throttle,
    "set-volume-options": set_volume_options,
}


def main(args):
    action_name = os.path.basename(args[0])
    try:
        action = ACTIONS[action_name]
    except KeyError:
        return "Action %s undefined" % action_name
    else:
        try:
            action(args)
        except Exception as e:
            hookenv.action_fail(str(e))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
