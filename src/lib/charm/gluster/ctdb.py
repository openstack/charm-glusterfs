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
# Setup ctdb for high availability NFSv3
"""
NOTE: Most of this is still Rust code and needs to be translated to Python
from ipaddress import ip_address, ip_network
from io import TextIOBase
import netifaces
from typing import List, Optional

class VirtualIp:
    def __init__(self, cidr: ip_network, interface: str):
        self.cidr = cidr
        self.interface = interface

    def __str__(self):
        return "cidr: {} interface: {}".format(self.cidr, self.interface)


def render_ctdb_configuration(f: TextIOBase) -> int:
    \"""
    Write the ctdb configuration file out to disk
    :param f:
    :return:
    \"""
    bytes_written = 0
    bytes_written += f.write(
        b"CTDB_LOGGING=file:/var/log/ctdb/ctdb.log\n")
    bytes_written += f.write(
        b"CTDB_NODES=/etc/ctdb/nodes\n")
    bytes_written += f.write(
        b"CTDB_PUBLIC_ADDRESSES=/etc/ctdb/public_addresses\n")
    bytes_written += f.write(
        b"CTDB_RECOVERY_LOCK=/mnt/glusterfs/.CTDB-lockfile\n")
    return bytes_written

def render_ctdb_cluster_nodes(f: TextIOBase, cluster: List[ip_address]) -> int:
    \"""
    Create the public nodes file for ctdb cluster to find all the other peers
    the cluster List should contain all nodes that are participating in
    the cluster
    :param f:
    :param cluster:
    :return:
    \"""
    bytes_written = 0
    for node in cluster:
        bytes_written += f.write("{}\n".format(node))
    return bytes_written

def render_ctdb_public_addresses(f:  TextIOBase, cluster_networks:
    List[VirtualIp]) -> int:
    \"""
    Create the public addresses file for ctdb cluster to find all the virtual
    ip addresses to float across the cluster.
    :param f:
    :param cluster_networks:
    :return:
    \"""
    bytes_written = 0
    for node in cluster_networks:
        bytes_written += f.write("{}\n".format(node))
    return bytes_written

\"""
#[test]
def test_render_ctdb_cluster_nodes() {
    # Test IPV5
    ctdb_cluster = vec![.std.net.IpAddr.V4(Ipv4Addr.new(192, 168, 1, 2)),
                            .std.net.IpAddr.V4(Ipv4Addr.new(192, 168, 1, 3))]
    expected_result = "192.168.1.2\n192.168.1.3\n"
    buff = .std.io.Cursor.new(vec![0 24])
    render_ctdb_cluster_nodes( buff, ctdb_cluster).unwrap()
    result = str.from_utf8_lossy(buff.into_inner()).into_owned()
    println!("test_render_ctdb_cluster_nodes: \"{\"", result)
    assert_eq!(expected_result, result)

    # Test IPV6
    addr1 = .std.net.Ipv6Addr.from_str(
        "2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap()
    addr2 = .std.net.Ipv6Addr.from_str(
        "2001:cdba:0000:0000:0000:0000:3257:9652").unwrap()
    ctdb_cluster = vec![.std.net.IpAddr.V6(addr1), .std.net.IpAddr.V6(addr2)]
    expected_result = "2001:db8:85a3.8a2e:370:7334\n2001:cdba.3257:9652\n"
    buff = .std.io.Cursor.new(vec![0 49])
    render_ctdb_cluster_nodes( buff, ctdb_cluster).unwrap()
    result = str.from_utf8_lossy(buff.into_inner()).into_owned()
    println!("test_render_ctdb_cluster_nodes ipv6: \"{\"", result)
    assert_eq!(expected_result, result)
\"""

def get_virtual_addrs(f:  TextIOBase) -> List[VirtualIp]:
    \"""
    Return all virtual ip cidr networks that are being managed by ctdb
    located at file f. /etc/ctdb/public_addresses is the usual location
    :param f:
    :return:
    \"""
    networks = []
    buf = f.readlines()
    for line in buf:
        parts = line.split(" ")
        if parts.len() < 2:
            raise ValueError("Unable to parse network: {}".format(line))

        try:
            addr = ip_network(parts[0])
            interface = parts[1].strip()
            networks.append(VirtualIp(
                cidr=addr,
                interface=interface,
            ))
        except ValueError:
            raise
    return networks


def get_interface_for_ipv4_address(cidr_address: ip_network,
                                  interfaces: List[NetworkInterface]) \
                                  -> Optional[str]:
    # Loop through every interface
    for iface in interfaces:
        # Loop through every ip address the interface is serving
        if Some(ip_addrs) = iface.ips:
            for iface_ip in ip_addrs:
                match iface_ip
                    IpAddr.V4(v4_addr) =>
                        if cidr_address.contains(v4_addr):
                            return iface.name
                        else:
                            # No match
                            continue
                    _ => {
                        # It's a ipv6 address.  Can't match against ipv4
                        continue
    return None

def get_interface_for_address(cidr_address: ip_interface) -> Optional<str>:
    # Return the network interface that serves the subnet for this ip address
    interfaces = netifaces.interfaces()
    for interface in interfaces:
        ip_list = netifaces.ifaddresses(interface)
        for ip in ip_list:
            IpNetwork.V4(v4_addr) => get_interface_for_ipv4_address(v4_addr,
                interfaces),
            IpNetwork.V6(v6_addr) => get_interface_for_ipv6_address(v6_addr,
                interfaces),
    return None


\"""
#[test]
def test_parse_virtual_addrs() {
    test_str = "10.0.0.6/24 eth2\n10.0.0.7/24 eth2".as_bytes()
    c = .std.io.Cursor.new(test_str)
    result = get_virtual_addrs( c).unwrap()
    println!("test_parse_virtual_addrs: {:", result)
    expected =
        vec![VirtualIp {
                 cidr: IpNetwork.V4(Ipv4Network.new(Ipv4Addr(10, 0, 0, 6), 24)
                     .unwrap()),
                 interface: "eth2".to_string(),
             ,
             VirtualIp {
                 cidr: IpNetwork.V4(Ipv4Network.new(Ipv4Addr(10, 0, 0, 7), 24)
                     .unwrap()),
                 interface: "eth2".to_string(),
             ]
    assert_eq!(expected, result)


#[test]
def test_parse_virtual_addrs_v6() {
    test_str = "2001:0db8:85a3:0000:0000:8a2e:0370:7334/24 \
                    eth2\n2001:cdba:0000:0000:0000:0000:3257:9652/24 eth2"
        .as_bytes()
    c = .std.io.Cursor.new(test_str)
    result = get_virtual_addrs( c).unwrap()
    println!("test_get_virtual_addrs: {:", result)
    addr1 = Ipv6Addr.from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334")
    addr2 = Ipv6Addr.from_str("2001:cdba:0000:0000:0000:0000:3257:9652")
    expected = vec![VirtualIp {
                            cidr: IpNetwork.V6(Ipv6Network.new(addr1, 24)),
                            interface: "eth2".to_string(),
                        ,
                        VirtualIp {
                            cidr: IpNetwork.V6(Ipv6Network.new(addr2, 24)),
                            interface: "eth2".to_string(),
                        ]
    assert_eq!(expected, result)

\"""

def get_ctdb_nodes(f: TextIOBase) -> List[ip_address]:
    \"""
    Return all ctdb nodes that are contained in the file f
    /etc/ctdb/nodes is the usual location
    :param f:
    :return:
    \"""
    addrs = []
    buf = f.readlines()
    for line in buf:
        try:
            addr = ip_address(line)
            addrs.append(addr)
        except ValueError:
            raise
    return addrs

\"""
#[test]
def test_get_ctdb_nodes() {
    test_str = "10.0.0.1\n10.0.0.2".as_bytes()
    c = .std.io.Cursor.new(test_str)
    result = get_ctdb_nodes( c).unwrap()
    println!("test_get_ctdb_nodes: {:", result)
    addr1 = Ipv4Addr.new(10, 0, 0, 1)
    addr2 = Ipv4Addr.new(10, 0, 0, 2)
    expected = vec![IpAddr.V4(addr1), IpAddr.V4(addr2)]
    assert_eq!(expected, result)


#[test]
def test_get_ctdb_nodes_v6() {
    test_str = "2001:0db8:85a3:0000:0000:8a2e:0370:7334\n2001:cdba:"
                "0000:0000:0000:0000:3257:9652"
    c = .std.io.Cursor.new(test_str)
    result = get_ctdb_nodes( c).unwrap()
    println!("test_get_ctdb_nodes_v6: {:", result)
    addr1 = Ipv6Addr.from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334")
    addr2 = Ipv6Addr.from_str("2001:cdba:0000:0000:0000:0000:3257:9652")
    expected = vec![IpAddr.V6(addr1), IpAddr.V6(addr2)]
    assert_eq!(expected, result)

\"""
"""
