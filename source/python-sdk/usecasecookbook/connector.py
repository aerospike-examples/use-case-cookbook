"""Parses command-line options for connecting to the cluster and turns them into a
connected ``Cluster``. Mirrors ../../java-sdk's SdkConnector onto this SDK's
``ClusterDefinition`` builder API.
"""

import argparse
import getpass

from aerospike_sdk.sync import Cluster, ClusterDefinition, Host


def add_connection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-hosts", "--hosts", default="localhost:3000",
        help="Comma-separated list of seed hosts, format hostname[:port],... Default: localhost:3000",
    )
    parser.add_argument("-U", "--user", help="User name for the cluster")
    parser.add_argument("-P", "--password", help="Password for the cluster")
    parser.add_argument("-cn", "--clusterName", help="Expected cluster name")
    parser.add_argument(
        "-sa", "--useServicesAlternate", action="store_true",
        help="Use services-alternate when connecting to the cluster",
    )


def validate_connection_options(args: argparse.Namespace) -> str:
    """Returns an error message string if the parsed args are invalid, else ""."""
    if args.user and not args.password:
        try:
            args.password = getpass.getpass("Enter password for cluster: ")
        except (EOFError, OSError):
            pass
    if args.user and not args.password:
        return "Password must be specified if a username is passed"
    return ""


def connect(args: argparse.Namespace) -> Cluster:
    hosts = Host.parse_hosts(args.hosts, 3000)
    definition = ClusterDefinition(hosts=hosts)
    if args.user:
        definition = definition.with_native_credentials(args.user, args.password)
    if args.clusterName:
        definition = definition.validate_cluster_name_is(args.clusterName)
    if args.useServicesAlternate:
        definition = definition.using_services_alternate()

    return definition.connect()
