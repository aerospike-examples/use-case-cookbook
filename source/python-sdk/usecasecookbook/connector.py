"""Parses command-line options for connecting to the cluster and turns them into a
connected ``SyncClient``. Mirrors ../../java-sdk's SdkConnector onto this SDK's
``SyncClient``/``ClientPolicy`` API.
"""

import argparse
import getpass

from aerospike_async import AuthMode, ClientPolicy
from aerospike_sdk import SyncClient


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


def connect(args: argparse.Namespace) -> SyncClient:
    policy = ClientPolicy()
    if args.user:
        policy.set_auth_mode(AuthMode.INTERNAL, user=args.user, password=args.password)
    if args.clusterName:
        policy.cluster_name = args.clusterName
    if args.useServicesAlternate:
        policy.use_services_alternate = True

    client = SyncClient(args.hosts, policy)
    client.connect()
    return client
