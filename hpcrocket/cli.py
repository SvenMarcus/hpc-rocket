import argparse
from typing import Dict, List
from hpcrocket.ssh.sshexecutor import ConnectionData

import yaml

from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.launchoptions import LaunchOptions


def parse_cli_args(args) -> LaunchOptions:
    parser = _setup_parser()

    config = parser.parse_args(args)
    if "configfile" in config:
        return _parse_yaml_configuration(config.configfile)

    return _parse_cli_configuration(config)


def _setup_parser():
    parser = argparse.ArgumentParser("hpclaunch")
    subparsers = parser.add_subparsers()

    _setup_yaml_parser(subparsers)
    _setup_cli_parser(subparsers)

    return parser


def _setup_cli_parser(subparsers):
    run_parser = subparsers.add_parser("run", help="Configure HPC Rocket from the command line")
    run_parser.add_argument("jobfile", type=str, help="The name of the job file to run with sbatch")
    run_parser.add_argument("--host", type=str, required=True, help="Address of the remote machine")
    run_parser.add_argument("--user", type=str, required=True, help="User on the remote machine")
    run_parser.add_argument("--password", type=str, help="The password for the given user")
    run_parser.add_argument("--private-key", type=str, help="A private SSH key")
    run_parser.add_argument("--keyfile", type=str, help="The path to a file containing a private SSH key")


def _setup_yaml_parser(subparsers):
    yaml_parser = subparsers.add_parser("launch", help="Configure HPC Rocket from a configuration file")
    yaml_parser.add_argument("configfile", type=str)


def _parse_cli_configuration(config):
    return LaunchOptions(
        config.jobfile,
        connection=ConnectionData(
            hostname=config.host,
            username=config.user,
            password=config.password,
            key=config.private_key,
            keyfile=config.keyfile
        ))


def _parse_yaml_configuration(path: str) -> LaunchOptions:
    with open(path, "r") as file:
        config = yaml.load(file, Loader=yaml.SafeLoader)

        return LaunchOptions(
            sbatch=config["sbatch"],
            copy_files=_collect_copy_instructions(config.get("copy", [])),
            clean_files=config.get("clean", []),
            collect_files=_collect_copy_instructions(config.get("collect", [])),
            connection=_connection_data_from_dict(config),
            proxyjumps=_collect_proxyjumps(config.get("proxyjumps", []))
        )


def _connection_data_from_dict(config):
    return ConnectionData(
        hostname=config["host"],
        username=config["user"],
        keyfile=config.get("private_keyfile"),
        password=str(config.get("password"))
    )


def _collect_proxyjumps(proxyjumps: List[Dict[str, str]]):
    return [_connection_data_from_dict(proxy) for proxy in proxyjumps]


def _collect_copy_instructions(copy_list):
    return [CopyInstruction(cp["from"],
                            cp["to"],
                            cp.get("overwrite", False))
            for cp in copy_list]
