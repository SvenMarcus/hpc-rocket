import argparse
from typing import Dict, List, Protocol

import yaml

from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.ssh.connectiondata import ConnectionData


def parse_cli_args(args) -> LaunchOptions:
    parser = _setup_parser()
    config = parser.parse_args(args)

    options_parser: Configuration = (YamlConfiguration(config.configfile)
                                     if "configfile" in config
                                     else CliConfiguration(config))

    return options_parser.parse()


def _setup_parser():
    parser = argparse.ArgumentParser("hpc-rocket")
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


class Configuration(Protocol):

    def parse(self) -> LaunchOptions:
        pass


class CliConfiguration(Configuration):

    def __init__(self, namespace: argparse.Namespace) -> None:
        self._namespace = namespace

    def parse(self) -> LaunchOptions:
        return LaunchOptions(
            self._namespace.jobfile,
            connection=ConnectionData(
                hostname=self._namespace.host,
                username=self._namespace.user,
                password=self._namespace.password,
                key=self._namespace.private_key,
                keyfile=self._namespace.keyfile
            ))


class YamlConfiguration(Configuration):

    def __init__(self, path: str) -> None:
        self._path = path

    def parse(self) -> LaunchOptions:
        with open(self._path, "r") as file:
            config = yaml.load(file, Loader=yaml.SafeLoader)

            return LaunchOptions(
                sbatch=config["sbatch"],
                copy_files=self._collect_copy_instructions(config.get("copy", [])),
                clean_files=config.get("clean", []),
                collect_files=self._collect_copy_instructions(config.get("collect", [])),
                connection=self._connection_data_from_dict(config),
                proxyjumps=self._collect_proxyjumps(config.get("proxyjumps", []))
            )

    def _connection_data_from_dict(self, config):
        return ConnectionData(
            hostname=config["host"],
            username=config["user"],
            keyfile=config.get("private_keyfile"),
            password=str(config.get("password"))
        )

    def _collect_proxyjumps(self, proxyjumps: List[Dict[str, str]]):
        return [self._connection_data_from_dict(proxy) for proxy in proxyjumps]

    def _collect_copy_instructions(self, copy_list):
        return [CopyInstruction(cp["from"],
                                cp["to"],
                                cp.get("overwrite", False))
                for cp in copy_list]
