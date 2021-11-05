import argparse
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union

import yaml

from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.launchoptions import Options, JobBasedOptions, LaunchOptions
from hpcrocket.ssh.connectiondata import ConnectionData


def parse_cli_args(args: List[str]) -> Options:
    parser = _setup_parser()
    config = parser.parse_args(args)

    options_builder: _OptionBuilder
    if config.command == "launch":
        options_builder = _LaunchConfigurationBuilder(config.configfile, config.watch)
    elif config.command == "status":
        options_builder = _StatusConfigurationBuilder(config.configfile, config.jobid)

    return options_builder.build()


def _setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("hpc-rocket")
    subparsers = parser.add_subparsers(dest="command")

    _setup_launch_parser(subparsers)
    _setup_status_parser(subparsers)

    return parser


def _setup_launch_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("launch", help="Launch a remote job")
    parser.add_argument("configfile", type=str)
    parser.add_argument("--watch", default=False, dest="watch", action="store_true")


def _setup_status_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("status", help="Check on a job's current status")
    parser.add_argument("configfile", type=str, help="A config file containing the connection data")
    parser.add_argument("jobid", type=str, help="The ID of the job you want to check")


class _OptionBuilder(ABC):

    @abstractmethod
    def build(self) -> Options:
        pass


class _LaunchConfigurationBuilder(_OptionBuilder):

    def __init__(self, path: str, watch: bool) -> None:
        self._path = path
        self._watch = watch

    def build(self) -> Options:
        config = _parse_yaml(self._path)

        return LaunchOptions(
            sbatch=config["sbatch"],
            watch=self._watch,
            copy_files=self._collect_copy_instructions(config.get("copy", [])),
            clean_files=config.get("clean", []),
            collect_files=self._collect_copy_instructions(config.get("collect", [])),
            **_connection_dict(config)  # type: ignore
        )

    @staticmethod
    def _collect_copy_instructions(copy_list: List[Dict[str, str]]) -> List[CopyInstruction]:
        return [CopyInstruction(cp["from"],
                                cp["to"],
                                bool(cp.get("overwrite", False)))
                for cp in copy_list]


class _StatusConfigurationBuilder(_OptionBuilder):

    def __init__(self, path: str, jobid: str) -> None:
        self._path = path
        self._jobid = jobid

    def build(self) -> Options:
        config = _parse_yaml(self._path)
        return JobBasedOptions(
            self._jobid,
            action=JobBasedOptions.Action.status,
            **_connection_dict(config)  # type: ignore
        )


def _parse_yaml(path):
    with open(path, "r") as file:
        return yaml.load(file, Loader=yaml.SafeLoader)


def _connection_dict(config: Dict[str, Any]) -> Dict[str, Union[ConnectionData, List[ConnectionData]]]:
    return {
        "connection": _connection_data_from_dict(config),
        "proxyjumps": _collect_proxyjumps(config.get("proxyjumps", []))
    }


def _connection_data_from_dict(config: Dict[str, str]) -> ConnectionData:
    return ConnectionData(
        hostname=config["host"],
        username=config["user"],
        keyfile=config.get("private_keyfile"),
        password=str(config.get("password"))
    )


def _collect_proxyjumps(proxyjumps: List[Dict[str, str]]) -> List[ConnectionData]:
    return [_connection_data_from_dict(proxy) for proxy in proxyjumps]
