import argparse
from typing import Any, Dict, List, Union, cast

import yaml

from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.launchoptions import (LaunchOptions, Options,
                                          SimpleJobOptions, WatchOptions)
from hpcrocket.ssh.connectiondata import ConnectionData


def parse_cli_args(args: List[str]) -> Options:
    parser = _setup_parser()
    config = parser.parse_args(args)

    option_builders = {
        "launch": _build_launch_options,
        "watch": _build_watch_options
    }

    builder = option_builders.get(config.command, _build_simple_job_options)

    return builder(config)


def _build_launch_options(config: argparse.Namespace) -> Options:
    path = cast(str, config.configfile)
    watch = cast(bool, config.watch)
    yaml_config = _parse_yaml(path)

    return LaunchOptions(
        sbatch=yaml_config["sbatch"],
        watch=watch,
        copy_files=_collect_copy_instructions(yaml_config.get("copy", [])),
        clean_files=yaml_config.get("clean", []),
        collect_files=_collect_copy_instructions(
            yaml_config.get("collect", [])),
        **_connection_dict(yaml_config)  # type: ignore
    )


def _collect_copy_instructions(copy_list: List[Dict[str, str]]) -> List[CopyInstruction]:
    return [CopyInstruction(cp["from"],
                            cp["to"],
                            bool(cp.get("overwrite", False)))
            for cp in copy_list]


def _build_simple_job_options(config: argparse.Namespace) -> Options:
    path = cast(str, config.configfile)
    jobid = cast(str, config.jobid)
    command = cast(str, config.command)
    yaml_config = _parse_yaml(path)

    return SimpleJobOptions(
        jobid=jobid,
        action=SimpleJobOptions.Action[command],
        ** _connection_dict(yaml_config)  # type: ignore
    )


def _build_watch_options(config: argparse.Namespace) -> Options:
    path = cast(str, config.configfile)
    jobid = cast(str, config.jobid)
    yaml_config = _parse_yaml(path)

    return WatchOptions(
        jobid=jobid,
        **_connection_dict(yaml_config)  # type: ignore
    )


def _setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("hpc-rocket")
    subparsers = parser.add_subparsers(dest="command")

    _setup_launch_parser(subparsers)
    _setup_status_parser(subparsers)
    _setup_watch_parser(subparsers)
    _setup_cancel_parser(subparsers)

    return parser


def _setup_launch_parser(subparsers: 'argparse._SubParsersAction[argparse.ArgumentParser]') -> None:
    parser = subparsers.add_parser("launch", help="Launch a remote job")
    parser.add_argument("configfile", type=str)
    parser.add_argument("--watch", default=False,
                        dest="watch", action="store_true")


def _setup_status_parser(subparsers: 'argparse._SubParsersAction[argparse.ArgumentParser]') -> None:
    parser = subparsers.add_parser(
        "status", help="Check on a job's current status")
    parser.add_argument("configfile", type=str,
                        help="A config file containing the connection data")
    parser.add_argument("jobid", type=str,
                        help="The ID of the job to be checked")


def _setup_cancel_parser(subparsers: 'argparse._SubParsersAction[argparse.ArgumentParser]') -> None:
    parser = subparsers.add_parser(
        "cancel", help="Cancel a job")
    parser.add_argument("configfile", type=str,
                        help="A config file containing the connection data")
    parser.add_argument("jobid", type=str,
                        help="The ID of the job to be canceled")


def _setup_watch_parser(subparsers: 'argparse._SubParsersAction[argparse.ArgumentParser]') -> None:
    parser = subparsers.add_parser(
        "watch", help="Monitor a job until it completes")
    parser.add_argument("configfile", type=str,
                        help="A config file containing the connection data")
    parser.add_argument("jobid", type=str,
                        help="The ID of the job to be monitored")


def _parse_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r") as file:
        return yaml.load(file, Loader=yaml.SafeLoader)  # type: ignore


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
