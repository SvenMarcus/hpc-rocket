import argparse
import os.path
from typing import Any, Dict, List, Optional, Union, cast

import yaml

from hpcrocket.core.filesystem import Filesystem
from hpcrocket.core.filesystem.progressive import CopyInstruction
from hpcrocket.core.launchoptions import (
    FinalizeOptions,
    ImmediateCommandOptions,
    LaunchOptions,
    Options,
    WatchOptions,
)
from hpcrocket.ssh.connectiondata import ConnectionData


class ParseError(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__()
        self._reason = reason

    def __str__(self) -> str:
        return self._reason


def parse_cli_args(
    args: List[str], filesystem: Filesystem
) -> Union[Options, ParseError]:
    parser = _setup_parser()
    config = parser.parse_args(args)
    return _create_options(config, filesystem)


def _create_options(
    config: argparse.Namespace, filesystem: Filesystem
) -> Union[Options, ParseError]:
    yaml_or_error = _parse_yaml(getattr(config, "configfile", "rocket.yml"), filesystem)
    if isinstance(yaml_or_error, ParseError):
        return yaml_or_error

    option_builders = {
        "launch": _build_launch_options,
        "finalize": _build_finalize_options,
        "watch": _build_watch_options,
    }

    builder = option_builders.get(config.command, _build_simple_job_options)

    yaml_config = yaml_or_error
    return builder(config, yaml_config)


def _build_launch_options(
    config: argparse.Namespace, yaml_config: Dict[str, Any]
) -> Options:
    watch = cast(bool, config.watch)

    sbatch = cast(str, yaml_config["sbatch"])
    return LaunchOptions(
        sbatch=os.path.expandvars(sbatch),
        watch=watch,
        copy_files=_collect_copy_instructions(yaml_config.get("copy", [])),
        clean_files=_clean_instructions(yaml_config.get("clean", [])),
        collect_files=_collect_copy_instructions(yaml_config.get("collect", [])),
        continue_if_job_fails=yaml_config.get("continue_if_job_fails", False),
        **_connection_dict(yaml_config),  # type: ignore
    )


def _build_finalize_options(
    config: argparse.Namespace, yaml_config: Dict[str, Any]
) -> Options:
    return FinalizeOptions(
        clean_files=_clean_instructions(yaml_config.get("clean", [])),
        collect_files=_collect_copy_instructions(yaml_config.get("collect", [])),
        **_connection_dict(yaml_config),  # type: ignore
    )


def _collect_copy_instructions(
    copy_list: List[Dict[str, str]]
) -> List[CopyInstruction]:
    return [
        CopyInstruction(
            os.path.expandvars(cp["from"]),
            os.path.expandvars(cp["to"]),
            bool(cp.get("overwrite", False)),
        )
        for cp in copy_list
    ]


def _clean_instructions(clean_instructions: List[str]) -> List[str]:
    return [os.path.expandvars(ci) for ci in clean_instructions]


def _build_simple_job_options(
    config: argparse.Namespace, yaml_config: Dict[str, Any]
) -> Options:
    jobid = cast(str, config.jobid)
    command = cast(str, config.command)

    return ImmediateCommandOptions(
        jobid=jobid,
        action=ImmediateCommandOptions.Action[command],
        **_connection_dict(yaml_config),  # type: ignore
    )


def _build_watch_options(
    config: argparse.Namespace, yaml_config: Dict[str, Any]
) -> Options:
    jobid = cast(str, config.jobid)
    return WatchOptions(jobid=jobid, **_connection_dict(yaml_config))  # type: ignore


def _setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("hpc-rocket")
    subparsers = parser.add_subparsers(dest="command")

    _setup_launch_parser(subparsers)
    _setup_finalize_parser(subparsers)
    _setup_status_parser(subparsers)
    _setup_watch_parser(subparsers)
    _setup_cancel_parser(subparsers)

    return parser


def _setup_launch_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    parser = subparsers.add_parser("launch", help="Launch a remote job")
    _add_configfile_arg(parser)
    parser.add_argument("--watch", default=False, dest="watch", action="store_true")


def _setup_finalize_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    parser = subparsers.add_parser(
        "finalize", help="Run collect and clean instructions"
    )
    _add_configfile_arg(parser)


def _setup_status_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    parser = subparsers.add_parser("status", help="Check on a job's current status")
    _add_configfile_arg(parser)
    parser.add_argument("jobid", type=str, help="The ID of the job to be checked")


def _setup_cancel_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    parser = subparsers.add_parser("cancel", help="Cancel a job")
    _add_configfile_arg(parser)
    parser.add_argument("jobid", type=str, help="The ID of the job to be canceled")


def _setup_watch_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    parser = subparsers.add_parser("watch", help="Monitor a job until it completes")
    _add_configfile_arg(parser)
    parser.add_argument("jobid", type=str, help="The ID of the job to be monitored")


def _add_configfile_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "configfile",
        type=str,
        default="rocket.yml",
        help="A config file containing the connection data. Defaults to rocket.yml",
    )


def _parse_yaml(path: str, filesystem: Filesystem) -> Union[Dict[str, Any], ParseError]:
    try:
        with filesystem.openread(path) as file:
            return yaml.load(file, Loader=yaml.SafeLoader)  # type: ignore
    except FileNotFoundError:
        return ParseError(f"File {path} does not exist!")


def _connection_dict(
    config: Dict[str, Any]
) -> Dict[str, Union[ConnectionData, List[ConnectionData]]]:
    return {
        "connection": _connection_data_from_dict(config),
        "proxyjumps": _collect_proxyjumps(config.get("proxyjumps", [])),
    }


def _connection_data_from_dict(config: Dict[str, str]) -> ConnectionData:
    return ConnectionData(
        hostname=cast(str, expand_or_none(config["host"])),
        username=cast(str, expand_or_none(config["user"])),
        keyfile=expand_or_none(config.get("private_keyfile")),
        password=expand_or_none(str(config.get("password"))),
    )


def expand_or_none(config_entry: Optional[str]) -> Optional[str]:
    if not config_entry:
        return None

    return os.path.expandvars(config_entry)


def _collect_proxyjumps(proxyjumps: List[Dict[str, str]]) -> List[ConnectionData]:
    return [_connection_data_from_dict(proxy) for proxy in proxyjumps]
