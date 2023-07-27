import argparse
import functools
import os
from typing import Any, Dict, List, Optional, Protocol, Tuple, Union, cast

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

from ._yaml import ParseError, parse_yaml


class OptionBuilder(Protocol):
    def __call__(
        self, config: argparse.Namespace, yaml_config: Dict[str, Any]
    ) -> Options:
        ...


def create_options(
    config: argparse.Namespace, filesystem: Filesystem
) -> Union[Options, ParseError]:
    yaml_or_error = parse_yaml(getattr(config, "configfile", "rocket.yml"), filesystem)
    if isinstance(yaml_or_error, ParseError):
        return yaml_or_error

    watch_builder = functools.partial(build_watch_options, filesystem=filesystem)
    simple_builder = functools.partial(build_simple_job_options, filesystem=filesystem)

    option_builders: Dict[str, OptionBuilder] = {
        "launch": build_launch_options,
        "finalize": build_finalize_options,
        "watch": watch_builder,
    }

    builder = option_builders.get(config.command, simple_builder)

    yaml_config = yaml_or_error
    return builder(config, yaml_config)


def build_launch_options(
    config: argparse.Namespace, yaml_config: Dict[str, Any]
) -> Options:
    watch = cast(bool, config.watch)

    sbatch, sbatch_copy_instruction = parse_sbatch(yaml_config)
    files_to_copy = copy_instructions(yaml_config.get("copy", []))
    if sbatch_copy_instruction:
        files_to_copy.append(sbatch_copy_instruction)

    return LaunchOptions(
        sbatch=os.path.expandvars(sbatch),
        watch=watch,
        copy_files=files_to_copy,
        clean_files=clean_instructions(yaml_config.get("clean", [])),
        collect_files=copy_instructions(yaml_config.get("collect", [])),
        continue_if_job_fails=yaml_config.get("continue_if_job_fails", False),
        job_id_file=config.jobid_file,
        **connection_dict(yaml_config),  # type: ignore
    )


def parse_sbatch(yaml_config: Dict[str, Any]) -> Tuple[str, Optional[CopyInstruction]]:
    sbatch: Union[str, Dict[str, str]] = yaml_config["sbatch"]
    if isinstance(sbatch, str):
        return sbatch, None

    copy = copy_instruction_from_dict(sbatch, dest_keyname="script")
    script = copy.destination

    return script, copy


def build_simple_job_options(
    config: argparse.Namespace, yaml_config: Dict[str, Any], filesystem: Filesystem
) -> Options:
    jobid = cast(str, config.jobid) or read_jobid_from_file(config, filesystem)
    command = cast(str, config.command)

    return ImmediateCommandOptions(
        jobid=jobid,
        action=ImmediateCommandOptions.Action[command],
        **connection_dict(yaml_config),  # type: ignore
    )


def build_watch_options(
    config: argparse.Namespace, yaml_config: Dict[str, Any], filesystem: Filesystem
) -> Options:
    jobid = cast(str, config.jobid) or read_jobid_from_file(config, filesystem)
    return WatchOptions(jobid=jobid, **connection_dict(yaml_config))  # type: ignore


def build_finalize_options(
    config: argparse.Namespace, yaml_config: Dict[str, Any]
) -> Options:
    return FinalizeOptions(
        clean_files=clean_instructions(yaml_config.get("clean", [])),
        collect_files=copy_instructions(yaml_config.get("collect", [])),
        **connection_dict(yaml_config),  # type: ignore
    )


def copy_instructions(copy_list: List[Dict[str, str]]) -> List[CopyInstruction]:
    return [copy_instruction_from_dict(cp) for cp in copy_list]


def copy_instruction_from_dict(
    cp: Dict[str, Any], dest_keyname: str = "to"
) -> CopyInstruction:
    return CopyInstruction(
        os.path.expandvars(cp["from"]),
        os.path.expandvars(cp[dest_keyname]),
        bool(cp.get("overwrite", False)),
    )


def clean_instructions(clean_instructions: List[str]) -> List[str]:
    return [os.path.expandvars(ci) for ci in clean_instructions]


def read_jobid_from_file(config: argparse.Namespace, filesystem: Filesystem) -> str:
    with filesystem.openread(config.read_jobid_from) as file:
        return file.read()


def connection_dict(
    config: Dict[str, Any]
) -> Dict[str, Union[ConnectionData, List[ConnectionData]]]:
    return {
        "connection": connection_data_from_dict(config),
        "proxyjumps": proxyjumps(config.get("proxyjumps", [])),
    }


def connection_data_from_dict(config: Dict[str, str]) -> ConnectionData:
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


def proxyjumps(proxyjumps: List[Dict[str, str]]) -> List[ConnectionData]:
    return [connection_data_from_dict(proxy) for proxy in proxyjumps]
