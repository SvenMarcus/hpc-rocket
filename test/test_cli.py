import os
from pathlib import Path
from typing import Dict, Generator, Iterator, List, Union, cast
from unittest.mock import patch

import pytest

from hpcrocket.cli import ParseError, parse_cli_args
from hpcrocket.core.filesystem.progressive import CopyInstruction
from hpcrocket.core.launchoptions import (
    FinalizeOptions,
    ImmediateCommandOptions,
    LaunchOptions,
    Options,
    WatchOptions,
)
from hpcrocket.pyfilesystem.localfilesystem import localfilesystem
from hpcrocket.ssh.connectiondata import ConnectionData

HOME = "/home/user"
REMOTE_USER = "the_user"
REMOTE_HOST = "cluster.example.com"
PROXY1_KEYFILE = "/home/user/.ssh/proxy1_keyfile"
PROXY3_PASSWORD = "PROXY3_PASS"

LOCAL_SLURM_SCRIPT_PATH = "slurm.job"
REMOTE_SLURM_SCRIPT_PATH = "remote_slurm.job"

REMOTE_RESULT_FILEPATH = "remote_result.txt"

ENV = {
    "HOME": HOME,
    "REMOTE_USER": REMOTE_USER,
    "REMOTE_HOST": REMOTE_HOST,
    "PROXY1_KEYFILE": PROXY1_KEYFILE,
    "PROXY3_PASSWORD": PROXY3_PASSWORD,
    "LOCAL_SLURM_SCRIPT_PATH": LOCAL_SLURM_SCRIPT_PATH,
    "REMOTE_SLURM_SCRIPT_PATH": REMOTE_SLURM_SCRIPT_PATH,
    "REMOTE_RESULT_FILEPATH": REMOTE_RESULT_FILEPATH,
}

CONNECTION_DATA = ConnectionData(
    hostname=REMOTE_HOST,
    username=REMOTE_USER,
    password="1234",
    keyfile="/home/user/.ssh/keyfile",
)

PROXYJUMPS = [
    ConnectionData(
        hostname="proxy1.example.com",
        username="proxy1-user",
        password="proxy1-pass",
        keyfile=PROXY1_KEYFILE,
    ),
    ConnectionData(
        hostname="proxy2.example.com",
        username="proxy2-user",
        password="proxy2-pass",
        keyfile="/home/user/.ssh/proxy2_keyfile",
    ),
    ConnectionData(
        hostname="proxy3.example.com",
        username="proxy3-user",
        password=PROXY3_PASSWORD,
        keyfile="/home/user/.ssh/proxy3_keyfile",
    ),
]


CLEAN_INSTRUCTIONS = ["mycopy.txt", REMOTE_SLURM_SCRIPT_PATH]
COLLECT_INSTRUCTIONS = [CopyInstruction(REMOTE_RESULT_FILEPATH, "result.txt", True)]


@pytest.fixture(autouse=True)
def setup_env() -> Generator[Dict[str, str], None, None]:
    with patch.dict(os.environ, ENV) as env:
        yield env


def run_parser(args: List[str]) -> Union[Options, ParseError]:
    return parse_cli_args(args, localfilesystem(os.getcwd()))


def test__given_valid_launch_args__should_return_matching_config() -> None:
    config = run_parser(
        ["launch", "--watch", "test/testconfig/config.yml", "--save-jobid", "test.log"]
    )

    assert config == LaunchOptions(
        sbatch=REMOTE_SLURM_SCRIPT_PATH,
        connection=CONNECTION_DATA,
        proxyjumps=PROXYJUMPS,
        copy_files=[
            CopyInstruction("myfile.txt", "mycopy.txt"),
            CopyInstruction(LOCAL_SLURM_SCRIPT_PATH, REMOTE_SLURM_SCRIPT_PATH, True),
        ],
        clean_files=CLEAN_INSTRUCTIONS,
        collect_files=COLLECT_INSTRUCTIONS,
        continue_if_job_fails=True,
        watch=True,
        job_id_file="test.log",
    )


def test__given_status_args__when_parsing__should_return_matching_config() -> None:
    config = run_parser(
        [
            "status",
            "test/testconfig/config.yml",
            "--jobid",
            "1234",
        ]
    )

    assert config == ImmediateCommandOptions(
        jobid="1234",
        action=ImmediateCommandOptions.Action.status,
        connection=CONNECTION_DATA,
        proxyjumps=PROXYJUMPS,
    )


def test__given_watch_args__when_parsing__should_return_matching_config() -> None:
    config = run_parser(
        [
            "watch",
            "test/testconfig/config.yml",
            "--jobid",
            "1234",
        ]
    )

    assert config == WatchOptions(
        jobid="1234",
        connection=CONNECTION_DATA,
        proxyjumps=PROXYJUMPS,
    )


def test__given_cancel_args__when_parsing__should_return_matching_config() -> None:
    config = run_parser(
        [
            "cancel",
            "test/testconfig/config.yml",
            "--jobid",
            "1234",
        ]
    )

    assert config == ImmediateCommandOptions(
        jobid="1234",
        action=ImmediateCommandOptions.Action.cancel,
        connection=CONNECTION_DATA,
        proxyjumps=PROXYJUMPS,
    )


def test__given_finalize_args__when_parsing__returns_matching_config() -> None:
    config = run_parser(
        [
            "finalize",
            "test/testconfig/config.yml",
        ]
    )

    assert config == FinalizeOptions(
        connection=CONNECTION_DATA,
        proxyjumps=PROXYJUMPS,
        clean_files=CLEAN_INSTRUCTIONS,
        collect_files=COLLECT_INSTRUCTIONS,
    )


@pytest.fixture
def log_file() -> Iterator[None]:
    log_file = Path("test.log")
    log_file.write_text("1234")
    yield
    log_file.unlink(missing_ok=True)


@pytest.mark.usefixtures("log_file")
@pytest.mark.parametrize("command", ("status", "watch", "cancel"))
def test__specified_option_to_read_jobid_from_log__when_parsing__options_contain_jobid(
    command: str,
) -> None:
    config = run_parser(
        [command, "test/testconfig/config.yml", "--read-jobid-from", "test.log"]
    )

    config = cast(ImmediateCommandOptions, config)
    assert config.jobid == "1234"


def test__given_non_existing_config_file__returns_parse_error() -> None:
    config = run_parser(["launch"])

    assert isinstance(config, ParseError)


def test__given_copy_information_in_sbatch__creates_options_with_copy_instructions() -> None:
    config = parse_cli_args(
        ["launch", "test/testconfig/sbatch_copy.yml"],
        localfilesystem(os.getcwd()),
    )

    config = cast(LaunchOptions, config)
    assert config.copy_files == [
        CopyInstruction(
            source="test/testconfig/local_slurm.job",
            destination="the-job-script.sh",
            overwrite=True,
        )
    ]
