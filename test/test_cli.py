import os
from typing import List

import pytest
from hpcrocket.cli import parse_cli_args
from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.launchoptions import (
    LaunchOptions,
    Options,
    SimpleJobOptions,
    WatchOptions,
)
from hpcrocket.pyfilesystem.localfilesystem import LocalFilesystem
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


@pytest.fixture(autouse=True)
def setup_env() -> None:
    os.environ.update(ENV)


def run_parser(args: List[str]) -> Options:
    return parse_cli_args(args, LocalFilesystem("."))


def test__given_valid_launch_args__should_return_matching_config() -> None:
    config = run_parser(
        [
            "launch",
            "--watch",
            "test/testconfig/config.yml",
        ]
    )

    assert config == LaunchOptions(
        sbatch=REMOTE_SLURM_SCRIPT_PATH,
        connection=CONNECTION_DATA,
        proxyjumps=PROXYJUMPS,
        copy_files=[
            CopyInstruction("myfile.txt", "mycopy.txt"),
            CopyInstruction(LOCAL_SLURM_SCRIPT_PATH, REMOTE_SLURM_SCRIPT_PATH, True),
        ],
        clean_files=["mycopy.txt", REMOTE_SLURM_SCRIPT_PATH],
        collect_files=[CopyInstruction(REMOTE_RESULT_FILEPATH, "result.txt", True)],
        watch=True,
    )


def test__given_status_args__when_parsing__should_return_matching_config() -> None:
    config = run_parser(
        [
            "status",
            "test/testconfig/config.yml",
            "1234",
        ]
    )

    assert config == SimpleJobOptions(
        jobid="1234",
        action=SimpleJobOptions.Action.status,
        connection=CONNECTION_DATA,
        proxyjumps=PROXYJUMPS,
    )


def test__given_watch_args__when_parsing__should_return_matching_config() -> None:
    config = run_parser(
        [
            "watch",
            "test/testconfig/config.yml",
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
            "1234",
        ]
    )

    assert config == SimpleJobOptions(
        jobid="1234",
        action=SimpleJobOptions.Action.cancel,
        connection=CONNECTION_DATA,
        proxyjumps=PROXYJUMPS,
    )
