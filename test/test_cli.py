from hpcrocket.cli import parse_cli_args
from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.launchoptions import (LaunchOptions, SimpleJobOptions,
                                          WatchOptions)
from hpcrocket.ssh.connectiondata import ConnectionData

CONNECTION_DATA = ConnectionData(
    hostname="cluster.example.com",
    username="the_user",
    password="1234",
    keyfile="/home/user/.ssh/keyfile",
)

PROXYJUMPS = [
    ConnectionData(
        hostname="proxy1.example.com",
        username="proxy1-user",
        password="proxy1-pass",
        keyfile="/home/user/.ssh/proxy1_keyfile",
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
        password="proxy3-pass",
        keyfile="/home/user/.ssh/proxy3_keyfile",
    ),
]


def test__given_valid_launch_args__should_return_matching_config():
    config = parse_cli_args([
        "launch",
        "--watch",
        "test/testconfig/config.yml"
    ])

    assert config == LaunchOptions(
        sbatch="slurm.job",
        connection=CONNECTION_DATA,
        proxyjumps=PROXYJUMPS,
        copy_files=[
            CopyInstruction("myfile.txt", "mycopy.txt"),
            CopyInstruction("slurm.job", "slurm.job", True)],
        clean_files=["mycopy.txt", "slurm.job"],
        collect_files=[CopyInstruction("result.txt", "result.txt", True)],
        watch=True
    )


def test__given_status_args__when_parsing__should_return_matching_config():
    config = parse_cli_args([
        "status",
        "test/testconfig/config.yml",
        "1234"
    ])

    assert config == SimpleJobOptions(
        jobid="1234",
        action=SimpleJobOptions.Action.status,
        connection=CONNECTION_DATA,
        proxyjumps=PROXYJUMPS,
    )


def test__given_watch_args__when_parsing__should_return_matching_config():
    config = parse_cli_args([
        "watch",
        "test/testconfig/config.yml",
        "1234"
    ])

    assert config == WatchOptions(
        jobid="1234",
        connection=CONNECTION_DATA,
        proxyjumps=PROXYJUMPS,
    )


def test__given_cancel_args__when_parsing__should_return_matching_config():
    config = parse_cli_args([
        "cancel",
        "test/testconfig/config.yml",
        "1234"
    ])

    assert config == SimpleJobOptions(
        jobid="1234",
        action=SimpleJobOptions.Action.cancel,
        connection=CONNECTION_DATA,
        proxyjumps=PROXYJUMPS
    )
