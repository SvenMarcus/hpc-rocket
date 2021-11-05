from hpcrocket.ssh.connectiondata import ConnectionData
from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.launchoptions import JobBasedOptions, LaunchOptions
from hpcrocket.cli import parse_cli_args


def test__given_valid_launch_args__should_return_matching_config():
    config = parse_cli_args([
        "launch",
        "--watch",
        "test/testconfig/config.yml"
    ])

    assert config == LaunchOptions(
        sbatch="slurm.job",
        connection=ConnectionData(
            hostname="cluster.example.com",
            username="the_user",
            password="1234",
            keyfile="/home/user/.ssh/keyfile",
        ),
        proxyjumps=[
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
        ],
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

    assert config == JobBasedOptions(
        jobid="1234",
        action=JobBasedOptions.Action.status,
        connection=ConnectionData(
            hostname="cluster.example.com",
            username="the_user",
            password="1234",
            keyfile="/home/user/.ssh/keyfile",
        ),
        proxyjumps=[
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
        ],
    )
