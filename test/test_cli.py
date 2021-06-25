from ssh_slurm_runner.launchoptions import LaunchOptions
from ssh_slurm_runner.cli import parse_cli_args


def test__given_valid_args__should_return_matching_config():
    config = parse_cli_args([
        "slurm.job",
        "--host",
        "cluster.example.com",
        "--user",
        "the_user",
        "--password",
        "the_password",
        "--keyfile",
        "/home/user/.ssh/kefile",
        "--private-key",
        "SECRET_KEY"

    ])

    assert config == LaunchOptions(
        sbatch="slurm.job",
        host="cluster.example.com",
        user="the_user",
        password="the_password",
        private_keyfile="/home/user/.ssh/kefile",
        private_key="SECRET_KEY"
    )
