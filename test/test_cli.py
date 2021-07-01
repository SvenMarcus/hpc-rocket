from ssh_slurm_runner.launchoptions import LaunchOptions
from ssh_slurm_runner.cli import parse_cli_args


def test__given_valid_args__should_return_matching_config():
    config = parse_cli_args([
        "run",
        "slurm.job",
        "--host",
        "cluster.example.com",
        "--user",
        "the_user",
        "--password",
        "the_password",
        "--keyfile",
        "/home/user/.ssh/keyfile",
        "--private-key",
        "SECRET_KEY"

    ])

    assert config == LaunchOptions(
        sbatch="slurm.job",
        host="cluster.example.com",
        user="the_user",
        password="the_password",
        private_keyfile="/home/user/.ssh/keyfile",
        private_key="SECRET_KEY"
    )


def test__given_valid_args_for_yaml__should_return_matching_config():
    config = parse_cli_args([
        "from-config",
        "test/testconfig/config.yml"
    ])

    assert config == LaunchOptions(
        sbatch="slurm.job",
        host="cluster.example.com",
        user="the_user",
        private_keyfile="/home/user/.ssh/keyfile",
        copy_files=[("myfile.txt", "mycopy.txt"), ("slurm.job", "slurm.job")],
        clean_files=["mycopy.txt", "slurm.job"]
    )
