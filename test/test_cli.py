from hpclaunch.environmentpreparation import CopyInstruction
from hpclaunch.launchoptions import LaunchOptions
from hpclaunch.cli import parse_cli_args


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
        copy_files=[
            CopyInstruction("myfile.txt", "mycopy.txt"), 
            CopyInstruction("slurm.job", "slurm.job", True)],
        clean_files=["mycopy.txt", "slurm.job"],
        collect_files=[CopyInstruction("result.txt", "result.txt", True)],
    )
