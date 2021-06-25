import argparse
from ssh_slurm_runner.launchoptions import LaunchOptions


def parse_cli_args(args) -> LaunchOptions:
    parser = argparse.ArgumentParser("ssh_slurm_runner")
    parser.add_argument("jobfile", type=str)
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--user", type=str, required=True)
    parser.add_argument("--password", type=str)
    parser.add_argument("--private-key", type=str)
    parser.add_argument("--keyfile", type=str)

    config = parser.parse_args(args)
    return LaunchOptions(
        config.jobfile,
        config.host,
        config.user,
        config.password,
        config.private_key,
        config.keyfile
    )
