import argparse

import yaml

from ssh_slurm_runner.launchoptions import LaunchOptions


def parse_cli_args(args) -> LaunchOptions:
    parser = argparse.ArgumentParser("ssh_slurm_runner")
    subparsers = parser.add_subparsers()

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("jobfile", type=str)
    run_parser.add_argument("--host", type=str, required=True)
    run_parser.add_argument("--user", type=str, required=True)
    run_parser.add_argument("--password", type=str)
    run_parser.add_argument("--private-key", type=str)
    run_parser.add_argument("--keyfile", type=str)

    yaml_parser = subparsers.add_parser("from-config")
    yaml_parser.add_argument("configfile", type=str)

    config = parser.parse_args(args)
    if "configfile" in config:
        return _parse_yaml_configuration(config.configfile)

    return LaunchOptions(
        config.jobfile,
        config.host,
        config.user,
        config.password,
        config.private_key,
        config.keyfile
    )


def _parse_yaml_configuration(path: str) -> LaunchOptions:
    with open(path, "r") as file:
        config = yaml.load(file, Loader=yaml.SafeLoader)

        return LaunchOptions(
            sbatch=config["sbatch"],
            host=config["host"],
            user=config["user"],
            password=config.get("password"),
            private_key=config.get("private_key"),
            private_keyfile=config.get("private_keyfile"),
            copy_files=_collect_copy_instructions(config.get("copy", [])),
            clean_files=config.get("clean", [])
        )


def _collect_copy_instructions(copy_list):
    return [(cp["from"], cp["to"]) for cp in copy_list]
