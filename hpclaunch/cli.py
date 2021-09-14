import argparse

import yaml

from hpclaunch.environmentpreparation import CopyInstruction
from hpclaunch.launchoptions import LaunchOptions


def parse_cli_args(args) -> LaunchOptions:
    parser = argparse.ArgumentParser("hpclaunch")
    subparsers = parser.add_subparsers()

    run_parser = subparsers.add_parser(
        "run", help="Configure HPC Launch from the command line")
    run_parser.add_argument("jobfile", type=str,
                            help="The name of the job file to run with sbatch")
    run_parser.add_argument("--host", type=str, required=True,
                            help="Address of the remote machine")
    run_parser.add_argument("--user", type=str, required=True,
                            help="User on the remote machine")
    run_parser.add_argument("--password", type=str,
                            help="The password for the given user")
    run_parser.add_argument("--private-key", type=str,
                            help="A private SSH key")
    run_parser.add_argument(
        "--keyfile", type=str, help="The path to a file containing a private SSH key")

    yaml_parser = subparsers.add_parser(
        "from-config", help="Configure HPC Launch from a configuration file")
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
            clean_files=config.get("clean", []),
            collect_files=_collect_copy_instructions(config.get("collect", []))
        )


def _collect_copy_instructions(copy_list):
    return [CopyInstruction(cp["from"],
                            cp["to"],
                            cp.get("overwrite", False))
            for cp in copy_list]
