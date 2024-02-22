import argparse

from importlib import metadata


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("hpc-rocket")
    _add_version_flag(parser)

    subparsers = parser.add_subparsers(dest="command")
    _setup_launch_parser(subparsers)
    _setup_finalize_parser(subparsers)
    _setup_status_parser(subparsers)
    _setup_watch_parser(subparsers)
    _setup_cancel_parser(subparsers)

    return parser


def _add_version_flag(parser: argparse.ArgumentParser) -> None:
    meta = metadata.metadata("hpc-rocket")
    parser.add_argument(
        "--version",
        action="version",
        version=f"{meta['name']} {meta['version']}",
    )


def _setup_launch_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    parser = subparsers.add_parser("launch", help="Launch a remote job")
    _add_configfile_arg(parser)
    parser.add_argument("--watch", default=False, dest="watch", action="store_true")
    parser.add_argument("--save-jobid", dest="jobid_file", type=str)


def _setup_finalize_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    parser = subparsers.add_parser(
        "finalize", help="Run collect and clean instructions"
    )
    _add_configfile_arg(parser)


def _setup_status_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    parser = subparsers.add_parser("status", help="Check on a job's current status")
    _add_configfile_arg(parser)
    _add_read_jobid_arg(parser)


def _setup_cancel_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    parser = subparsers.add_parser("cancel", help="Cancel a job")
    _add_configfile_arg(parser)
    _add_read_jobid_arg(parser)


def _setup_watch_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    parser = subparsers.add_parser("watch", help="Monitor a job until it completes")
    _add_configfile_arg(parser)
    _add_read_jobid_arg(parser)


def _add_configfile_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "configfile",
        type=str,
        nargs="?",
        default="rocket.yml",
        help="A config file containing the connection data. Defaults to rocket.yml",
    )


def _add_read_jobid_arg(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--jobid", type=str, help="The ID of the job to be monitored")
    group.add_argument(
        "--read-jobid-from",
        type=str,
        help="Read the job ID from a previously saved log file",
    )
