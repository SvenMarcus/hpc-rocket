import dataclasses
from test.slurmoutput import DEFAULT_JOB_ID
from typing import List, Optional

from hpcrocket.core.filesystem.progressive import CopyInstruction
from hpcrocket.core.launchoptions import (
    FinalizeOptions,
    LaunchOptions,
    ImmediateCommandOptions,
    WatchOptions,
)
from hpcrocket.ssh.connectiondata import ConnectionData

LOCAL_FILE = "myfile.txt"
LOCAL_DIR = "localdir/"

REMOTE_FILE = "mycopy.txt"
REMOTE_DIR = "remotedir/"

COLLECTED_FILE = "mycollect.txt"

GLOB_PATTERN = "*.txt"
NON_MATCHING_FILE = "NON_MATCHING_FILE.gif"


def launch_options(
    copy: Optional[List[CopyInstruction]] = None,
    collect: Optional[List[CopyInstruction]] = None,
    clean: Optional[List[str]] = None,
    connection: Optional[ConnectionData] = None,
    proxyjumps: Optional[List[ConnectionData]] = None,
    watch: bool = False,
) -> LaunchOptions:
    return LaunchOptions(
        connection=connection or main_connection(),
        proxyjumps=proxyjumps or [],
        sbatch="test.job",
        watch=watch,
        poll_interval=0,
        copy_files=copy or [],
        collect_files=collect or [],
        clean_files=clean or [],
    )


def launch_options_with_copy() -> LaunchOptions:
    return launch_options(copy=[CopyInstruction(LOCAL_FILE, REMOTE_FILE)])


def launch_options_with_collect() -> LaunchOptions:
    return launch_options(
        collect=[CopyInstruction(REMOTE_FILE, COLLECTED_FILE)],
        watch=True,
    )


def launch_options_with_clean() -> LaunchOptions:
    return launch_options(clean=[REMOTE_FILE], watch=True)


def launch_options_copy_collect() -> LaunchOptions:
    return launch_options(
        copy=[CopyInstruction(LOCAL_FILE, REMOTE_FILE)],
        collect=[CopyInstruction(REMOTE_FILE, COLLECTED_FILE)],
        watch=True,
    )


def launch_options_copy_collect_clean() -> LaunchOptions:
    opts = launch_options_copy_collect()
    opts.clean_files = [REMOTE_FILE]
    return opts


def launch_options_with_proxy() -> LaunchOptions:
    return launch_options(proxyjumps=[proxy_connection()])


def launch_options_with_proxy_only_password() -> LaunchOptions:
    return launch_options(
        connection=main_connection_only_password(),
        proxyjumps=[proxy_connection_only_password()],
    )


def main_connection() -> ConnectionData:
    return ConnectionData(
        hostname="example.com",
        username="myuser",
        password="mypassword",
        key="PRIVATE",
        keyfile="my_private_keyfile",
    )


def main_connection_only_password() -> ConnectionData:
    return dataclasses.replace(main_connection(), key=None, keyfile=None)


def proxy_connection() -> ConnectionData:
    return ConnectionData(
        hostname="proxy1-host",
        username="proxy1-user",
        password="proxy1-pass",
        keyfile="~/proxy1-keyfile",
    )


def proxy_connection_only_password() -> ConnectionData:
    return dataclasses.replace(proxy_connection(), key=None, keyfile=None)


def watch_options_with_proxy() -> WatchOptions:
    return WatchOptions(
        jobid=DEFAULT_JOB_ID,
        connection=main_connection(),
        proxyjumps=[proxy_connection()],
        poll_interval=0,
    )


def simple_options_with_proxy(
    action: ImmediateCommandOptions.Action,
) -> ImmediateCommandOptions:
    return ImmediateCommandOptions(
        jobid=DEFAULT_JOB_ID,
        action=action,
        connection=main_connection(),
        proxyjumps=[proxy_connection()],
    )


def status_options_with_proxy() -> ImmediateCommandOptions:
    return simple_options_with_proxy(ImmediateCommandOptions.Action.status)


def cancel_options_with_proxy() -> ImmediateCommandOptions:
    return simple_options_with_proxy(ImmediateCommandOptions.Action.cancel)


def finalize_options() -> FinalizeOptions:
    return FinalizeOptions(connection=main_connection())


def finalize_options_with_collect() -> FinalizeOptions:
    return FinalizeOptions(
        connection=main_connection(),
        collect_files=[CopyInstruction(REMOTE_FILE, COLLECTED_FILE)],
    )


def finalize_options_with_clean() -> FinalizeOptions:
    return FinalizeOptions(
        connection=main_connection(),
        clean_files=[REMOTE_FILE],
    )


def finalize_options_with_collect_and_clean() -> FinalizeOptions:
    return FinalizeOptions(
        connection=main_connection(),
        collect_files=[CopyInstruction(REMOTE_FILE, COLLECTED_FILE)],
        clean_files=[REMOTE_FILE],
    )
