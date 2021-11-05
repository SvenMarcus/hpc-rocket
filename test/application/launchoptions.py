import dataclasses
from typing import List, Optional
from hpcrocket.core.environmentpreparation import CopyInstruction

from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.ssh.connectiondata import ConnectionData


def options(
    copy: Optional[List[CopyInstruction]] = None,
    collect: Optional[List[CopyInstruction]] = None,
    clean: Optional[List[str]] = None,
    connection: Optional[ConnectionData] = None,
    proxyjumps: Optional[List[ConnectionData]] = None,
    watch: bool = False
) -> LaunchOptions:
    return LaunchOptions(
        connection=connection or main_connection(),
        proxyjumps=proxyjumps or [],
        sbatch="test.job",
        watch=watch,
        poll_interval=0,
        copy_files=copy or [],
        collect_files=collect or [],
        clean_files=clean or []
    )


def options_with_proxy() -> LaunchOptions:
    return options(proxyjumps=[proxy_connection()])


def options_with_proxy_only_password() -> LaunchOptions:
    return options(connection=main_connection_only_password(),
                   proxyjumps=[proxy_connection_only_password()])


def main_connection() -> ConnectionData:
    return ConnectionData(
        hostname="example.com",
        username="myuser",
        password="mypassword",
        key="PRIVATE",
        keyfile="my_private_keyfile",
    )


def main_connection_only_password() -> ConnectionData:
    return dataclasses.replace(
        main_connection(),
        key=None,
        keyfile=None)


def proxy_connection() -> ConnectionData:
    return ConnectionData(
        hostname="proxy1-host",
        username="proxy1-user",
        password="proxy1-pass",
        keyfile="~/proxy1-keyfile"
    )


def proxy_connection_only_password() -> ConnectionData:
    return dataclasses.replace(
        proxy_connection(),
        key=None,
        keyfile=None)
