import dataclasses
from typing import List
from hpcrocket.core.environmentpreparation import CopyInstruction

from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.ssh.connectiondata import ConnectionData


def options(
    copy: List[CopyInstruction] = None,
    collect: List[CopyInstruction] = None,
    clean: List[str] = None,
    connection: ConnectionData = None,
    proxyjumps: List[ConnectionData] = None,
    watch: bool = False
):
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


def options_with_proxy():
    return options(proxyjumps=[proxy_connection()])


def options_with_proxy_only_password():
    return options(connection=main_connection_only_password(),
                   proxyjumps=[proxy_connection_only_password()])


def main_connection():
    return ConnectionData(
        hostname="example.com",
        username="myuser",
        password="mypassword",
        key="PRIVATE",
        keyfile="my_private_keyfile",
    )


def main_connection_only_password():
    return dataclasses.replace(
        main_connection(),
        key=None,
        keyfile=None)


def proxy_connection():
    return ConnectionData(
        hostname="proxy1-host",
        username="proxy1-user",
        password="proxy1-pass",
        keyfile="~/proxy1-keyfile"
    )


def proxy_connection_only_password():
    return dataclasses.replace(
        proxy_connection(),
        key=None,
        keyfile=None)
