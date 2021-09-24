import dataclasses

from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.ssh.sshexecutor import ConnectionData


def options():
    return LaunchOptions(
        connection=main_connection(),
        sbatch="test.job",
        poll_interval=0
    )


def options_with_proxy():
    return dataclasses.replace(options(), proxyjumps=[proxy_connection()])


def main_connection():
    return ConnectionData(
        hostname="example.com",
        username="myuser",
        password="mypassword",
        key="PRIVATE",
        keyfile="my_private_keyfile",
    )


def proxy_connection():
    return ConnectionData(
        hostname="proxy1-host",
        username="proxy1-user",
        password="proxy1-pass",
        keyfile="~/proxy1-keyfile"
    )


def options_with_files_to_copy(files_to_copy):
    return dataclasses.replace(
        options(),
        copy_files=files_to_copy
    )


def options_with_files_to_copy_and_clean(files_to_copy, files_to_clean):
    return dataclasses.replace(
        options_with_files_to_copy(files_to_copy),
        clean_files=files_to_clean)


def options_with_files_to_copy_collect_and_clean(files_to_copy, files_to_collect, files_to_clean):
    return dataclasses.replace(
        options_with_files_to_copy_and_clean(
            files_to_copy,
            files_to_clean),
        collect_files=files_to_collect)
