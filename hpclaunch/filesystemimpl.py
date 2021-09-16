from typing import List
from hpclaunch.errors import SSHError
import fs.base
from fs.errors import CreateFailed
import fs.osfs
from fs.subfs import ClosingSubFS

import hpclaunch.chmodsshfs as sshfs
from hpclaunch.pyfilesystembased import PyFilesystemBased
from hpclaunch.sshexecutor import ConnectionData, build_channel_with_proxyjumps


class LocalFilesystem(PyFilesystemBased):
    """
    A PyFilesystem2 based filesystem that uses the computer's local filesystem
    """

    def __init__(self, rootpath: str) -> None:
        """
        Args:
            rootpath (str): The path the filesystem should be opened in
        """
        self._internal_fs = fs.osfs.OSFS(rootpath)

    @property
    def internal_fs(self) -> fs.base.FS:
        return self._internal_fs


class SSHFilesystem(PyFilesystemBased):
    """
    A PyFilesystem2 based Filesystem that connects to a remote machine via SSH
    """

    def __init__(
            self, connection_data: ConnectionData,
            proxyjumps: List[ConnectionData] = None) -> None:
        """
        Args:
            user (str): The user on the remote machine
            host (str): The address of the remote machine
            password (str): The user's password on the remote machine. Alternative to `private_key`.
            private_key (str): The user's private SSH key. Alternative to `password`.
        """
        self._internal_fs = self._create_new_sshfilesystem(
            connection_data, proxyjumps)

    def _create_new_sshfilesystem(self, connection_data: ConnectionData, proxyjumps: List[ConnectionData] = None):
        try:
            channel = build_channel_with_proxyjumps(connection_data, proxyjumps or [])
            fs = sshfs.PermissionChangingSSHFSDecorator(
                host=connection_data.hostname,
                user=connection_data.username,
                passwd=connection_data.password,
                pkey=connection_data.key or connection_data.keyfile,
                port=connection_data.port, sock=channel)

            return fs.opendir(f"/home/{connection_data.username}", factory=ClosingSubFS)
        except CreateFailed as err:
            raise SSHError(f"Could not connect to {connection_data.hostname}") from err

    @property
    def internal_fs(self) -> fs.base.FS:
        return self._internal_fs
