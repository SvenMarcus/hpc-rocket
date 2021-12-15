from typing import List, Optional

import fs.base
import hpcrocket.ssh.chmodsshfs as sshfs
from fs.errors import CreateFailed
from fs.subfs import ClosingSubFS
from hpcrocket.pyfilesystem.pyfilesystembased import PyFilesystemBased
from hpcrocket.ssh.connectiondata import ConnectionData
from hpcrocket.ssh.errors import SSHError
from hpcrocket.ssh.sshexecutor import build_channel_with_proxyjumps


class SSHFilesystem(PyFilesystemBased):
    """
    A PyFilesystem2 based Filesystem that connects to a remote machine via SSH
    """

    def __init__(
            self, connection_data: ConnectionData,
            proxyjumps: Optional[List[ConnectionData]] = None) -> None:
        """
        Args:
            user (str): The user on the remote machine
            host (str): The address of the remote machine
            password (str): The user's password on the remote machine. Alternative to `private_key`.
            private_key (str): The user's private SSH key. Alternative to `password`.
        """
        self._internal_fs = self._create_new_sshfilesystem(
            connection_data, proxyjumps)

    def _create_new_sshfilesystem(self, connection_data: ConnectionData,
                                  proxyjumps: Optional[List[ConnectionData]] = None) -> fs.base.FS:
        try:
            channel = build_channel_with_proxyjumps(
                connection_data, proxyjumps or [])
            fs = sshfs.PermissionChangingSSHFSDecorator(
                host=connection_data.hostname,
                user=connection_data.username,
                passwd=connection_data.password,
                pkey=connection_data.key or connection_data.keyfile,
                port=connection_data.port, sock=channel)

            return fs.opendir(fs.homedir(), factory=ClosingSubFS)
        except CreateFailed as err:
            raise SSHError(
                f"Could not connect to {connection_data.hostname}") from err

    @property
    def internal_fs(self) -> fs.base.FS:
        return self._internal_fs
