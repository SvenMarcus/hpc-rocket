import fs.base
import fs.sshfs as sshfs

from ssh_slurm_runner.pyfilesystem import PyFilesystemBased


class SSHFilesystem(PyFilesystemBased):
    """
    A PyFilesystem2 based Filesystem that connects to a remote machine via SSH
    """

    def __init__(self, user: str, host: str, private_key: str) -> None:
        """
        Args:
            user (str): The user on the remote machine
            host (str): The address of the remote machine
            private_key (str): The user's private SSH key. Needed for authentication.
        """
        self._internal_fs = sshfs.SSHFS(host, user=user, pkey=private_key)

    @property
    def internal_fs(self) -> fs.base.FS:
        return self._internal_fs
