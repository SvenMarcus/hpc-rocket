import fs.base
import fs.osfs
from fs.subfs import ClosingSubFS

import ssh_slurm_runner.chmodsshfs as sshfs
from ssh_slurm_runner.pyfilesystem import PyFilesystemBased


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

    def __init__(self, user: str, host: str, password: str = None, private_key: str = None, private_keyfile: str = None) -> None:
        """
        Args:
            user (str): The user on the remote machine
            host (str): The address of the remote machine
            password (str): The user's password on the remote machine. Alternative to `private_key`.
            private_key (str): The user's private SSH key. Alternative to `password`.
        """
        self._internal_fs = sshfs.PermissionChangingSSHFSDecorator(
            host, user=user, passwd=password, pkey=private_key or private_keyfile).opendir(f"/home/{user}", factory=ClosingSubFS)

    @property
    def internal_fs(self) -> fs.base.FS:
        return self._internal_fs
