from abc import ABC, abstractmethod

import fs.base
import fs.copy as fscp
from fs.enums import ResourceType
import fs.sshfs as sshfs

from ssh_slurm_runner.filesystem import Filesystem


class PyFilesystemBased(Filesystem, ABC):
    """
    Abstract base class for Filesystems based on PyFilesystem2
    """

    @property
    @abstractmethod
    def internal_fs(self) -> fs.base.FS:
        """Returns the internally used PyFilesystem

        Returns:
            fs.base.FS: The internal PyFilesystem
        """
        pass


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

    def copy(self, source: str, target: str, filesystem: 'Filesystem' = None) -> None:
        self._raise_if_source_does_not_exist(source)
        self._raise_if_target_exists(target, filesystem)

        if filesystem:
            self._try_copy_to_filesystem(source, target, filesystem)
            return

        self._try_copy(source, target)

    def _raise_if_source_does_not_exist(self, source):
        if not self.exists(source):
            raise FileNotFoundError(source)

    def _raise_if_target_exists(self, target, filesystem):
        target_filesystem = filesystem or self
        if target_filesystem.exists(target):
            raise FileExistsError(target)

    def delete(self, path: str) -> None:
        if not self.exists(path):
            raise FileNotFoundError(path)

        if self.internal_fs.gettype(path) is ResourceType.directory:
            self.internal_fs.removetree(path)
            return

        self.internal_fs.remove(path)

    def exists(self, path: str) -> None:
        return self.internal_fs.exists(path)

    def _try_copy_to_filesystem(self, source, target, filesystem):
        if not isinstance(filesystem, PyFilesystemBased):
            raise RuntimeError(
                "SSHFilesystem currently only works with PyFilesystem2 based Filesystems")

        fscp.copy_file(self.internal_fs, source,
                       filesystem.internal_fs, target)

    def _try_copy(self, source, target):
        if self.internal_fs.gettype(source) is ResourceType.directory:
            self.internal_fs.copydir(source, target, create=True)
            return

        self.internal_fs.copy(source, target)
