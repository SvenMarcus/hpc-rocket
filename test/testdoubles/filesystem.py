from contextlib import contextmanager
from unittest.mock import patch, DEFAULT

from fs.memoryfs import MemoryFS

from hpcrocket.core.filesystem import FilesystemFactory, Filesystem


class DummyFilesystemFactory(FilesystemFactory):

    def create_local_filesystem(self) -> 'Filesystem':
        return DummyFilesystem()

    def create_ssh_filesystem(self) -> 'Filesystem':
        return DummyFilesystem()


class DummyFilesystem(Filesystem):

    def exists(self, path: str) -> bool:
        return False

    def copy(self, source: str, target: str, overwrite: bool = False, filesystem: 'Filesystem' = None) -> None:
        pass

    def delete(self, path: str) -> None:
        pass


class MemoryFilesystemFactory(FilesystemFactory):

    def create_local_filesystem(self) -> 'Filesystem':
        return MemoryFS()

    def create_ssh_filesystem(self) -> 'Filesystem':
        return MemoryFS()


@contextmanager
def sshfs_with_connection_fake(sshclient_mock):

    def emulate_connect(*args, **kwargs):
        map_to_paramiko_arguments(kwargs)
        sshclient_mock.connect(*args, **kwargs)
        return DEFAULT

    def map_to_paramiko_arguments(kwargs):
        kwargs["hostname"] = kwargs["host"]
        kwargs["username"] = kwargs["user"]
        kwargs["password"] = kwargs["passwd"]
        kwargs["pkey"] = kwargs["pkey"]

    patcher = patch("hpcrocket.ssh.chmodsshfs.PermissionChangingSSHFSDecorator")
    patched = patcher.start()
    patched.side_effect = emulate_connect
    yield patched

    patcher.stop()