from contextlib import contextmanager
from typing import List, Optional, cast
from unittest.mock import DEFAULT, patch

from hpcrocket.core.filesystem import Filesystem, FilesystemFactory


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


class MemoryFilesystemFactoryStub(FilesystemFactory):

    def __init__(self, local_fs: 'MemoryFilesystemFake' = None, ssh_fs: 'MemoryFilesystemFake' = None) -> None:
        self.local_filesystem = local_fs or MemoryFilesystemFake()
        self.ssh_filesystem = ssh_fs or MemoryFilesystemFake()

    def create_local_filesystem(self) -> 'Filesystem':
        return self.local_filesystem

    def create_ssh_filesystem(self) -> 'Filesystem':
        return self.ssh_filesystem


class MemoryFilesystemFake(Filesystem):

    def __init__(self, files: List[str] = []) -> None:
        self.files = set(files)

    def copy(
            self, source: str, target: str, overwrite: bool = False, filesystem: Optional['Filesystem'] = None) -> None:
        assert filesystem is None or isinstance(filesystem, MemoryFilesystemFake)

        if not self.exists(source):
            raise FileNotFoundError(source)

        other = cast(MemoryFilesystemFake, filesystem)
        if other.exists(target) and not overwrite:
            raise FileExistsError(target)

        other.files.add(target)

    def delete(self, path: str) -> None:
        if not self.exists(path):
            raise FileNotFoundError(path)

        self.files.remove(path)

    def exists(self, path: str) -> bool:
        return path in self.files


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
