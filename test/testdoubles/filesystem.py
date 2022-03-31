import fnmatch
import os.path
from contextlib import contextmanager
from dataclasses import dataclass, field
from copy import deepcopy
from typing import List, Optional, Set, cast, Union
from unittest.mock import DEFAULT, patch

from hpcrocket.core.filesystem import Filesystem, FilesystemFactory
from hpcrocket.typesafety import get_or_raise


class DummyFilesystemFactory(FilesystemFactory):

    def create_local_filesystem(self) -> 'Filesystem':
        return DummyFilesystem()

    def create_ssh_filesystem(self) -> 'Filesystem':
        return DummyFilesystem()


class DummyFilesystem(Filesystem):

    def glob(self, pattern: str) -> List[str]:
        return []

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


@dataclass
class FileStub:
    name: str
    parent: 'DirectoryStub'
    content: str = ""

    def is_dir(self) -> bool:
        return False


@dataclass
class DirectoryStub:
    name: str
    parent: Optional['DirectoryStub'] = None
    items: List['FilesystemItem'] = field(default_factory=list)

    def is_dir(self) -> bool:
        return True


FilesystemItem = Union[FileStub, DirectoryStub]


class MemoryFilesystemFake(Filesystem):

    def __init__(self, files: List[str] = []) -> None:
        self._root = DirectoryStub("")

    def create_file_stub(self, path: str, content: str) -> None:
        head, filename = os.path.split(path)
        dir_stub = self._root
        if head:
            dir_stub = self._create_dir_stub(head)
        file_stub = FileStub(filename, dir_stub, content)
        dir_stub.items.append(file_stub)

    def create_dir_stub(self, path: str) -> None:
        self._create_dir_stub(path)

    def _create_dir_stub(self, path: str) -> DirectoryStub:
        split_path = path.split(os.path.sep)
        current_dir = self._root

        walked_path = ""
        for dirname in split_path:
            walked_path += dirname
            match = self._find_matching_item(walked_path)
            if match and match.is_dir():
                current_dir = cast(DirectoryStub, match)
            else:
                current_dir = self._create_sub_dir(current_dir, dirname)

            walked_path += os.path.sep

        return current_dir

    def _create_sub_dir(self, current_dir: DirectoryStub, dirname: str) -> DirectoryStub:
        sub_dir = DirectoryStub(dirname, current_dir)
        current_dir.items.append(sub_dir)
        return sub_dir

    def get_content_of_file_stub(self, path: str) -> str:
        return cast(FileStub, self._find_matching_item(path)).content

    def glob(self, pattern: str) -> List[str]:
        matches: List[str] = []
        cd = self._root
        walked_path = ""
        for item in cd.items:
            item_path = walked_path + os.path.sep + item.name
            if fnmatch.fnmatch(item_path, pattern):
                if item.is_dir():
                    # walk down
        return []

    def copy(
        self,
        source: str,
        target: str,
        overwrite: bool = False,
        filesystem: Optional['Filesystem'] = None
    ) -> None:
        assert filesystem is None or isinstance(filesystem, MemoryFilesystemFake)
        other = cast(MemoryFilesystemFake, filesystem) or self
        self._raise_if_target_file_exists(other, target, overwrite)
        self._perform_copy(other, source, target, overwrite)

    def _perform_copy(
        self,
        other: 'MemoryFilesystemFake',
        source: str,
        target: str,
        overwrite: bool
    ) -> None:
        match = get_or_raise(
            self._find_matching_item(source),
            FileNotFoundError
        )

        if match.is_dir():  # type: ignore
            match = cast(DirectoryStub, match)
            self._copy_directory(source, target, overwrite, other, match)
        else:
            match = cast(FileStub, match)
            self._copy_single_file(other, match, target, overwrite)

    def _raise_if_target_file_exists(
        self,
        other: 'MemoryFilesystemFake',
        target: str,
        overwrite: bool,
    ):
        target_item = other._find_matching_item(target)
        if target_item and not target_item.is_dir() and not overwrite:
            raise FileExistsError(target)

    def _copy_directory(
        self,
        source: str,
        target: str,
        overwrite: bool,
        other: 'MemoryFilesystemFake',
        match: DirectoryStub
    ) -> None:
        match = cast(DirectoryStub, match)
        self._copy_files_recursive(match, source, target, overwrite, other)

    def _copy_files_recursive(
        self,
        directory: DirectoryStub,
        base_path: str,
        target_path: str,
        overwrite: bool,
        other: 'MemoryFilesystemFake'
    ) -> None:
        for item in directory.items:
            item_path = os.path.join(base_path, item.name)
            if not item.is_dir():
                item = cast(FileStub, item)
                target_path = os.path.join(target_path, item.name)
                self._copy_single_file(other, item, target_path, overwrite)
            else:
                item = cast(DirectoryStub, item)
                self._copy_files_recursive(item, item_path, target_path, overwrite, other)

    def _copy_single_file(
        self,
        fs: 'MemoryFilesystemFake',
        file: FileStub,
        path: str,
        overwrite: bool
    ) -> None:
        match = fs._find_matching_item(path)
        if match and not overwrite:
            raise FileExistsError()

        if match and overwrite:
            match = cast(FileStub, match)
            match.content = file.content
        else:
            fs.create_file_stub(path, file.content)

    def delete(self, path: str) -> None:
        if not self.exists(path):
            raise FileNotFoundError(path)

        file = cast(FilesystemItem, self._find_matching_item(path))
        if file.parent:
            file.parent.items.remove(file)

    def exists(self, path: str) -> bool:
        return self._find_matching_item(path) is not None

    def _find_matching_item(self, path: str) -> Optional[FilesystemItem]:
        split_path = path.split(os.path.sep)
        current_item: Optional[FilesystemItem] = self._root

        for name in split_path:
            current_item = cast(FilesystemItem, current_item)
            if current_item.is_dir():
                current_item = cast(DirectoryStub, current_item)
                current_item = self._matching_item_in_dir(current_item, name)

                if not current_item:
                    return None

        return current_item

    def _matching_item_in_dir(
        self,
        current_item: DirectoryStub,
        name: str
    ) -> Optional[FilesystemItem]:
        return next(
            filter(
                lambda d: d.name == name,
                current_item.items
            ), None
        )


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
