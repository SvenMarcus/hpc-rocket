import fnmatch
from io import TextIOWrapper
import io
import os.path
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional, TextIO, Tuple, Union, cast
from unittest.mock import DEFAULT, Mock, patch

from hpcrocket.core.filesystem import Filesystem, FilesystemFactory


class DummyFilesystemFactory(FilesystemFactory):
    def create_local_filesystem(self) -> "Filesystem":
        return DummyFilesystem()

    def create_ssh_filesystem(self) -> "Filesystem":
        return DummyFilesystem()


class DummyFilesystem(Filesystem):
    def glob(self, pattern: str) -> List[str]:
        return []

    def exists(self, path: str) -> bool:
        return False

    def copy(
        self,
        source: str,
        target: str,
        overwrite: bool = False,
        filesystem: Optional["Filesystem"] = None,
    ) -> None:
        pass

    def delete(self, path: str) -> None:
        pass

    def openread(self, path: str) -> TextIOWrapper:
        return TextIOWrapper(io.BytesIO())


class MemoryFilesystemFactoryStub(FilesystemFactory):
    def __init__(
        self,
        local_fs: Optional["MemoryFilesystemFake"] = None,
        ssh_fs: Optional["MemoryFilesystemFake"] = None,
    ) -> None:
        self.local_filesystem = local_fs or MemoryFilesystemFake()
        self.ssh_filesystem = ssh_fs or MemoryFilesystemFake()

    def create_local_filesystem(self) -> "Filesystem":
        return self.local_filesystem

    def create_ssh_filesystem(self) -> "Filesystem":
        return self.ssh_filesystem

    def create_local_files(self, *files: str) -> None:
        self._create_files_for_fs(self.local_filesystem, files)

    def create_remote_files(self, *files: str) -> None:
        self._create_files_for_fs(self.ssh_filesystem, files)

    def _create_files_for_fs(
        self, fs: "MemoryFilesystemFake", files: Tuple[str, ...]
    ) -> None:
        for file in files:
            fs.create_file_stub(file, "")


@dataclass
class FileStub:
    path: str
    content: str = ""

    def is_dir(self) -> bool:
        return False


@dataclass
class DirectoryStub:
    path: str

    def is_dir(self) -> bool:
        return True


FilesystemItem = Union[FileStub, DirectoryStub]


class MemoryFilesystemFake(Filesystem):
    def __init__(self, files: List[str] = []) -> None:
        self._filesystem: List[FilesystemItem] = []
        for file in files:
            self.create_file_stub(file, "")

    def create_file_stub(self, path: str, content: str) -> None:
        parent, _ = os.path.split(path)
        if parent and not self.exists(parent):
            self.create_dir_stub(parent)

        self._filesystem.append(FileStub(path, content))

    def create_dir_stub(self, path: str) -> None:
        parent, _ = os.path.split(path)
        while parent and not self.exists(parent):
            self.create_dir_stub(parent)
            parent, _ = os.path.split(path)
        self._filesystem.append(DirectoryStub(path))

    def get_content_of_file_stub(self, path: str) -> str:
        file = next(filter(lambda f: f.path == path, self._filesystem))
        return cast(FileStub, file).content

    def glob(self, pattern: str) -> List[str]:
        pattern = pattern.replace("**/", "*")
        return [
            file.path
            for file in self._filesystem
            if fnmatch.fnmatch(file.path, pattern)
        ]

    def copy(
        self,
        source: str,
        target: str,
        overwrite: bool = False,
        filesystem: Optional["Filesystem"] = None,
    ) -> None:
        assert filesystem is None or isinstance(
            filesystem, (MemoryFilesystemFake, Mock)
        )
        other = cast(MemoryFilesystemFake, filesystem) or self
        self._raise_if_target_file_exists(other, target, overwrite)
        self._perform_copy(other, source, target, overwrite)

    def delete(self, path: str) -> None:
        items = self._get_matching_items(path)

        if not items:
            raise FileNotFoundError(path)

        for item in items:
            self._filesystem.remove(item)

    def openread(self, path: str) -> TextIOWrapper:
        file = self._find_matching_item(path)
        if file is None or file.is_dir():
            raise FileNotFoundError(path)

        file = cast(FileStub, file)
        content_as_bytes = io.BytesIO()
        content_as_bytes.write(file.content.encode())
        content_as_bytes.seek(0, 0)
        return TextIOWrapper(content_as_bytes)

    def _get_items_by_glob(self, pattern: str) -> List[FilesystemItem]:
        pattern = pattern.replace("**/", "*")
        return [
            file for file in self._filesystem if fnmatch.fnmatch(file.path, pattern)
        ]

    def _perform_copy(
        self, other: "MemoryFilesystemFake", source: str, target: str, overwrite: bool
    ) -> None:
        matches = self._get_matching_items(source)

        if not matches:
            raise FileNotFoundError(source)

        for match in matches:
            if match.is_dir():
                self._copy_directory(source, target, other)
            else:
                match = cast(FileStub, match)
                self._copy_single_file(other, match, source, target, overwrite)

    def _raise_if_target_file_exists(
        self,
        other: "MemoryFilesystemFake",
        target: str,
        overwrite: bool,
    ) -> None:
        if overwrite:
            return

        target_item = other._find_matching_item(target)
        if target_item and not target_item.is_dir():
            raise FileExistsError(target)

    def _copy_directory(
        self,
        source: str,
        target: str,
        other: "MemoryFilesystemFake",
    ) -> None:
        children = self._find_children(source)
        for child in children:
            if not child.is_dir():
                basename = os.path.basename(child.path)
                target_path = os.path.join(target, basename)
                child = cast(FileStub, child)
                other.create_file_stub(target_path, child.content)

    def _copy_single_file(
        self,
        fs: "MemoryFilesystemFake",
        file_to_copy: FileStub,
        source: str,
        target: str,
        overwrite: bool,
    ) -> None:
        existing_file = cast(FileStub, fs._find_matching_item(target))
        if existing_file and overwrite:
            self._overwrite_file_content(file_to_copy, existing_file)
            return

        self._create_file_copy(fs, file_to_copy, source, target, overwrite)

    def _overwrite_file_content(
        self, file_to_copy: FileStub, existing_file: FileStub
    ) -> None:
        existing_file.content = file_to_copy.content

    def _create_file_copy(
        self,
        fs: "MemoryFilesystemFake",
        file: FileStub,
        source: str,
        target: str,
        overwrite: bool,
    ) -> None:
        target_path = self._final_target_path(file, source, target)
        target_path = self._append_filename_if_dir(file, target_path)
        self._raise_if_target_file_exists(fs, target_path, overwrite)

        fs.create_file_stub(target_path, file.content)

    def _append_filename_if_dir(self, file: FileStub, target_path: str) -> str:
        if target_path.endswith(os.path.sep):
            target_path = os.path.join(target_path, os.path.basename(file.path))

        return target_path

    def _final_target_path(self, file: FileStub, source: str, target: str) -> str:
        if not "*" in source:
            return target

        base = os.path.basename(file.path)
        subpath = self._minimal_matching_subpath(file, source)
        if subpath != file.path:
            base = file.path[len(subpath) :].strip("/")

        return os.path.join(target, base)

    def _minimal_matching_subpath(self, file: FileStub, pattern: str) -> str:
        walked_path = ""
        for walked_path in self._walk_path(file.path):
            path_matches = fnmatch.fnmatch(walked_path, pattern)
            if path_matches:
                break

        return walked_path

    def _walk_path(self, path: str) -> Generator[str, None, None]:
        def is_last_component(component: str, all_components: List[str]) -> bool:
            return component != all_components[-1]

        path_components = path.split(os.path.sep)
        walked_path = ""
        for component in path_components:
            walked_path += component
            if is_last_component(component, path_components):
                walked_path += os.path.sep

            yield walked_path

    def _get_matching_items(self, path: str) -> List[FilesystemItem]:
        if "*" in path:
            items = self._get_items_by_glob(path)
        else:
            item = self._find_matching_item(path)
            items = [item] if item else self._find_children(path)
        return items

    def _find_children(self, path: str) -> List[FilesystemItem]:
        return self._get_items_by_glob(os.path.join(path, "*"))

    def exists(self, path: str) -> bool:
        return self._find_matching_item(path) is not None

    def _find_matching_item(self, path: str) -> Optional[FilesystemItem]:
        return next(
            filter(lambda f: f.path == path if f else False, self._filesystem), None
        )


@contextmanager
def sshfs_with_connection_fake(sshclient_mock: Mock) -> Generator[Mock, None, None]:
    def emulate_connect(*args: Any, **kwargs: str) -> Any:
        map_to_paramiko_arguments(kwargs)
        sshclient_mock.connect(*args, **kwargs)
        return DEFAULT

    def map_to_paramiko_arguments(kwargs: Dict[str, str]) -> None:
        kwargs["hostname"] = kwargs["host"]
        kwargs["username"] = kwargs["user"]
        kwargs["password"] = kwargs["passwd"]
        kwargs["pkey"] = kwargs["pkey"]

    patcher = patch("hpcrocket.ssh.chmodsshfs.PermissionChangingSSHFSDecorator")
    patched = patcher.start()
    patched.side_effect = emulate_connect
    yield patched

    patcher.stop()
