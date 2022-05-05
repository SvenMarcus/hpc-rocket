from io import TextIOWrapper
import os.path
from typing import List, Optional, cast
import unittest
from unittest.mock import MagicMock

import pytest
from test.test_filesystem_abc import FilesystemTest

import fs.base
from fs.memoryfs import MemoryFS

from hpcrocket.core.filesystem import Filesystem
from hpcrocket.pyfilesystem.pyfilesystembased import PyFilesystemBased


# This class name starts with an underscore because pytest tries to collect it as test otherwise
class _TestFilesystemImpl(PyFilesystemBased):
    def __init__(self, fs_mock: MemoryFS) -> None:
        super().__init__()
        self._internal_fs = fs_mock

    @property
    def internal_fs(self) -> fs.base.FS:
        return self._internal_fs


class NonPyFilesystemBasedFilesystem(Filesystem):
    def __init__(self) -> None:
        pass

    def glob(self, pattern: str) -> List[str]:
        return []

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

    def exists(self, path: str) -> bool:
        pass


    def openread(self, path: str) -> TextIOWrapper:
        pass


class PyFilesystemBasedTest(FilesystemTest, unittest.TestCase):
    def create_filesystem(self) -> Filesystem:
        return _TestFilesystemImpl(MemoryFS())

    def create_file(self, filesystem: Filesystem, path: str, content: str = "") -> None:
        mem_fs = cast(PyFilesystemBased, filesystem).internal_fs
        head, _ = os.path.split(path)
        if head and not mem_fs.exists(head):
            mem_fs.makedirs(head)

        mem_fs.writetext(path, content)

    def create_dir(self, filesystem: Filesystem, directory: str) -> None:
        mem_fs = cast(PyFilesystemBased, filesystem).internal_fs
        mem_fs.makedirs(directory)

    def get_file_content(self, filesystem: Filesystem, path: str) -> str:
        mem_fs = cast(PyFilesystemBased, filesystem).internal_fs
        return mem_fs.readtext(path)

    def test__when_copying_file_to_other_filesystem__and_parent_dir_exists__should_not_try_to_create_dirs(
        self,
    ) -> None:
        target_parent_dir = "another/folder"

        target_fs = MemoryFS()
        target_fs.makedirs(target_parent_dir)
        target_fs_wrapping_mock = MagicMock(spec=MemoryFS, wraps=target_fs)

        sut = cast(_TestFilesystemImpl, self.create_filesystem())
        origin_fs = sut.internal_fs
        origin_fs.create(self.SOURCE)

        complete_path = f"{target_parent_dir}/{self.TARGET}"
        sut.copy(
            self.SOURCE,
            complete_path,
            filesystem=_TestFilesystemImpl(target_fs_wrapping_mock),
        )

        target_fs_wrapping_mock.makedirs.assert_not_called()

    def test__when_copying_to_non_pyfilesystem__should_raise_runtime_error(
        self,
    ) -> None:
        target_fs = NonPyFilesystemBasedFilesystem()

        origin_fs = MemoryFS()
        origin_fs.create(self.SOURCE)
        sut = _TestFilesystemImpl(origin_fs)

        with pytest.raises(RuntimeError):
            sut.copy(self.SOURCE, self.TARGET, filesystem=target_fs)
