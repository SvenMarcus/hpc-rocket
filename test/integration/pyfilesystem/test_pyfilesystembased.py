from io import TextIOWrapper
import os.path
from typing import List, Optional, cast
import unittest
from unittest.mock import MagicMock

import pytest
from test.test_filesystem_abc import FilesystemTest

from fs.memoryfs import MemoryFS

from hpcrocket.core.filesystem import Filesystem
from hpcrocket.pyfilesystem.pyfilesystembased import PyFilesystemBased


# This class name starts with an underscore because pytest tries to collect it as test otherwise
class _TestFilesystemImpl(PyFilesystemBased):
    def __init__(self, fs_mock: MemoryFS, dir: str = "/", home: str = "/") -> None:
        super().__init__(fs_mock, dir, home)


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
        ...

    def exists(self, path: str) -> bool:
        ...

    def openread(self, path: str) -> TextIOWrapper:
        ...


class PyFilesystemBasedTest(FilesystemTest, unittest.TestCase):
    def create_filesystem(self, dir: str = "/") -> Filesystem:
        mem_fs = MemoryFS()
        _ = mem_fs.makedirs(dir, recreate=True)
        return _TestFilesystemImpl(mem_fs, dir, self.home_dir_abs())

    def create_file(self, filesystem: Filesystem, path: str, content: str = "") -> None:
        pyfs = cast(PyFilesystemBased, filesystem)
        path = str(pyfs.current_dir.joinpath(path))
        mem_fs = pyfs.internal_fs
        head, _ = os.path.split(path)
        if head and not mem_fs.exists(head):
            mem_fs.makedirs(head)

        mem_fs.writetext(path, content)

    def create_dir(self, filesystem: Filesystem, directory: str) -> None:
        pyfs = cast(PyFilesystemBased, filesystem)
        directory = str(pyfs.current_dir.joinpath(directory))
        mem_fs = cast(PyFilesystemBased, filesystem).internal_fs
        mem_fs.makedirs(directory)

    def get_file_content(self, filesystem: Filesystem, path: str) -> str:
        pyfs = cast(PyFilesystemBased, filesystem)
        path = str(pyfs.current_dir.joinpath(path))
        mem_fs = pyfs.internal_fs
        return mem_fs.readtext(path)

    def test__when_copying_file_to_other_filesystem__and_parent_dir_exists__should_not_try_to_create_dirs(
        self,
    ) -> None:
        target_parent_dir = "another/folder"

        target_fs = MemoryFS()
        target_fs.makedirs(target_parent_dir)
        target_fs_wrapping_mock = MagicMock(spec=MemoryFS, wraps=target_fs)

        sut = cast(_TestFilesystemImpl, self.create_filesystem())
        self.create_file(sut, self.SOURCE, "")

        complete_path = f"{target_parent_dir}/{self.TARGET}"
        sut.copy(
            self.SOURCE,
            complete_path,
            filesystem=_TestFilesystemImpl(target_fs_wrapping_mock),
        )

        makedirs = cast(MagicMock, target_fs_wrapping_mock.makedirs)
        makedirs.assert_not_called()

    def test__when_copying_to_non_pyfilesystem__should_raise_runtime_error(
        self,
    ) -> None:
        target_fs = NonPyFilesystemBasedFilesystem()

        origin_fs = MemoryFS()
        origin_fs.create(self.SOURCE)
        sut = _TestFilesystemImpl(origin_fs)

        with pytest.raises(RuntimeError):
            sut.copy(self.SOURCE, self.TARGET, filesystem=target_fs)
