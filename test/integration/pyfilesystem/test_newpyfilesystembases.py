import os.path
from typing import cast
import unittest
from test.test_filesystem_abc import FilesystemTest

import fs.base
from fs.memoryfs import MemoryFS

from hpcrocket.core.filesystem import Filesystem
from hpcrocket.pyfilesystem.pyfilesystembased import PyFilesystemBased


# This class name starts with an underscore because pytest tries to collect it as test otherwise
class _TestFilesystemImpl(PyFilesystemBased):

    def __init__(self, fs_mock) -> None:
        super().__init__()
        self._internal_fs = fs_mock

    @property
    def internal_fs(self) -> fs.base.FS:
        return self._internal_fs


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
