from typing import cast
import unittest
from hpcrocket.core.filesystem import Filesystem

from test.test_filesystem_abc import FilesystemTest
from test.testdoubles.filesystem import MemoryFilesystemFake


class TestMemoryFilesystem(FilesystemTest, unittest.TestCase):

    def create_filesystem(self) -> Filesystem:
        return MemoryFilesystemFake()

    def create_file(self, filesystem: Filesystem, path: str, content: str = "") -> None:
        fs = cast(MemoryFilesystemFake, filesystem)
        fs.create_file_stub(path, content)

    def create_dir(self, filesystem: Filesystem, directory: str) -> None:
        fs = cast(MemoryFilesystemFake, filesystem)
        fs.create_dir_stub(directory)

    def get_file_content(self, filesystem: Filesystem, path: str) -> str:
        fs = cast(MemoryFilesystemFake, filesystem)
        return fs.get_content_of_file_stub(path)

    def test__when_creating_file__it_exists(self):
        path = "file.txt"
        sut = self.create_filesystem()
        self.create_file(sut, path, "content")

        assert sut.exists(path)
        self.assert_file_content_equals(sut, path, "content")

    def test__when_searching_non_existing_file__exists_is_false(self):
        path = "file.txt"
        sut = self.create_filesystem()

        assert sut.exists(path) is False

    def test__when_creating_nested_file__it_exists(self):
        path = "sub/dir/file.txt"
        sut = self.create_filesystem()
        self.create_file(sut, path)

        assert sut.exists(path)

    def test__when_pathname_only_partial_match__exists_is_false(self):
        path = "sub/dir/file.txt"
        search_path = "sub/dir/other.txt"
        sut = self.create_filesystem()
        self.create_file(sut, path)

        assert sut.exists(search_path) is False