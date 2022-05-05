from io import TextIOWrapper
import os.path
from abc import ABC, abstractmethod

import pytest

from hpcrocket.core.filesystem import Filesystem


class FilesystemTest(ABC):

    SOURCE = "file.txt"
    TARGET = "copy.txt"

    @abstractmethod
    def create_filesystem(self) -> Filesystem:
        pass

    @abstractmethod
    def create_file(self, filesystem: Filesystem, path: str, content: str = "") -> None:
        pass

    @abstractmethod
    def create_dir(self, filesystem: Filesystem, directory: str) -> None:
        pass

    @abstractmethod
    def get_file_content(self, filesystem: Filesystem, path: str) -> str:
        pass

    def assert_file_content_equals(
        self, filesystem: Filesystem, path: str, content: str
    ) -> None:
        assert self.get_file_content(filesystem, path) == content

    def test__when_copying_file__should_copy_to_target_path(self) -> None:
        sut = self.create_filesystem()

        content = "content"
        self.create_file(sut, self.SOURCE, content)

        sut.copy(self.SOURCE, self.TARGET)

        assert sut.exists(self.TARGET)
        self.assert_file_content_equals(sut, self.TARGET, content)

    def test__when_copying_file_to_other_dir__it_copies_file_to_target_path(
        self,
    ) -> None:
        sut = self.create_filesystem()
        self.create_file(sut, "dir/sub/file.txt")

        sut.copy("dir/sub/file.txt", "other/")

        assert sut.exists("other/file.txt")

    def test__when_copying_file__but_parent_dir_missing__should_create_missing_dirs(
        self,
    ) -> None:
        complete_path = "another/folder/" + self.TARGET

        sut = self.create_filesystem()
        self.create_file(sut, self.SOURCE)

        sut.copy(self.SOURCE, complete_path)

        assert sut.exists(complete_path)

    def test__a__when_copying_directory__should_copy_entire_directory(self) -> None:
        src_dir = "mydir"
        copy_dir = "copydir"
        sut = self.create_filesystem()
        self.create_file(sut, os.path.join(src_dir, self.SOURCE))

        sut.copy(src_dir, copy_dir)

        assert sut.exists(f"{copy_dir}/{self.SOURCE}")

    def test__when_copying_file_to_other_filesystem__should_call_copy_file(
        self,
    ) -> None:
        target_fs = self.create_filesystem()
        sut = self.create_filesystem()
        self.create_file(sut, self.SOURCE, "content")

        sut.copy(self.SOURCE, self.TARGET, filesystem=target_fs)

        assert target_fs.exists(self.TARGET)
        self.assert_file_content_equals(target_fs, self.TARGET, "content")

    def test__when_copying_file_to_dir_on_other_filesystem__it_copies_file_to_target_path(
        self,
    ) -> None:
        target_fs = self.create_filesystem()
        sut = self.create_filesystem()
        self.create_file(sut, "dir/sub/file.txt")

        sut.copy("dir/sub/file.txt", "other/", filesystem=target_fs)

        assert target_fs.exists("other/file.txt")

    def test__when_copying_file_to_other_filesystem__but_parent_dir_missing__should_create_missing_dirs(
        self,
    ) -> None:
        complete_path = "another/folder/" + self.TARGET

        target_fs = self.create_filesystem()
        sut = self.create_filesystem()
        self.create_file(sut, self.SOURCE)

        sut.copy(self.SOURCE, complete_path, filesystem=target_fs)

        assert target_fs.exists(complete_path)

    def test__when_copying__but_source_does_not_exist__should_raise_file_not_found_error(
        self,
    ) -> None:
        sut = self.create_filesystem()

        with pytest.raises(FileNotFoundError):
            sut.copy(self.SOURCE, self.TARGET)

    def test__when_copying__but_file_exists__should_raise_file_exists_error(
        self,
    ) -> None:
        sut = self.create_filesystem()
        self.create_file(sut, self.SOURCE)
        self.create_file(sut, self.TARGET)

        with pytest.raises(FileExistsError):
            sut.copy(self.SOURCE, self.TARGET)

    def test__when_copying_to_existing_path_with_overwrite_enabled__should_copy_file(
        self,
    ) -> None:
        sut = self.create_filesystem()
        self.create_file(sut, self.SOURCE, "new content")
        self.create_file(sut, self.TARGET, "old content")

        sut.copy(self.SOURCE, self.TARGET, overwrite=True)

        self.assert_file_content_equals(sut, self.TARGET, "new content")

    def test__when_copying_to_other_filesystem__but_file_exists__should_raise_file_exists_error(
        self,
    ) -> None:
        sut = self.create_filesystem()
        self.create_file(sut, self.SOURCE)

        target_fs = self.create_filesystem()
        self.create_file(target_fs, self.TARGET)

        with pytest.raises(FileExistsError):
            sut.copy(self.SOURCE, self.TARGET, filesystem=target_fs)

    def test__when_copying_to_existing_path_on_other_filesystem_with_overwrite_enabled__should_copy_file(
        self,
    ) -> None:
        target_fs = self.create_filesystem()
        self.create_file(target_fs, self.TARGET, "old content")

        sut = self.create_filesystem()
        self.create_file(sut, self.SOURCE, "new content")

        sut.copy(self.SOURCE, self.TARGET, filesystem=target_fs, overwrite=True)

        self.assert_file_content_equals(target_fs, self.TARGET, "new content")

    def test__when_copying_directory_to_other_filesystem__should_copy_dir(self) -> None:
        source_dir = "mydir"
        target_dir = "copydir"

        sut = self.create_filesystem()
        self.create_file(sut, os.path.join(source_dir, self.SOURCE), "content")

        target_fs = self.create_filesystem()
        sut.copy(source_dir, target_dir, filesystem=target_fs)

        complete_path = f"{target_dir}/{self.SOURCE}"
        assert target_fs.exists(complete_path)
        self.assert_file_content_equals(target_fs, complete_path, "content")

    def test__when_copying_directory__but_directory_exists__should_copy_into_existing_directory(
        self,
    ) -> None:
        source_dir = "sourcedir"
        target_dir = "targetdir"

        sut = self.create_filesystem()
        self.create_dir(sut, target_dir)
        self.create_file(sut, os.path.join(source_dir, self.SOURCE))

        sut.copy("sourcedir", "targetdir")

        complete_path = f"targetdir/{self.SOURCE}"
        assert sut.exists(complete_path)

    def test__when_deleting_file__should_remove_file_from_fs(self) -> None:
        sut = self.create_filesystem()
        self.create_file(sut, self.SOURCE)

        sut.delete(self.SOURCE)

        assert not sut.exists(self.SOURCE)

    def test__when_deleting_directory__should_delete_directory_with_contents(
        self,
    ) -> None:
        dir_path = "mydir"
        sut = self.create_filesystem()
        self.create_file(sut, os.path.join(dir_path, self.SOURCE))

        sut.delete(dir_path)

        assert not sut.exists(dir_path)

    def test__when_deleting_file_but_does_not_exist__should_raise_file_not_found_error(
        self,
    ) -> None:
        sut = self.create_filesystem()

        with pytest.raises(FileNotFoundError):
            sut.delete(self.SOURCE)

    def test__when_copying_files_with_glob_pattern__it_copies_matching_files(
        self,
    ) -> None:
        sut = self.create_filesystem()
        self.create_file(sut, "hello.txt")
        self.create_file(sut, "world.txt")
        self.create_file(sut, "nope.gif")

        sut.copy("*.txt", "newdir/")

        assert sut.exists("newdir/hello.txt")
        assert sut.exists("newdir/world.txt")
        assert not sut.exists("newdir/nope.gif")

    def test__when_copying_files_with_glob_pattern__but_file_exists__it_raises_an_error(
        self,
    ) -> None:
        sut = self.create_filesystem()
        self.create_file(sut, "hello.txt")
        self.create_file(sut, "sub_dir/hello.txt")

        with pytest.raises(FileExistsError):
            sut.copy("*.txt", "sub_dir")

    def test__when_copying_dirs_with_glob_patterns__it_copies_matching_dirs_with_content(
        self,
    ) -> None:
        sut = self.create_filesystem()
        self.create_file(sut, "dir/sub/first/file.txt")
        self.create_file(sut, "dir/sub/second/another.txt")

        sut.copy("dir/sub/*", "otherdir/")

        assert sut.exists("otherdir/first/file.txt")
        assert sut.exists("otherdir/second/another.txt")

    def test__when_copying_nested_files_with_glob_pattern_to_dir__it_copies_matching_files_into_target_dir(
        self,
    ) -> None:
        sut = self.create_filesystem()
        other = self.create_filesystem()
        self.create_file(sut, "dir/sub/sub2/first.txt")
        self.create_file(sut, "dir/sub/sub2/second.txt")

        sut.copy("dir/sub/sub2/*.txt", "otherdir/", filesystem=other)

        assert other.exists("otherdir/first.txt")
        assert other.exists("otherdir/second.txt")

    def test__when_deleting_with_glob_pattern__it_deletes_matching_files(self) -> None:
        sut = self.create_filesystem()
        self.create_file(sut, "hello.txt")
        self.create_file(sut, "world.txt")
        self.create_file(sut, "nope.gif")

        sut.delete("*.txt")

        assert not sut.exists("hello.txt")
        assert not sut.exists("world.txt")
        assert sut.exists("nope.gif")

    def test__when_globbing__it_returns_matching_paths(self) -> None:
        sut = self.create_filesystem()
        self.create_file(sut, "hello.txt")
        self.create_file(sut, "world.txt")
        self.create_file(sut, "nope.gif")
        self.create_file(sut, "sub/nomatch.gif")
        self.create_file(sut, "sub/match.txt")
        self.create_file(sut, "sub/dir/match.txt")

        actual = sut.glob("**/*.txt")

        assert actual == [
            "hello.txt",
            "world.txt",
            "sub/match.txt",
            "sub/dir/match.txt",
        ]

    def test__when_reading_file__returns_text_io_wrapper(self) -> None:
        file_content = "the content"

        sut = self.create_filesystem()
        self.create_file(sut, "myfile.txt", file_content)
        with sut.openread("myfile.txt") as file:
            assert file.read() == file_content

    def test__when_reading_nonexisting_file__raises_file_not_found(self) -> None:
        sut = self.create_filesystem()

        with pytest.raises(FileNotFoundError):
            sut.openread("nonexisting")

    def test__when_reading_directory__raises_file_not_found(self) -> None:
        sut = self.create_filesystem()
        self.create_dir(sut, "mydir")

        with pytest.raises(FileNotFoundError):
            sut.openread("mydir")