from unittest.mock import MagicMock

import fs
import fs.base
import pytest
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


class NonPyFilesystemBasedFilesystem(Filesystem):

    def __init__(self) -> None:
        pass

    def copy(self, source: str, target: str, overwrite: bool = False, filesystem: 'Filesystem' = None) -> None:
        pass

    def delete(self, path: str) -> None:
        pass

    def exists(self, path: str) -> bool:
        pass


SOURCE = "file.txt"
TARGET = "copy.txt"


def write_file_with_content(mem_fs: fs.base.FS, path: str, content: str = "") -> None:
    with mem_fs.open(path, "w") as src_file:
        src_file.write(content)


def assert_file_content_equals(mem_fs: fs.base.FS, path: str, content: str):
    with mem_fs.open(path) as target_file:
        line = target_file.readline()
        assert line == content


def test__when_copying_file__should_copy_to_target_path():
    mem_fs = MemoryFS()
    content = "content"
    write_file_with_content(mem_fs, SOURCE, content)

    sut = _TestFilesystemImpl(mem_fs)

    sut.copy(SOURCE, TARGET)

    assert mem_fs.exists(TARGET)
    assert_file_content_equals(mem_fs, TARGET, content)


def test__when_copying_file__but_parent_dir_missing__should_create_missing_dirs():
    complete_path = "another/folder/" + TARGET

    mem_fs = MemoryFS()
    write_file_with_content(mem_fs, SOURCE)

    sut = _TestFilesystemImpl(mem_fs)
    sut.copy(SOURCE, complete_path)

    assert mem_fs.exists(complete_path)


def test__a__when_copying_directory__should_copy_entire_directory():
    src_dir = "mydir"
    copy_dir = "copydir"
    mem_fs = MemoryFS()
    sub_fs = mem_fs.makedir(src_dir)
    write_file_with_content(sub_fs, SOURCE)

    sut = _TestFilesystemImpl(mem_fs)

    sut.copy(src_dir, copy_dir)

    assert mem_fs.exists(f"{copy_dir}/{SOURCE}")


def test__when_copying_file_to_other_filesystem__should_call_copy_file():
    target_fs = MemoryFS()
    origin_fs = MemoryFS()
    write_file_with_content(origin_fs, SOURCE, "content")

    sut = _TestFilesystemImpl(origin_fs)

    sut.copy(SOURCE, TARGET, filesystem=_TestFilesystemImpl(target_fs))

    assert target_fs.exists(TARGET)
    assert_file_content_equals(target_fs, TARGET, "content")


def test__when_copying_file_to_other_filesystem__but_parent_dir_missing__should_create_missing_dirs():
    complete_path = "another/folder/" + TARGET

    target_fs = MemoryFS()
    origin_fs = MemoryFS()
    write_file_with_content(origin_fs, SOURCE)

    sut = _TestFilesystemImpl(origin_fs)

    sut.copy(SOURCE, complete_path, filesystem=_TestFilesystemImpl(target_fs))

    assert target_fs.exists(complete_path)


def test__when_copying_file_to_other_filesystem__and_parent_dir_exists__should_not_try_to_create_dirs():
    target_parent_dir = "another/folder"

    target_fs = MemoryFS()
    target_fs.makedirs(target_parent_dir)
    target_fs_wrapping_mock = MagicMock(spec=MemoryFS, wraps=target_fs)

    origin_fs = MemoryFS()
    origin_fs.create(SOURCE)

    sut = _TestFilesystemImpl(origin_fs)

    complete_path = f"{target_parent_dir}/{TARGET}"
    sut.copy(SOURCE, complete_path, filesystem=_TestFilesystemImpl(target_fs_wrapping_mock))

    target_fs_wrapping_mock.makedirs.assert_not_called()


def test__when_copying__but_source_does_not_exist__should_raise_file_not_found_error():
    sut = _TestFilesystemImpl(MemoryFS())

    with pytest.raises(FileNotFoundError):
        sut.copy(SOURCE, TARGET)


def test__when_copying__but_file_exists__should_raise_file_exists_error():
    mem_fs = MemoryFS()
    mem_fs.create(SOURCE)
    mem_fs.create(TARGET)

    sut = _TestFilesystemImpl(mem_fs)

    with pytest.raises(FileExistsError):
        sut.copy(SOURCE, TARGET)


def test__when_copying_to_existing_path_with_overwrite_enabled__should_copy_file():
    mem_fs = MemoryFS()
    write_file_with_content(mem_fs, SOURCE, "new content")
    write_file_with_content(mem_fs, TARGET, "old content")

    sut = _TestFilesystemImpl(mem_fs)

    sut.copy(SOURCE, TARGET, overwrite=True)

    assert_file_content_equals(mem_fs, TARGET, "new content")


def test__when_copying_to_other_filesystem__but_file_exists__should_raise_file_exists_error():
    origin_fs = MemoryFS()
    origin_fs.create(SOURCE)

    target_fs = MemoryFS()
    target_fs.create(TARGET)

    sut = _TestFilesystemImpl(origin_fs)

    with pytest.raises(FileExistsError):
        sut.copy(SOURCE, TARGET, filesystem=_TestFilesystemImpl(target_fs))


def test__when_copying_to_existing_path_on_other_filesystem_with_overwrite_enabled__should_copy_file():
    target_fs = MemoryFS()
    write_file_with_content(target_fs, TARGET, "old content")

    origin_fs = MemoryFS()
    write_file_with_content(origin_fs, SOURCE, "new content")
    sut = _TestFilesystemImpl(origin_fs)

    sut.copy(SOURCE, TARGET, filesystem=_TestFilesystemImpl(
        target_fs), overwrite=True)

    assert_file_content_equals(target_fs, TARGET, "new content")


def test__when_copying_directory_to_other_filesystem__should_copy_dir():
    source_dir = "mydir"
    target_dir = "copydir"

    origin_fs = MemoryFS()
    sub_fs = origin_fs.makedir(source_dir)
    write_file_with_content(sub_fs, SOURCE, "content")

    sut = _TestFilesystemImpl(origin_fs)

    target_fs = MemoryFS()
    sut.copy(source_dir, target_dir, filesystem=_TestFilesystemImpl(target_fs))

    complete_path = f"{target_dir}/{SOURCE}"
    assert target_fs.exists(complete_path)
    assert_file_content_equals(target_fs, complete_path, "content")


def test__when_copying_directory__but_directory_exists__should_copy_into_existing_directory():
    origin_fs = MemoryFS()
    sub_fs = origin_fs.makedir("sourcedir")
    origin_fs.makedir("targetdir")
    write_file_with_content(sub_fs, SOURCE, "content")

    sut = _TestFilesystemImpl(origin_fs)
    sut.copy("sourcedir", "targetdir")

    complete_path = f"targetdir/{SOURCE}"
    assert origin_fs.exists(complete_path)


def test__when_copying_to_non_pyfilesystem__should_raise_runtime_error():
    target_fs = NonPyFilesystemBasedFilesystem()

    origin_fs = MemoryFS()
    origin_fs.create(SOURCE)
    sut = _TestFilesystemImpl(origin_fs)

    with pytest.raises(RuntimeError):
        sut.copy(SOURCE, TARGET, filesystem=target_fs)


def test__when_deleting_file__should_remove_file_from_fs():
    fs = MemoryFS()
    fs.create(SOURCE)

    sut = _TestFilesystemImpl(fs)

    sut.delete(SOURCE)

    assert not fs.exists(SOURCE)


def test__when_deleting_directory__should_call_fs_removetree():
    dir_path = "mydir"
    fs = MemoryFS()
    sub_fs = fs.makedir(dir_path)
    sub_fs.create(SOURCE)

    sut = _TestFilesystemImpl(fs)

    sut.delete(dir_path)

    assert not fs.exists(dir_path)


def test__when_deleting_file_but_does_not_exist__should_raise_file_not_found_error():
    sut = _TestFilesystemImpl(MemoryFS())

    with pytest.raises(FileNotFoundError):
        sut.delete(SOURCE)
