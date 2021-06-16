from unittest.mock import MagicMock, Mock, patch

import fs.base
import pytest
from fs import ResourceType
from ssh_slurm_runner.filesystem import Filesystem
from ssh_slurm_runner.sshfilesystem import PyFilesystemBased, SSHFilesystem


class MockFilesystem(PyFilesystemBased):

    def __init__(self) -> None:
        self._internal_fs = MagicMock(spec="fs.base.FS").return_value
        self.existing_files = set()

    def internal_fs(self) -> fs.base.FS:
        return self._internal_fs

    def copy(self, source: str, target: str, filesystem: 'Filesystem') -> None:
        pass

    def delete(self, path: str) -> None:
        pass

    def exists(self, path: str) -> None:
        return path in self.existing_files


class NonPyFilesystemBasedFilesystem(Filesystem):

    def __init__(self) -> None:
        pass

    def copy(self, source: str, target: str, filesystem: 'Filesystem') -> None:
        pass

    def delete(self, path: str) -> None:
        pass

    def exists(self, path: str) -> None:
        pass


SOURCE = "~/file.txt"
TARGET = "~/another/folder/copy.txt"


@pytest.fixture
def sshfs_type_mock():
    # The mocking does not work for some reason if only one of the paths is mocked
    patcher1 = patch("fs.sshfs.sshfs.SSHFS")
    patcher2 = patch("fs.sshfs.SSHFS")
    patcher1.start()
    mock = patcher2.start()
    mock.return_value.configure_mock(exists=exists_with_files(SOURCE))

    yield mock

    patcher1.stop()
    patcher2.stop()


@pytest.fixture
def copy_file():
    patcher = patch("fs.copy.copy_file")

    yield patcher.start()

    patcher.stop()


def test__given_ssh_client__when_creating_new_instance__should_create_sshfs_with_connection_data(sshfs_type_mock):
    sut = SSHFilesystem('user', 'host', 'privatekey')

    sshfs_type_mock.assert_called_with('host', user='user', pkey='privatekey')


def test__when_copying_file__should_call_copy_on_sshfs(sshfs_type_mock):
    sut = SSHFilesystem('user', 'host', 'privatekey')

    sut.copy(SOURCE, TARGET)

    sshfs_mock = sshfs_type_mock.return_value
    sshfs_mock.copy.assert_called_with(SOURCE, TARGET)

def test__when_copying_directory__should_call_copydir_on_sshfs(sshfs_type_mock):
    src_dir = "~/mydir"
    copy_dir = "~/copydir"
    sshfs_mock = sshfs_mock_copy_expecting_directory(sshfs_type_mock, src_dir)

    sut = SSHFilesystem('user', 'host', 'privatekey')

    sut.copy(src_dir, copy_dir)

    sshfs_mock.copydir.assert_called_with(src_dir, copy_dir, create=True)

def test__when_copying_file_to_other_filesystem__should_call_copy_file(sshfs_type_mock, copy_file):
    fs_mock = MockFilesystem()
    sut = SSHFilesystem('user', 'host', 'privatekey')

    sut.copy(SOURCE, TARGET, filesystem=fs_mock)

    sshfs_mock = sshfs_type_mock.return_value
    copy_file.assert_called_with(
        sshfs_mock, SOURCE, fs_mock.internal_fs, TARGET)


def test__when_copying__but_source_does_not_exist__should_raise_file_not_found_error(sshfs_type_mock):
    sshfs_type_mock.return_value.configure_mock(exists=exists_with_files())
    sut = SSHFilesystem('user', 'host', 'privatekey')

    with pytest.raises(FileNotFoundError):
        sut.copy(SOURCE, TARGET)


def test__when_copying__but_file_exists__should_raise_file_exists_error(sshfs_type_mock):
    sshfs_type_mock.return_value.configure_mock(
        exists=exists_with_files(SOURCE, TARGET))

    sut = SSHFilesystem('user', 'host', 'privatekey')

    with pytest.raises(FileExistsError):
        sut.copy(SOURCE, TARGET)


def test__when_copying_to_other_filesystem__but_file_exists__should_raise_file_exists_error(sshfs_type_mock, copy_file):
    fs_mock = MockFilesystem()
    fs_mock.existing_files.add(TARGET)
    sut = SSHFilesystem('user', 'host', 'privatekey')

    with pytest.raises(FileExistsError):
        sut.copy(SOURCE, TARGET, filesystem=fs_mock)


def test__when_copying_to_non_pyfilesystem__should_raise_runtime_error(sshfs_type_mock):
    fs_mock = NonPyFilesystemBasedFilesystem()
    sut = SSHFilesystem('user', 'host', 'privatekey')

    with pytest.raises(RuntimeError):
        sut.copy(SOURCE, TARGET, filesystem=fs_mock)


def test__when_deleting_file__should_call_sshfs_remove(sshfs_type_mock):
    sut = SSHFilesystem('user', 'host', 'privatekey')

    sut.delete(SOURCE)

    sshfs_mock = sshfs_type_mock.return_value
    sshfs_mock.remove.assert_called_with(SOURCE)


def test__when_deleting_directory__should_call_sshfs_removetree(sshfs_type_mock):
    dir_path = "~/mydir"
    sshfs_mock = sshfs_mock_remove_expecting_directory(sshfs_type_mock, dir_path)

    sut = SSHFilesystem('user', 'host', 'privatekey')

    sut.delete(dir_path)

    sshfs_mock.removetree.assert_called_with(dir_path)


def test__when_deleting_file_but_does_not_exist__should_raise_file_not_found_error(sshfs_type_mock):
    sshfs_type_mock.return_value.configure_mock(exists=exists_with_files())
    sut = SSHFilesystem('user', 'host', 'privatekey')

    with pytest.raises(FileNotFoundError):
        sut.delete(SOURCE)


def sshfs_mock_copy_expecting_directory(sshfs_type_mock, src_dir):
    sshfs_mock = sshfs_type_mock.return_value

    def copy(src, dst):
        import fs.errors
        raise fs.errors.FileExpected(src)

    sshfs_mock.configure_mock(
        exists=exists_with_files(src_dir),
        gettype=lambda _: ResourceType.directory,
        copy=copy
    )
    
    return sshfs_mock


def sshfs_mock_remove_expecting_directory(sshfs_type_mock, dir_path):
    sshfs_mock = sshfs_type_mock.return_value

    def remove(path):
        import fs.errors
        raise fs.errors.FileExpected(path)

    sshfs_mock.configure_mock(
        exists=exists_with_files(dir_path),
        gettype=lambda _: ResourceType.directory,
        remove=remove
    )

    return sshfs_mock


def exists_with_files(*args):
    def exists(path: str):
        return path in args

    return exists
