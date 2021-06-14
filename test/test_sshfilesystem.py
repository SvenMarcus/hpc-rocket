from ssh_slurm_runner.filesystem import Filesystem
import pytest
from ssh_slurm_runner.sshfilesystem import SSHFilesystem
from unittest.mock import MagicMock, patch


class MockFilesystem(Filesystem):

    def __init__(self) -> None:
        self.fs = MagicMock(spec="fs.base.FS").return_value

    def copy(self, source: str, target: str, filesystem: 'Filesystem') -> None:
        pass

    def delete(self, path: str) -> None:
        pass


@pytest.fixture
def sshfs_type_mock():
    # The mocking does not work for some reason if only one of the paths is mocked
    patcher1 = patch("fs.sshfs.sshfs.SSHFS")
    patcher2 = patch("fs.sshfs.SSHFS")
    patcher1.start()
    yield patcher2.start()

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

    sut.copy("~/file.txt", "~/another/folder/copy.txt")

    sshfs_mock = sshfs_type_mock.return_value
    sshfs_mock.copy.assert_called_with(
        "~/file.txt", "~/another/folder/copy.txt")


def test__when_copying_file_to_other_filesystem__should_call_copy_file(sshfs_type_mock, copy_file):
    fs_mock = MockFilesystem()
    sut = SSHFilesystem('user', 'host', 'privatekey')

    sut.copy("~/file.txt", "~/another/folder/copy.txt", filesystem=fs_mock)

    sshfs_mock = sshfs_type_mock.return_value
    copy_file.assert_called_with(
        sshfs_mock, "~/file.txt", fs_mock.fs, "~/another/folder/copy.txt")
