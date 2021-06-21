from unittest.mock import patch

import pytest
from ssh_slurm_runner.filesystemimpl import LocalFilesystem, SSHFilesystem


@pytest.fixture
def osfs_type_mock():
    patcher = patch("fs.osfs.OSFS")
    yield patcher.start()

    patcher.stop()


def test_local_filesystem__when_creating_new_instance__should_open_fs_in_folder(osfs_type_mock):
    expected_path = "~/myfolder"
    sut = LocalFilesystem(expected_path)

    osfs_type_mock.assert_called_with(expected_path)


@pytest.fixture
def sshfs_type_mock():
    # The mocking does not work for some reason if only one of the paths is mocked
    patcher1 = patch("fs.sshfs.sshfs.SSHFS")
    patcher2 = patch("fs.sshfs.SSHFS")
    patcher1.start()
    mock = patcher2.start()

    yield mock

    patcher1.stop()
    patcher2.stop()


def test__given_sshfilesystem__when_creating_new_instance__should_create_sshfs_with_connection_data(sshfs_type_mock):
    sut = SSHFilesystem('user', 'host', "password", 'privatekey')

    sshfs_type_mock.assert_called_with(
        'host', user='user', passwd='password', pkey='privatekey')
