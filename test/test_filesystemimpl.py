from hpclaunch.errors import SSHError
from unittest.mock import patch

import pytest
from hpclaunch.filesystemimpl import LocalFilesystem, SSHFilesystem


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
    patcher = patch("fs.sshfs.SSHFS")

    yield patcher.start()

    patcher.stop()


def test__given_sshfilesystem__when_creating_new_instance__should_create_sshfs_with_connection_data(sshfs_type_mock):
    sut = SSHFilesystem('user', 'host', "password", private_key='privatekey')

    sshfs_type_mock.assert_called_with(
        'host', user='user', passwd='password', pkey='privatekey')


def test__given_sshfilesystem__when_creating_new_instance_with_keyfile__should_create_sshfs_with_connection_data(sshfs_type_mock):
    sut = SSHFilesystem('user', 'host', private_keyfile='~/path/to/keyfile')

    sshfs_type_mock.assert_called_with(
        'host', user='user', passwd=None, pkey='~/path/to/keyfile')

def test__when_ssh_connection_fails__should_raise_ssh_error(sshfs_type_mock):
    from fs.errors import CreateFailed
    sshfs_type_mock.side_effect = CreateFailed("Connection failed")

    with pytest.raises(SSHError):
        SSHFilesystem('user', 'host', "password", private_key='privatekey')