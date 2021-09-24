from test.sshfilesystem_assertions import (
    assert_sshfs_connected_with_connection_data,
    assert_sshfs_connected_with_keyfile_from_connection_data,
    assert_sshfs_connected_with_private_key_from_connection_data)
from unittest.mock import Mock, patch

import pytest
from hpcrocket.local.localfilesystem import LocalFilesystem
from hpcrocket.ssh.errors import SSHError
from hpcrocket.ssh.sshexecutor import ConnectionData
from hpcrocket.ssh.sshfilesystem import SSHFilesystem


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
    connection = ConnectionData(
        hostname="host",
        username="user",
        password="password",
        key="privatekey",
        keyfile="mykeyfile"
    )

    sut = SSHFilesystem(connection)

    assert_sshfs_connected_with_connection_data(sshfs_type_mock, connection)


def test__given_sshfilesystem__when_creating_new_instance_with_private_key__should_create_sshfs_with_connection_data(sshfs_type_mock):
    connection = ConnectionData(
        hostname="host",
        username="user",
        key="privatekey"
    )
    sut = SSHFilesystem(connection)

    assert_sshfs_connected_with_private_key_from_connection_data(sshfs_type_mock, connection)


def test__given_sshfilesystem__when_creating_new_instance_with_keyfile__should_create_sshfs_with_connection_data(
        sshfs_type_mock):

    connection = ConnectionData(
        hostname="host",
        username="user",
        keyfile="~/path/to/keyfile"
    )

    sut = SSHFilesystem(connection)

    assert_sshfs_connected_with_keyfile_from_connection_data(sshfs_type_mock, connection)


def test__when_ssh_connection_fails__should_raise_ssh_error(sshfs_type_mock):
    from fs.errors import CreateFailed
    sshfs_type_mock.side_effect = CreateFailed("Connection failed")

    connection = ConnectionData(
        hostname="host",
        username="user",
        keyfile="~/path/to/keyfile"
    )

    with pytest.raises(SSHError):
        SSHFilesystem(connection)
