
from test.testdoubles.filesystem import sshfs_with_connection_fake
from test.testdoubles.sshclient import ProxyJumpVerifyingSSHClient
from unittest.mock import ANY, Mock, patch

from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.pyfilesystem.factory import PyFilesystemFactory
from hpcrocket.pyfilesystem.localfilesystem import LocalFilesystem
from hpcrocket.pyfilesystem.sshfilesystem import SSHFilesystem
from hpcrocket.ssh.connectiondata import ConnectionData


def options():
    return LaunchOptions(
        sbatch="",
        connection=ConnectionData(
            hostname="example.com",
            username="user",
            password="password",
            key="key",
            port=1),
        proxyjumps=[ConnectionData(
            hostname="proxy",
            username="proxy-user",
            password="password")]
    )


@patch("fs.osfs.OSFS")
def test__when_creating_local_filesystem__should_return_local_filesystem_in_current_dir(mock: Mock):
    sut = PyFilesystemFactory(options())

    actual = sut.create_local_filesystem()

    assert isinstance(actual, LocalFilesystem)
    mock.assert_called_with(".")


@patch("paramiko.SSHClient")
def test__when_creating_ssh_filesystem__should_return_connected_ssh_filesystem(sshclient_type_mock: Mock):
    opts = options()
    sshclient_mock = ProxyJumpVerifyingSSHClient(opts.connection, opts.proxyjumps)
    sshclient_type_mock.return_value = sshclient_mock
    sut = PyFilesystemFactory(opts)

    with sshfs_with_connection_fake(sshclient_mock):
        actual = sut.create_ssh_filesystem()

        assert isinstance(actual, SSHFilesystem)
        sshclient_mock.verify()


@patch("hpcrocket.ssh.chmodsshfs.PermissionChangingSSHFSDecorator")
def test__when_creating_ssh_filesystem__should_open_homedir(chmodsshfs_type_mock):
    sshfs_mock = chmodsshfs_type_mock.return_value
    sshfs_mock.homedir.return_value = "/home/proxy-user"
    sut = PyFilesystemFactory(options())

    with patch("paramiko.SSHClient"):
        sut.create_ssh_filesystem()

        sshfs_mock.opendir.assert_called_with("/home/proxy-user", factory=ANY)
