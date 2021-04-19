from typing import Callable
from paramiko.channel import ChannelStdinFile
import paramiko
import pytest

from unittest.mock import MagicMock, Mock, patch
from ssh_slurm_runner.sshexecutor import RemoteCommand, SSHExecutor
from test.paramiko_sshclient_mockutil import make_get_transport, make_close, get_blocking_channel_exit_status_ready_func


@pytest.fixture
def pm_sshclient_fake():
    patcher = patch("paramiko.SSHClient")
    patched = patcher.start()
    patched.return_value.configure_mock(
        get_transport=make_get_transport(patched.return_value),
        close=make_close(patched.return_value)
    )

    yield patched.return_value

    patcher.stop()


@pytest.fixture
def stdout():
    stdout = MagicMock("paramiko.channel.ChannelFile")

    exit_status_ready = get_blocking_channel_exit_status_ready_func(stdout)
    stdout.configure_mock(
        channel=MagicMock(
            exit_status_ready=exit_status_ready,
            exit_status=666),
        readlines=lambda: [
            "first stdout line",
            "second stdout line"
        ]
    )

    return stdout


@pytest.fixture
def stderr():
    stderr = MagicMock("paramiko.channel.ChannelStderrFile")
    stderr.configure_mock(readlines=lambda: [
        "first stderr line",
        "second stderr line"
    ])

    return stderr


def test__when_connecting_with_user_and_keyfile__should_connect_with_username_and_keyfile_to_host(pm_sshclient_fake: Mock):
    sut = SSHExecutor("cluster.example.com")
    sut.connect("myuser", keyfile="/home/myuser/.ssh/keyfile")

    pm_sshclient_fake.connect.assert_called_with(
        'cluster.example.com', username='myuser', key_filename='/home/myuser/.ssh/keyfile', password=None)


def test__when_connecting_with_user_and_password__should_call_connect_with_username_and_password(pm_sshclient_fake: Mock):
    sut = SSHExecutor("cluster.example.com")
    sut.connect("myuser", password="12345")

    pm_sshclient_fake.connect.assert_called_with(
        'cluster.example.com', username='myuser', password='12345', key_filename=None)


def test__when_connecting__is_connected_should_be_true(pm_sshclient_fake: Mock):
    sut = SSHExecutor("cluster.example.com")
    sut.connect("myuser", password="12345")

    assert sut.is_connected == True


def test__when_not_connecting__is_connected__should_be_false(pm_sshclient_fake: Mock):
    sut = SSHExecutor("cluster.example.com")

    assert sut.is_connected == False


def test__given_client_connected__when_executing_command__should_return_remote_command_with_stdin_stdout_stderr(pm_sshclient_fake: Mock):
    pm_sshclient_fake.exec_command.return_value = (
        Mock("paramiko.channel.ChannelStdinFile"),
        Mock("paramiko.channel.ChannelFile"),
        Mock("paramiko.channel.ChannelStderrFile"))

    sut = SSHExecutor("cluster.example.com")
    sut.connect("myuser", password="12345")

    actual = sut.exec_command("echo 'Hello World'")

    assert isinstance(actual, RemoteCommand)


def test__given_client_connected__when_executing_command__should_call_exec_command_on_client(pm_sshclient_fake: Mock):
    pm_sshclient_fake.exec_command.return_value = (
        Mock("paramiko.channel.ChannelStdinFile"),
        Mock("paramiko.channel.ChannelFile"),
        Mock("paramiko.channel.ChannelStderrFile"))

    sut = SSHExecutor("cluster.example.com")
    sut.connect("myuser", password="12345")

    _ = sut.exec_command("echo 'Hello World'")

    pm_sshclient_fake.exec_command.assert_called_with("echo 'Hello World'")


def test__given_disconnected_client__when_executing_command__should_raise_ssh_exception(pm_sshclient_fake: Mock):
    sut = SSHExecutor("cluster.example.com")

    with pytest.raises(paramiko.SSHException):
        sut.exec_command("echo 'Hello World'")


def test__given_connected_client__when_disconnecting__is_connected_should_be_false(pm_sshclient_fake: Mock):
    sut = SSHExecutor("cluster.example.com")
    sut.connect("myuser", password="12345")

    sut.disconnect()

    assert sut.is_connected == False
