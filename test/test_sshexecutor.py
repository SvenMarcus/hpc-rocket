from test.testdoubles.sshclient import ProxyJumpVerifyingSSHClient
from unittest.mock import Mock, patch

import paramiko
import pytest
from hpcrocket.ssh.connectiondata import ConnectionData
from hpcrocket.ssh.errors import SSHError
from hpcrocket.ssh.sshexecutor import RemoteCommand, SSHExecutor


def connection_data():
    return ConnectionData(
        hostname="example.com",
        username="user",
        port=22,
        keyfile="~/.ssh/file",
        password="1234")


def proxy_connection_data(proxy_index=1):
    return ConnectionData(
        hostname=f"proxy-host-{proxy_index}",
        username=f"proxy-user-{proxy_index}",
        password=f"proxy-pass-{proxy_index}",
        keyfile=f"proxy-keyfile-{proxy_index}",
        port=proxy_index
    )


@patch("paramiko.SSHClient")
def test__given_connection_data__when_connecting__should_connect_client(sshclient_class):
    sshclient_instance = sshclient_class.return_value

    sut = SSHExecutor(connection_data())

    sut.connect()

    assert_connected_with_data(sshclient_instance, connection_data())


@patch("paramiko.SSHClient")
def test__when_connection_fails__should_raise_ssherror(sshclient_class):
    sshclient_instance = sshclient_class.return_value
    sshclient_instance.connect.side_effect = paramiko.AuthenticationException

    sut = SSHExecutor(connection_data())

    with pytest.raises(SSHError):
        sut.connect()


@patch("paramiko.SSHClient")
def test__given_new_client__is_connected__should_be_false(sshclient_class):
    sut = SSHExecutor(connection_data())

    assert not sut.is_connected


@patch("paramiko.SSHClient")
def test__given_connected_client__is_connected__should_be_true(sshclient_class):
    sut = SSHExecutor(connection_data())
    sut.connect()

    assert sut.is_connected


@patch("paramiko.SSHClient")
def test__when_connection_fails__is_connected__should_be_false(sshclient_class):
    sshclient_instance = sshclient_class.return_value
    sshclient_instance.connect.side_effect = paramiko.AuthenticationException

    sut = SSHExecutor(connection_data())
    with pytest.raises(SSHError):
        sut.connect()

    assert not sut.is_connected


@patch("paramiko.SSHClient")
def test__given_connected_client__when_disconnecting__should_disconnect(sshclient_class):
    sshclient_instance = sshclient_class.return_value

    sut = SSHExecutor(connection_data())
    sut.connect()

    sut.close()

    sshclient_instance.close.assert_called()
    assert not sut.is_connected


@patch("paramiko.SSHClient")
def test__given_connected_client__when_executing_command__should_execute_command(sshclient_class):
    sshclient_instance = sshclient_class.return_value
    sshclient_instance.exec_command.return_value = (Mock(), Mock(), Mock())

    sut = SSHExecutor(connection_data())
    sut.connect()

    sut.exec_command("dummycmd")

    sshclient_instance.exec_command.assert_called_with("dummycmd")


@patch("paramiko.SSHClient")
def test__given_connected_client__when_executing_command__should_return_remote_command(sshclient_class):
    sshclient_instance = sshclient_class.return_value
    sshclient_instance.exec_command.return_value = (Mock(), Mock(), Mock())

    sut = SSHExecutor(connection_data())
    sut.connect()

    actual = sut.exec_command("dummycmd")

    assert isinstance(actual, RemoteCommand)


@patch("paramiko.SSHClient")
def test__given_proxyjump__when_connecting__should_connect_to_destination_through_proxy(sshclient_class):
    mock = ProxyJumpVerifyingSSHClient(connection_data(), [proxy_connection_data()])
    sshclient_class.return_value = mock

    sut = SSHExecutor(connection_data(), proxyjumps=[proxy_connection_data()])
    sut.connect()

    mock.verify()


@patch("paramiko.SSHClient")
def test__given_two_proxyjumps__when_connecting__should_connect_to_proxies_then_destination(sshclient_class):
    jumps = [proxy_connection_data(1), proxy_connection_data(2)]
    mock = ProxyJumpVerifyingSSHClient(connection_data(), jumps)
    sshclient_class.return_value = mock

    sut = SSHExecutor(connection_data(), proxyjumps=jumps)
    sut.connect()

    mock.verify()


@patch("paramiko.SSHClient")
def test__given_proxyjump__when_connection_to_proxy_fails__should_raise_ssherror(sshclient_class):
    proxy_mock, _ = proxy_mock_with_transport()
    proxy_mock.connect.side_effect = paramiko.AuthenticationException

    sshclient_class.return_value = proxy_mock

    sut = SSHExecutor(connection_data(), proxyjumps=[proxy_connection_data()])

    with pytest.raises(SSHError):
        sut.connect()


def proxy_mock_with_transport(proxy_index=1):
    channel_mock = Mock(f"channel-mock-{proxy_index}")

    transport_mock = Mock(name=f"transport-mock-{proxy_index}")
    transport_mock.open_channel.return_value = channel_mock

    proxy_mock = Mock(name=f"proxy-mock-{proxy_index}")
    proxy_mock.get_transport.return_value = transport_mock

    return proxy_mock, channel_mock


def assert_connected_with_data(sshclient_mock: Mock, connection: ConnectionData, channel: Mock = None):
    sshclient_mock.connect.assert_called_with(
        hostname=connection.hostname,
        username=connection.username,
        port=connection.port,
        password=connection.password,
        key_filename=connection.keyfile,
        pkey=connection.key,
        sock=channel
    )
