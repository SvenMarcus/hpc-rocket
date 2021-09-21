from unittest.mock import Mock, patch

import pytest
import paramiko
from hpcrocket.ssh.sshexecutor import ConnectionData, SSHExecutor, RemoteCommand
from hpcrocket.ssh.errors import SSHError


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

    sut = SSHExecutor()

    sut.connect(connection_data())

    assert_connected_with_data(sshclient_instance, connection_data())


@patch("paramiko.SSHClient")
def test__when_connection_fails__should_raise_ssherror(sshclient_class):
    sshclient_instance = sshclient_class.return_value
    sshclient_instance.connect.side_effect = paramiko.AuthenticationException

    sut = SSHExecutor()

    with pytest.raises(SSHError):
        sut.connect(connection_data())


@patch("paramiko.SSHClient")
def test__given_new_client__is_connected__should_be_false(sshclient_class):
    sut = SSHExecutor()

    assert not sut.is_connected


@patch("paramiko.SSHClient")
def test__given_connected_client__is_connected__should_be_true(sshclient_class):
    sut = SSHExecutor()
    sut.connect(connection_data())

    assert sut.is_connected


@patch("paramiko.SSHClient")
def test__when_connection_fails__is_connected__should_be_false(sshclient_class):
    sshclient_instance = sshclient_class.return_value
    sshclient_instance.connect.side_effect = paramiko.AuthenticationException

    sut = SSHExecutor()
    with pytest.raises(SSHError):
        sut.connect(connection_data())

    assert not sut.is_connected


@patch("paramiko.SSHClient")
def test__given_connected_client__when_disconnecting__should_disconnect(sshclient_class):
    sshclient_instance = sshclient_class.return_value

    sut = SSHExecutor()
    sut.connect(connection_data())

    sut.disconnect()

    sshclient_instance.close.assert_called()
    assert not sut.is_connected


@patch("paramiko.SSHClient")
def test__given_connected_client__when_executing_command__should_execute_command(sshclient_class):
    sshclient_instance = sshclient_class.return_value
    sshclient_instance.exec_command.return_value = (Mock(), Mock(), Mock())

    sut = SSHExecutor()
    sut.connect(connection_data())

    sut.exec_command("dummycmd")

    sshclient_instance.exec_command.assert_called_with("dummycmd")


@patch("paramiko.SSHClient")
def test__given_connected_client__when_executing_command__should_return_remote_command(sshclient_class):
    sshclient_instance = sshclient_class.return_value
    sshclient_instance.exec_command.return_value = (Mock(), Mock(), Mock())

    sut = SSHExecutor()
    sut.connect(connection_data())

    actual = sut.exec_command("dummycmd")

    assert isinstance(actual, RemoteCommand)


@patch("paramiko.SSHClient")
def test__given_proxyjump__when_connecting__should_connect_to_destination_through_proxy(sshclient_class):
    proxy_mock, transport_channel = proxy_mock_with_transport()

    main_mock = Mock(name="main-mock")
    mocks = iter((main_mock, proxy_mock))

    def next_mock():
        return next(mocks)

    sshclient_class.side_effect = next_mock

    sut = SSHExecutor()
    sut.connect(connection_data(), proxyjumps=[proxy_connection_data()])

    assert_connected_with_data(proxy_mock, proxy_connection_data())
    assert_channel_opened(proxy_mock.get_transport(), connection_data())
    assert_connected_with_data(main_mock, connection_data(), channel=transport_channel)


@patch("paramiko.SSHClient")
def test__given_two_proxyjumps__when_connecting__should_connect_to_proxies_then_destination(sshclient_class):
    first_proxy, first_transport_channel = proxy_mock_with_transport(1)
    second_proxy, second_transport_channel = proxy_mock_with_transport(2)

    main_mock = Mock(name="main-mock")
    mocks = iter((main_mock, first_proxy, second_proxy))

    def next_mock():
        return next(mocks)

    sshclient_class.side_effect = next_mock

    sut = SSHExecutor()
    first_proxy_connection = proxy_connection_data(1)
    second_proxy_connection = proxy_connection_data(2)
    final_connection = connection_data()
    sut.connect(final_connection,
                proxyjumps=[
                    first_proxy_connection,
                    second_proxy_connection])

    assert_connected_with_data(first_proxy, first_proxy_connection)
    assert_channel_opened(first_proxy.get_transport(), second_proxy_connection)

    assert_connected_with_data(second_proxy, second_proxy_connection, first_transport_channel)
    assert_channel_opened(second_proxy.get_transport(), final_connection)

    assert_connected_with_data(main_mock, connection_data(), second_transport_channel)


@patch("paramiko.SSHClient")
def test__given_proxyjump__when_connection_to_proxy_fails__should_raise_ssherror(sshclient_class):
    proxy_mock, _ = proxy_mock_with_transport()
    proxy_mock.connect.side_effect = paramiko.AuthenticationException

    sshclient_class.return_value = proxy_mock

    sut = SSHExecutor()

    with pytest.raises(SSHError):
        sut.connect(connection_data(), proxyjumps=[proxy_connection_data()])


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


def assert_channel_opened(transport, connection):
    transport.open_channel.assert_called_with(
        "direct-tcpip", (connection.hostname,
                         connection.port), ('', 0)
    )
