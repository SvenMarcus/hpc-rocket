from hpcrocket.ssh.connectiondata import ConnectionData


def assert_sshfs_connected_with_connection_data(sshfs_type_mock, connection_data: ConnectionData, channel=None):
    sshfs_type_mock.assert_called_with(
        host=connection_data.hostname,
        user=connection_data.username,
        passwd=connection_data.password,
        pkey=connection_data.key,
        port=connection_data.port, sock=channel)



def assert_sshfs_connected_with_password_from_connection_data(sshfs_type_mock, connection_data: ConnectionData):
    sshfs_type_mock.assert_called_with(
        host=connection_data.hostname,
        user=connection_data.username,
        passwd=connection_data.password,
        pkey=None,
        port=connection_data.port, sock=None)


def assert_sshfs_connected_with_private_key_from_connection_data(sshfs_type_mock, connection_data: ConnectionData):
    sshfs_type_mock.assert_called_with(
        host=connection_data.hostname,
        user=connection_data.username,
        passwd=None,
        pkey=connection_data.key,
        port=connection_data.port, sock=None)


def assert_sshfs_connected_with_keyfile_from_connection_data(sshfs_type_mock, connection_data: ConnectionData):
    sshfs_type_mock.assert_called_with(
        host=connection_data.hostname,
        user=connection_data.username,
        passwd=None,
        pkey=connection_data.keyfile,
        port=connection_data.port, sock=None)
