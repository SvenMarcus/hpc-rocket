from unittest.mock import MagicMock, Mock


def make_get_transport(sshclient: Mock, active: bool = True):
    """
    Creates the sshclient.get_transport() method that returns active/inactive Transport depending on the 'active' argument

    Args:
        pm_ssh_client_fake (Mock): The SSHClient Mock

        active (bool): Determines if an active or inactive Transport will be returned by get_transport()
    """
    transport_stub = MagicMock("paramiko.transport.Transport")
    transport_stub.configure_mock(is_active=lambda: active)

    def get_transport():
        if sshclient.connect.called:
            return transport_stub

        return None

    return get_transport


def make_close(pm_ssh_client_fake: Mock):
    """
    Returns a sshclient.close() method that changes sshclient.make_transport() to return an inactive Transport

    Args:
        pm_ssh_client_fake (Mock): The SSHClient Mock
    """
    def close():
        pm_ssh_client_fake.configure_mock(
            get_transport=make_get_transport(
                pm_ssh_client_fake,
                active=False))

    return close


def get_blocking_channel_exit_status_ready_func(stdout, exit_code=0):
    """
    Returns a channel.exit_status_ready() methods the needs to be called twice before it returns True and sets the exit status to the given value
    """
    counter = 0

    def exit_status_ready():
        nonlocal counter
        counter += 1
        if counter == 2:
            stdout.channel.exit_status = exit_code
        return counter == 2

    return exit_status_ready
