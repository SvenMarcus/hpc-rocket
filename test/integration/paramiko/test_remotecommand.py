from test.testdoubles.paramiko_sshclient_mockutil import \
    get_blocking_channel_exit_status_ready_func
from unittest.mock import MagicMock

import pytest
from hpcrocket.ssh.sshexecutor import RemoteCommand


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


def test_when_calling_wait_until_exit__should_block_until_exit_status_ready(stdout, stderr):
    sut = RemoteCommand(MagicMock("paramiko.channel.ChannelStdinFile"),
                        stdout,
                        stderr)

    actual = sut.wait_until_exit()

    assert actual == 0
    assert sut.exit_status == 0


def test__given_waited_until_exit__when_getting_stdout_and_stderr__should_return_channel_results(stdout, stderr):
    sut = RemoteCommand(MagicMock("paramiko.channel.ChannelStdinFile"),
                        stdout,
                        stderr)

    actual = sut.wait_until_exit()

    assert sut.stdout() == ["first stdout line", "second stdout line"]
    assert sut.stderr() == ["first stderr line", "second stderr line"]
