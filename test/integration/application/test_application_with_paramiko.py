from dataclasses import replace
from test.integration.application.fixtures import *
from test.application.launchoptions import *
from test.slurmoutput import get_success_lines
from test.testdoubles.filesystem import DummyFilesystemFactory
from test.testdoubles.sshclient import (ChannelFileStub, CmdSpecificSSHClientStub, DelayedChannelSpy,
                                        ProxyJumpVerifyingSSHClient,
                                        SSHClientMock)
from unittest.mock import Mock

import pytest
from hpcrocket.core.application import Application
from hpcrocket.ssh.sshexecutor import SSHExecutor


def make_sut(options, ui=None):
    executor = SSHExecutor(options.connection, options.proxyjumps)
    return Application(executor, DummyFilesystemFactory(), ui or Mock())


# NOTE: The options contain connection data that resolves the home directory to an absolute path
# Therefore we need to initialize the options AFTER the home directory has been set by the test fixture
# For this reason we use a function to simply return the option initializer inside the finalized test environment
def options(option_func):
    return option_func


OPTION_MAKERS = (
    options(status_options_with_proxy),
    options(cancel_options_with_proxy),
    options(watch_options_with_proxy),
    options(launch_options_with_proxy),
)


@pytest.mark.parametrize(["input_keyfile", "expected_keyfile"], INPUT_AND_EXPECTED_KEYFILE_PATHS)
def test__given_valid_launch_config__when_running__should_run_sbatch_over_ssh(sshclient_type_mock,
                                                                              input_keyfile: str,
                                                                              expected_keyfile: str):

    connection = replace(main_connection(), keyfile=input_keyfile)
    valid_options = launch_options(connection=connection)

    sshclient_mock = SSHClientMock(
        launch_options=valid_options,
        private_keyfile_abspath=expected_keyfile)

    sshclient_type_mock.return_value = sshclient_mock
    sut = make_sut(valid_options)

    sut.run(valid_options)

    sshclient_mock.verify()


@pytest.mark.parametrize("options_maker", OPTION_MAKERS)
def test__given_options_with_proxy_jumps__when_running__should_connect_to_executor_through_proxies(
        sshclient_type_mock,
        options_maker):

    options = options_maker()
    cleaned_up_proxyconnection = replace(proxy_connection(), keyfile=f"{HOME_DIR}/proxy1-keyfile")

    mock = ProxyJumpVerifyingSSHClient(
        main_connection(),
        [cleaned_up_proxyconnection])

    sshclient_type_mock.return_value = mock
    sut = make_sut(options)

    sut.run(options)

    mock.verify()


def test__given_valid_config__when_running_long_running_job__should_wait_for_completion(sshclient_type_mock):
    channel_spy = DelayedChannelSpy(exit_code=0, calls_until_exit=2)
    sshclient_type_mock.return_value = CmdSpecificSSHClientStub({
        "sbatch": ChannelFileStub(lines=["1234"]),
        "sacct": ChannelFileStub(
            lines=get_success_lines(),
            channel=channel_spy
        )
    })

    sut = make_sut(launch_options(watch=True))

    actual = sut.run(launch_options(watch=True))

    assert actual == 0
    assert channel_spy.times_called == 2
