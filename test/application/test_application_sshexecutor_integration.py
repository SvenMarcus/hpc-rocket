from dataclasses import replace
from test.application.fixtures import *
from test.application.launchoptions import *
from test.slurmoutput import get_success_lines
from test.testdoubles.filesystem import DummyFilesystemFactory
from test.testdoubles.sshclient import (ChannelFileStub, ChannelStub,
                                        DelayedChannelSpy,
                                        ProxyJumpVerifyingSSHClient,
                                        SSHClientMock)
from unittest.mock import DEFAULT, Mock, call

import pytest
from hpcrocket.core.application import Application
from hpcrocket.core.slurmbatchjob import SlurmJobStatus, SlurmTaskStatus
from hpcrocket.ssh.errors import SSHError
from hpcrocket.ssh.sshexecutor import SSHExecutorFactory


@pytest.mark.parametrize(["input_keyfile", "expected_keyfile"], INPUT_AND_EXPECTED_KEYFILE_PATHS)
def test__given_valid_config__when_running__should_run_sbatch_over_ssh(sshclient_type_mock,
                                                                       input_keyfile: str,
                                                                       expected_keyfile: str):

    connection = replace(main_connection(), keyfile=input_keyfile)
    valid_options = replace(options(), connection=connection)

    sshclient_mock = SSHClientMock(
        launch_options=valid_options,
        private_keyfile_abspath=expected_keyfile)

    sshclient_type_mock.return_value = sshclient_mock
    sut = Application(SSHExecutorFactory(valid_options), DummyFilesystemFactory(), Mock())

    sut.run(valid_options)

    sshclient_mock.verify()


def test__given_options_with_proxy_jumps__when_running__should_connect_to_executor_through_proxies(sshclient_type_mock):
    cleaned_up_proxyconnection = replace(proxy_connection(), keyfile=f"{HOME_DIR}/proxy1-keyfile")

    mock = ProxyJumpVerifyingSSHClient(
        main_connection(),
        [cleaned_up_proxyconnection])

    sshclient_type_mock.return_value = mock

    sut = Application(SSHExecutorFactory(options_with_proxy()), DummyFilesystemFactory(), Mock())

    sut.run(options_with_proxy())

    mock.verify()


def test__given_ssh_connection_not_available_for_executor__when_running__should_log_error_and_exit(sshclient_type_mock):
    ssh_client_mock = sshclient_type_mock.return_value
    ssh_client_mock.connect.side_effect = SSHError(main_connection().hostname)

    ui_spy = Mock()
    sut = Application(SSHExecutorFactory(options()), DummyFilesystemFactory(), ui_spy)

    sut.run(options())

    ssh_client_mock.exec_command.assert_not_called()
    ui_spy.error.assert_called_once_with(f"SSHError: {main_connection().hostname}")


@ pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_valid_config__when_sbatch_job_succeeds__should_return_exit_code_zero():
    sut = Application(SSHExecutorFactory(options()), DummyFilesystemFactory(), Mock())

    actual = sut.run(options())

    assert actual == 0


@ pytest.mark.usefixtures("failing_sshclient_stub")
def test__given_valid_config__when_sbatch_job_fails__should_return_exit_code_one():

    sut = Application(SSHExecutorFactory(options()), DummyFilesystemFactory(), Mock())

    actual = sut.run(options())

    assert actual == 1


def test__given_valid_config__when_running_long_running_job__should_wait_for_completion(sshclient_type_mock):

    sut = Application(SSHExecutorFactory(options()), DummyFilesystemFactory(), Mock())

    channel_spy = DelayedChannelSpy(exit_code=1, calls_until_exit=2)
    sshclient_type_mock.return_value = CmdSpecificSSHClientStub({
        "sbatch": ChannelFileStub(lines=["1234"]),
        "sacct": ChannelFileStub(
            lines=get_success_lines(),
            channel=channel_spy
        )
    })

    actual = sut.run(options())

    assert actual == 0
    assert channel_spy.times_called == 2


@ pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_ui__when_running__should_update_ui_after_polling():
    ui_spy = Mock()
    sut = Application(SSHExecutorFactory(options()), DummyFilesystemFactory(), ui_spy)

    _ = sut.run(options())

    ui_spy.update.assert_called_with(completed_slurm_job())


def test__given_running_application__when_canceling_after_polling_job__should_cancel_job(sshclient_type_mock):
    from threading import Thread

    sut = Application(SSHExecutorFactory(options()), DummyFilesystemFactory(), Mock())

    long_running = int(1e10)
    sacct_channel = ChannelFileStub(
        lines=get_success_lines(),
        channel=DelayedChannelSpy(calls_until_exit=long_running)
    )

    sshclient_mock = Mock(wraps=CmdSpecificSSHClientStub({
        "sbatch": ChannelFileStub(lines=["1234"]),
        "sacct": sacct_channel,
        "scancel": ChannelFileStub(lines=[])
    }))

    sshclient_mock.exec_command.side_effect = mark_as_done_after_scancel(
        sacct_channel)

    sshclient_type_mock.return_value = sshclient_mock

    thread = Thread(target=lambda: sut.run(options()))
    thread.start()
    wait_until_first_job_poll(sshclient_mock)

    actual = sut.cancel()

    thread.join()

    assert call.exec_command("scancel 1234") in sshclient_mock.method_calls
    assert actual == 130


def wait_until_first_job_poll(sshclient_mock):
    args_list = sshclient_mock.exec_command.call_args_list
    while not any(args[0].startswith("sacct") for args, _ in args_list):
        continue


def mark_as_done_after_scancel(channel):
    def _mark_cmd_as_done_after_scancel(command):
        if command.startswith("scancel"):
            channel._channel = ChannelStub()

        return DEFAULT

    return _mark_cmd_as_done_after_scancel


def completed_slurm_job():
    return SlurmJobStatus(
        id="1603353",
        name="PyFluidsTest",
        state="COMPLETED",
        tasks=[
            SlurmTaskStatus("1603353", "PyFluidsTest", "COMPLETED"),
            SlurmTaskStatus("1603353.bat+", "batch", "COMPLETED"),
            SlurmTaskStatus("1603353.ext+",  "extern", "COMPLETED"),
            SlurmTaskStatus("1603353.0", "singularity", "COMPLETED"),
            SlurmTaskStatus("1603353.1", "singularity", "COMPLETED"),
            SlurmTaskStatus("1603353.2", "singularity", "COMPLETED"),
            SlurmTaskStatus("1603353.3", "singularity", "COMPLETED")
        ]
    )
