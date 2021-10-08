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
    valid_options = options(connection=connection)

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
    

def test__given_valid_config__when_running_long_running_job__should_wait_for_completion(sshclient_type_mock):

    sut = Application(SSHExecutorFactory(options(watch=True)), DummyFilesystemFactory(), Mock())

    channel_spy = DelayedChannelSpy(exit_code=1, calls_until_exit=2)
    sshclient_type_mock.return_value = CmdSpecificSSHClientStub({
        "sbatch": ChannelFileStub(lines=["1234"]),
        "sacct": ChannelFileStub(
            lines=get_success_lines(),
            channel=channel_spy
        )
    })

    actual = sut.run(options(watch=True))

    assert actual == 0
    assert channel_spy.times_called == 2


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
