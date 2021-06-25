import os

from test.sshclient_testdoubles import (ChannelFileStub,
                                        CmdSpecificSSHClientStub,
                                        DelayedChannelSpy, SSHClientMock)
from typing import List
from unittest.mock import Mock, patch

import pytest
from ssh_slurm_runner.application import Application
from ssh_slurm_runner.launchoptions import LaunchOptions
from ssh_slurm_runner.slurmrunner import SlurmJob, SlurmTask


@pytest.fixture
def sshclient_type_mock():
    patcher = patch("paramiko.SSHClient")
    type_mock = patcher.start()

    yield type_mock

    patcher.stop()


@pytest.fixture
def valid_options():
    return LaunchOptions(
        host="example.com",
        user="myuser",
        password="mypassword",
        private_key="PRIVATE",
        private_keyfile="my_private_keyfile",
        sbatch="test.job",
        poll_interval=0
    )


@pytest.fixture
def success_lines():
    with open("test/slurmoutput/sacct_completed.txt", "r") as file:
        lines = file.readlines()
        return lines


def test__given_valid_config__when_running__should_run_sbatch_over_ssh(sshclient_type_mock,
                                                                       valid_options: LaunchOptions,
                                                                       success_lines: List[str]):
    os.environ['HOME'] = "/home/user"
    sshclient_mock = SSHClientMock(
        cmd_to_channels={
            "sbatch": ChannelFileStub(lines=["1234"]),
            "sacct": ChannelFileStub(lines=success_lines)
        },
        launch_options=valid_options,
        host_key_file="/home/user/.ssh/known_hosts")

    sshclient_type_mock.return_value = sshclient_mock
    sut = Application(valid_options, Mock())

    sut.run()

    sshclient_mock.verify()


def test__given_valid_config__when_sbatch_job_succeeds__should_return_exit_code_zero(sshclient_type_mock,
                                                                                     valid_options: LaunchOptions,
                                                                                     success_lines: List[str]):

    sut = Application(valid_options, Mock())

    sshclient_type_mock.return_value = CmdSpecificSSHClientStub({
        "sbatch": ChannelFileStub(lines=["1234"]),
        "sacct": ChannelFileStub(lines=success_lines)
    })

    actual = sut.run()

    assert actual == 0


def test__given_valid_config__when_sbatch_job_fails__should_return_exit_code_one(sshclient_type_mock,
                                                                                 valid_options: LaunchOptions):

    sut = Application(valid_options, Mock())

    with open("test/slurmoutput/sacct_completed_failed.txt", "r") as file:
        error_lines = file.readlines()

        sshclient_type_mock.return_value = CmdSpecificSSHClientStub({
            "sbatch": ChannelFileStub(lines=["1234"]),
            "sacct": ChannelFileStub(lines=error_lines)
        })

        actual = sut.run()

        assert actual == 1


def test__given_valid_config__when_running_long_running_job__should_wait_for_completion(sshclient_type_mock,
                                                                                        valid_options: LaunchOptions,
                                                                                        success_lines: List[str]):

    sut = Application(valid_options, Mock())

    channel_spy = DelayedChannelSpy(exit_code=1, calls_until_exit=2)
    sshclient_type_mock.return_value = CmdSpecificSSHClientStub({
        "sbatch": ChannelFileStub(lines=["1234"]),
        "sacct": ChannelFileStub(
            lines=success_lines,
            channel=channel_spy
        )
    })

    actual = sut.run()

    assert actual == 0
    assert channel_spy.times_called == 2


def test__given_ui__when_running__should_update_ui_after_polling(valid_options: LaunchOptions, sshclient_type_mock, success_lines: List[str]):
    ui_spy = Mock()
    sut = Application(valid_options, ui_spy)

    sshclient_type_mock.return_value = CmdSpecificSSHClientStub({
        "sbatch": ChannelFileStub(lines=["1234"]),
        "sacct": ChannelFileStub(lines=success_lines)
    })

    actual = sut.run()

    ui_spy.update.assert_called_with(completed_slurm_job())


def completed_slurm_job():
    return SlurmJob(
        id="1603353",
        name="PyFluidsTest",
        state="COMPLETED",
        tasks=[
            SlurmTask("1603353", "PyFluidsTest", "COMPLETED"),
            SlurmTask("1603353.bat+", "batch", "COMPLETED"),
            SlurmTask("1603353.ext+",  "extern", "COMPLETED"),
            SlurmTask("1603353.0", "singularity", "COMPLETED"),
            SlurmTask("1603353.1", "singularity", "COMPLETED"),
            SlurmTask("1603353.2", "singularity", "COMPLETED"),
            SlurmTask("1603353.3", "singularity", "COMPLETED")
        ]
    )
