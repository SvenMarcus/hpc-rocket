import os
from test.pyfilesystem_testdoubles import PyFilesystemStub
from test.sshclient_testdoubles import (ChannelFileStub,
                                        CmdSpecificSSHClientStub,
                                        DelayedChannelSpy, SSHClientMock)
from typing import List, Tuple
from unittest.mock import MagicMock, Mock, patch

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
    return get_success_lines()


def get_success_lines():
    with open("test/slurmoutput/sacct_completed.txt", "r") as file:
        lines = file.readlines()
        return lines


@pytest.fixture
def successful_sshclient_stub(sshclient_type_mock, success_lines):
    sshclient_type_mock.return_value = CmdSpecificSSHClientStub({
        "sbatch": ChannelFileStub(lines=["1234"]),
        "sacct": ChannelFileStub(lines=success_lines)
    })


@pytest.fixture(autouse=True)
def osfs_type_mock():
    patcher = patch("fs.osfs.OSFS")
    yield patcher.start()

    patcher.stop()


@pytest.fixture(autouse=True)
def sshfs_type_mock():
    # The mocking does not work for some reason if only one of the paths is mocked
    patcher1 = patch("fs.sshfs.sshfs.SSHFS")
    patcher2 = patch("fs.sshfs.SSHFS")
    patcher1.start()
    mock = patcher2.start()

    yield mock

    patcher1.stop()
    patcher2.stop()


@pytest.fixture
def fs_copy_file_mock():
    patcher = patch("fs.copy.copy_file")
    yield patcher.start()

    patcher.stop()


def make_options_with_files_to_copy(files_to_copy):
    options = LaunchOptions(
        host="example.com",
        user="myuser",
        password="mypassword",
        private_key="PRIVATE",
        private_keyfile="my_private_keyfile",
        sbatch="test.job",
        poll_interval=0,
        copy_files=files_to_copy
    )

    return options


class VerifyFilesystemAndSSHClientCallOrder:

    def __init__(self, osfs_type_mock,
                 sshfs_type_mock,
                 fs_copy_file_mock,
                 sshclient_type_mock,
                 expected_copies: List[Tuple[str, str]]) -> None:

        self.call_order = []
        self.osfs_mock = self._make_osfs_mock(osfs_type_mock, expected_copies)
        self.sshfs_mock = self._make_sshfs_mock(sshfs_type_mock)

        self.expected_call_order = self._prepare_expected_call_order(
            expected_copies, self.osfs_mock, self.sshfs_mock)

        fs_copy_file_mock.side_effect = self._pyfs_copy_file
        sshclient_type_mock.return_value.exec_command = self._sshclient_exec_command

    def _make_osfs_mock(self, osfs_type_mock, expected_copies):
        osfs_mock = PyFilesystemStub(
            [src for src, _ in expected_copies])
        osfs_type_mock.return_value = osfs_mock

        return osfs_mock

    def _make_sshfs_mock(self, sshfs_type_mock):
        sshfs_mock = PyFilesystemStub()
        sshfs_type_mock.return_value = sshfs_mock

        return sshfs_mock

    def _prepare_expected_call_order(self, expected_copies, osfs_mock, sshfs_mock):
        expected_call_order = [
            (osfs_mock, src, sshfs_mock, dest)
            for src, dest in expected_copies
        ]

        expected_call_order.extend(
            ["exec_command sbatch", "exec_command sacct"])

        return expected_call_order

    def _pyfs_copy_file(self, origin_fs, origin_path, dest_fs, dest_path):
        args = (origin_fs, origin_path, dest_fs, dest_path)
        self.call_order.append(args)

    def _sshclient_exec_command(self, command):
        self.call_order.append(f"exec_command {command.split()[0]}")
        return Mock(ChannelFileStub), ChannelFileStub(lines=get_success_lines()), Mock(ChannelFileStub)

    def verify(self):
        assert self.call_order == self.expected_call_order


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


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_valid_config__when_running__should_open_local_fs_in_current_directory(valid_options: LaunchOptions, osfs_type_mock):

    sut = Application(valid_options, Mock())

    sut.run()

    osfs_type_mock.assert_called_with(".")


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_valid_config__when_running__should_login_to_sshfs_with_correct_credentials(valid_options: LaunchOptions, sshfs_type_mock):

    sut = Application(valid_options, Mock())

    sut.run()

    sshfs_type_mock.assert_called_with(
        valid_options.host, user=valid_options.user, passwd=valid_options.password, pkey=valid_options.private_key)


def test__given_valid_config_with_files_to_copy_and_clean__when_running__should_copy_files_before_running_sbatch_then_clean(osfs_type_mock, sshfs_type_mock, fs_copy_file_mock: Mock,
                                                                                                                            sshclient_type_mock):

    files_to_copy = [("myfile.txt", "mycopy.txt")]
    options = make_options_with_files_to_copy(files_to_copy)

    call_order_mock = VerifyFilesystemAndSSHClientCallOrder(osfs_type_mock, sshfs_type_mock,
                                                            fs_copy_file_mock, sshclient_type_mock,
                                                            expected_copies=files_to_copy)

    sut = Application(options, Mock())

    sut.run()

    call_order_mock.verify()


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_valid_config__when_sbatch_job_succeeds__should_return_exit_code_zero(valid_options: LaunchOptions):

    sut = Application(valid_options, Mock())

    actual = sut.run()

    assert actual == 0


def test__given_valid_config__when_sbatch_job_fails__should_return_exit_code_one(valid_options: LaunchOptions, sshclient_type_mock):

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


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_ui__when_running__should_update_ui_after_polling(valid_options: LaunchOptions):
    ui_spy = Mock()
    sut = Application(valid_options, ui_spy)

    _ = sut.run()

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
