import dataclasses
import os
from dataclasses import replace
from ssh_slurm_runner.environmentpreparation import CopyInstruction
from ssh_slurm_runner.errors import SSHError
from test.pyfilesystem_testdoubles import (PyFilesystemFake, PyFilesystemStub,
                                           copy_file_between_filesystems_fake)
from test.sshclient_testdoubles import (ChannelFileStub, ChannelStub,
                                        CmdSpecificSSHClientStub,
                                        DelayedChannelSpy, SSHClientMock)
from unittest import mock
from unittest.mock import MagicMock, Mock, call, patch

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


def get_success_lines():
    with open("test/slurmoutput/sacct_completed.txt", "r") as file:
        lines = file.readlines()
        return lines


def get_error_lines():
    with open("test/slurmoutput/sacct_completed_failed.txt", "r") as file:
        error_lines = file.readlines()
        return error_lines


@pytest.fixture
def successful_sshclient_stub(sshclient_type_mock):
    ssh_stub = CmdSpecificSSHClientStub({
        "sbatch": ChannelFileStub(lines=["1234"]),
        "sacct": ChannelFileStub(lines=get_success_lines())
    })

    wrapper_mock = Mock(wraps=ssh_stub)
    sshclient_type_mock.return_value = wrapper_mock

    return wrapper_mock


@pytest.fixture
def failing_sshclient_stub(sshclient_type_mock):
    ssh_stub = CmdSpecificSSHClientStub({
        "sbatch": ChannelFileStub(lines=["1234"]),
        "sacct": ChannelFileStub(lines=get_error_lines())
    })

    wrapper_mock = Mock(wraps=ssh_stub)
    sshclient_type_mock.return_value = wrapper_mock

    return wrapper_mock


@pytest.fixture(autouse=True)
def osfs_type_mock():
    patcher = patch("fs.osfs.OSFS")
    yield patcher.start()

    patcher.stop()


@pytest.fixture(autouse=True)
def sshfs_type_mock():
    patcher = patch(
        "ssh_slurm_runner.chmodsshfs.PermissionChangingSSHFSDecorator")

    yield patcher.start()

    patcher.stop()


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


def make_options_with_files_to_copy_and_clean(files_to_copy, files_to_clean):
    options = LaunchOptions(
        host="example.com",
        user="myuser",
        password="mypassword",
        private_key="PRIVATE",
        private_keyfile="my_private_keyfile",
        sbatch="test.job",
        poll_interval=0,
        copy_files=files_to_copy,
        clean_files=files_to_clean
    )

    return options


HOME_DIR = "/home/myuser"
INPUT_AND_EXPECTED_KEYFILE_PATHS = [
    ("my_private_keyfile", "my_private_keyfile"),
    ("~/.ssh/private_keyfile", f"{HOME_DIR}/.ssh/private_keyfile"),
    ("~/~folder~/private_keyfile", f"{HOME_DIR}/~folder~/private_keyfile"),
    ("~folder~/private_keyfile", f"~folder~/private_keyfile")
]


@pytest.mark.parametrize(["input_keyfile", "expected_keyfile"], INPUT_AND_EXPECTED_KEYFILE_PATHS)
def test__given_valid_config__when_running__should_run_sbatch_over_ssh(sshclient_type_mock,
                                                                       valid_options: LaunchOptions,
                                                                       input_keyfile: str,
                                                                       expected_keyfile: str):
    os.environ['HOME'] = HOME_DIR
    valid_options = replace(
        valid_options, private_keyfile=input_keyfile)

    sshclient_mock = SSHClientMock(
        cmd_to_channels={
            "sbatch": ChannelFileStub(lines=["1234"]),
            "sacct": ChannelFileStub(lines=get_success_lines())
        },
        launch_options=valid_options,
        host_key_file=f"{HOME_DIR}/.ssh/known_hosts",
        private_keyfile_abspath=expected_keyfile)

    sshclient_type_mock.return_value = sshclient_mock
    sut = Application(Mock())

    sut.run(valid_options)

    sshclient_mock.verify()


def test__given_ssh_connection_not_available_for_executor__when_running__should_log_error_and_exit(valid_options, sshclient_type_mock):
    ssh_client_mock = sshclient_type_mock.return_value
    ssh_client_mock.connect.side_effect = SSHError(valid_options.host)

    ui_spy = Mock()
    sut = Application(ui_spy)

    sut.run(valid_options)

    ssh_client_mock.exec_command.assert_not_called()
    ui_spy.error.assert_called_once_with(f"SSHError: {valid_options.host}")


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_valid_config__when_running__should_open_local_fs_in_current_directory(
        valid_options: LaunchOptions, osfs_type_mock):

    sut = Application(Mock())

    sut.run(valid_options)

    osfs_type_mock.assert_called_with(".")


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_valid_config__when_running__should_login_to_sshfs_with_correct_credentials(
        valid_options: LaunchOptions, sshfs_type_mock):

    sut = Application(Mock())

    sut.run(valid_options)

    sshfs_type_mock.assert_called_with(
        valid_options.host, user=valid_options.user, passwd=valid_options.password, pkey=valid_options.private_key)


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_ssh_connection_not_available_for_sshfs__when_running__should_log_error_and_exit(valid_options, sshfs_type_mock):
    sshfs_type_mock.side_effect = SSHError(valid_options.host)

    ui_spy = Mock()
    sut = Application(ui_spy)

    sut.run(valid_options)

    ui_spy.error.assert_called_once_with(f"SSHError: {valid_options.host}")


@pytest.mark.usefixtures("successful_sshclient_stub")
@pytest.mark.parametrize(["input_keyfile", "expected_keyfile"], INPUT_AND_EXPECTED_KEYFILE_PATHS)
def test__given_config_with_only_private_keyfile__when_running__should_login_to_sshfs_with_correct_credentials(sshfs_type_mock,
                                                                                                               input_keyfile,
                                                                                                               expected_keyfile):
    valid_options = LaunchOptions(
        host="example.com",
        user="myuser",
        private_keyfile=input_keyfile,
        sbatch="test.job",
        poll_interval=0
    )

    sut = Application(Mock())

    sut.run(valid_options)

    sshfs_type_mock.assert_called_with(
        valid_options.host, user=valid_options.user, passwd=valid_options.password, pkey=expected_keyfile)


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_config_with_only_password__when_running__should_login_to_sshfs_with_correct_credentials(sshfs_type_mock):
    valid_options = LaunchOptions(
        host="example.com",
        user="myuser",
        password="mypassword",
        sbatch="test.job",
        poll_interval=0
    )

    sut = Application(Mock())

    sut.run(valid_options)

    sshfs_type_mock.assert_called_with(
        valid_options.host, user=valid_options.user, passwd=valid_options.password, pkey=valid_options.private_key)


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_config__when_running__should_open_sshfs_in_home_dir(sshfs_type_mock: MagicMock,
                                                                    valid_options: LaunchOptions,):
    sut = Application(Mock())

    sut.run(valid_options)

    sshfs_mock: MagicMock = sshfs_type_mock.return_value
    method_name, args, _ = sshfs_mock.mock_calls[0]

    assert method_name == "opendir"
    assert args == (HOME_DIR,)


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_config_with_files_to_copy__when_running__should_copy_files_to_remote_filesystem(osfs_type_mock,
                                                                                                sshfs_type_mock,
                                                                                                fs_copy_file_mock):
    options = make_options_with_files_to_copy([
        CopyInstruction("myfile.txt", "mycopy.txt"),
        CopyInstruction("otherfile.gif", "copy.gif")
    ])

    osfs_type_mock.return_value = PyFilesystemStub(
        ["myfile.txt", "otherfile.gif"])

    filesystem_fake = PyFilesystemFake()
    sshfs_type_mock.return_value = filesystem_fake

    fs_copy_file_mock.side_effect = copy_file_between_filesystems_fake

    sut = Application(Mock())

    sut.run(options)

    assert "mycopy.txt" in filesystem_fake.existing_files
    assert "copy.gif" in filesystem_fake.existing_files


def test__given_config_with_files_to_copy__when_running__should_copy_files_to_remote_filesystem_before_running_job(osfs_type_mock,
                                                                                                                   sshfs_type_mock,
                                                                                                                   fs_copy_file_mock,
                                                                                                                   successful_sshclient_stub):
    options = make_options_with_files_to_copy(
        [CopyInstruction("myfile.txt", "mycopy.txt")])

    osfs_type_mock.return_value = PyFilesystemStub(["myfile.txt"])
    sshfs_type_mock.return_value = PyFilesystemFake()

    call_order, call_logger = make_call_logger_with_capture()
    fs_copy_file_mock.side_effect = call_logger("copy_file")
    successful_sshclient_stub.exec_command.side_effect = call_logger(
        "exec_command")

    sut = Application(Mock())

    sut.run(options)

    first_two_calls = call_order[:2]
    assert first_two_calls == ["copy_file", "exec_command"]


@pytest.mark.usefixtures("sshclient_type_mock")
def test__given_config_with_non_existing_file_to_copy__when_running__should_perform_rollback_and_exit(osfs_type_mock,
                                                                                                      sshfs_type_mock,
                                                                                                      fs_copy_file_mock):
    options = make_options_with_files_to_copy([
        CopyInstruction("myfile.txt", "mycopy.txt"),
        CopyInstruction("otherfile.gif", "copy.gif")
    ])
    osfs_type_mock.return_value = PyFilesystemStub(["myfile.txt"])

    sshfs_fake = PyFilesystemFake()
    sshfs_type_mock.return_value = sshfs_fake
    fs_copy_file_mock.side_effect = copy_file_between_filesystems_fake

    sut = Application(Mock())

    exit_code = sut.run(options)

    assert "mycopy.txt" not in sshfs_fake.existing_files
    assert exit_code == 1


@pytest.mark.usefixtures("sshclient_type_mock", "fs_copy_file_mock")
def test__given_config_with_non_existing_file_to_copy__when_running__should_print_to_ui(osfs_type_mock,
                                                                                        sshfs_type_mock):
    options = make_options_with_files_to_copy([
        CopyInstruction("myfile.txt", "mycopy.txt"),
        CopyInstruction("otherfile.gif", "copy.gif")
    ])
    osfs_type_mock.return_value = PyFilesystemStub(["myfile.txt"])

    sshfs_fake = PyFilesystemFake()
    sshfs_type_mock.return_value = sshfs_fake

    ui_spy = Mock()
    sut = Application(ui_spy)

    sut.run(options)

    assert call.error(
        "FileNotFoundError: otherfile.gif") in ui_spy.method_calls


@pytest.mark.usefixtures("sshclient_type_mock")
def test__given_config_with_already_existing_file_to_copy__when_running__should_perform_rollback_and_exit(osfs_type_mock,
                                                                                                          sshfs_type_mock,
                                                                                                          fs_copy_file_mock):
    options = make_options_with_files_to_copy([
        CopyInstruction("myfile.txt", "mycopy.txt"),
        CopyInstruction("otherfile.gif", "copy.gif")
    ])
    osfs_type_mock.return_value = PyFilesystemStub(
        ["myfile.txt", "otherfile.gif"])

    sshfs_fake = PyFilesystemFake(["copy.gif"])
    sshfs_type_mock.return_value = sshfs_fake
    fs_copy_file_mock.side_effect = copy_file_between_filesystems_fake

    sut = Application(Mock())

    exit_code = sut.run(options)

    assert "mycopy.txt" not in sshfs_fake.existing_files
    assert "copy.gif" in sshfs_fake.existing_files
    assert exit_code == 1


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_config_with_files_to_clean__when_running__should_remove_files_from_remote_filesystem(osfs_type_mock,
                                                                                                     sshfs_type_mock,
                                                                                                     fs_copy_file_mock):
    options = make_options_with_files_to_copy_and_clean(
        [CopyInstruction("myfile.txt", "mycopy.txt")],
        ["mycopy.txt"]
    )

    osfs_type_mock.return_value = PyFilesystemStub(["myfile.txt"])

    filesystem_fake = PyFilesystemFake()
    sshfs_type_mock.return_value = filesystem_fake
    fs_copy_file_mock.side_effect = copy_file_between_filesystems_fake

    sut = Application(Mock())

    sut.run(options)

    assert "mycopy.txt" not in filesystem_fake.existing_files


def test__given_config_with_files_to_clean__when_running__should_clean_files_to_remote_filesystem_after_completing_job(osfs_type_mock,
                                                                                                                       sshfs_type_mock,
                                                                                                                       fs_copy_file_mock,
                                                                                                                       successful_sshclient_stub):
    options = make_options_with_files_to_copy_and_clean(
        [CopyInstruction("myfile.txt", "mycopy.txt")],
        ["mycopy.txt"]
    )

    call_order, call_logger = make_call_logger_with_capture()

    osfs_type_mock.return_value = PyFilesystemStub(["myfile.txt"])

    pyfilesystem_wrapper_mock = MagicMock(wraps=PyFilesystemFake())
    pyfilesystem_wrapper_mock.opendir.return_value = pyfilesystem_wrapper_mock
    pyfilesystem_wrapper_mock.remove.side_effect = call_logger("remove")
    sshfs_type_mock.return_value = pyfilesystem_wrapper_mock

    fs_copy_file_mock.side_effect = copy_file_between_filesystems_fake
    successful_sshclient_stub.exec_command.side_effect = call_logger(
        "exec_command")

    sut = Application(Mock())

    sut.run(options)

    assert call_order == ["exec_command", "exec_command", "remove"]


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_config_with_files_to_collect__when_running__should_collect_files_from_remote_filesystem_after_completing_job_and_before_cleaning(osfs_type_mock,
                                                                                                                                                 sshfs_type_mock,
                                                                                                                                                 fs_copy_file_mock):
    options = dataclasses.replace(make_options_with_files_to_copy_and_clean(
        [CopyInstruction("myfile.txt", "mycopy.txt")],
        ["mycopy.txt"]
    ), collect_files=[CopyInstruction("mycopy.txt", "mycopy.txt")])

    local_fs_fake = PyFilesystemFake(["myfile.txt"])
    osfs_type_mock.return_value = local_fs_fake

    ssh_fs_fake = PyFilesystemFake()
    sshfs_type_mock.return_value = ssh_fs_fake
    fs_copy_file_mock.side_effect = copy_file_between_filesystems_fake

    sut = Application(Mock())

    sut.run(options)

    assert "mycopy.txt" in local_fs_fake.existing_files
    assert "mycopy.txt" not in ssh_fs_fake.existing_files


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_valid_config__when_sbatch_job_succeeds__should_return_exit_code_zero(valid_options: LaunchOptions):
    sut = Application(Mock())

    actual = sut.run(valid_options)

    assert actual == 0


@pytest.mark.usefixtures("failing_sshclient_stub")
def test__given_valid_config__when_sbatch_job_fails__should_return_exit_code_one(valid_options: LaunchOptions):

    sut = Application(Mock())

    actual = sut.run(valid_options)

    assert actual == 1


def test__given_valid_config__when_running_long_running_job__should_wait_for_completion(sshclient_type_mock,
                                                                                        valid_options: LaunchOptions):

    sut = Application(Mock())

    channel_spy = DelayedChannelSpy(exit_code=1, calls_until_exit=2)
    sshclient_type_mock.return_value = CmdSpecificSSHClientStub({
        "sbatch": ChannelFileStub(lines=["1234"]),
        "sacct": ChannelFileStub(
            lines=get_success_lines(),
            channel=channel_spy
        )
    })

    actual = sut.run(valid_options)

    assert actual == 0
    assert channel_spy.times_called == 2


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_ui__when_running__should_update_ui_after_polling(valid_options: LaunchOptions):
    ui_spy = Mock()
    sut = Application(ui_spy)

    _ = sut.run(valid_options)

    ui_spy.update.assert_called_with(completed_slurm_job())


def test__given_running_application__when_canceling_after_polling_job__should_cancel_job(sshclient_type_mock, valid_options: LaunchOptions):
    from threading import Thread

    sut = Application(Mock())

    long_running = 1e10
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

    thread = Thread(target=lambda: sut.run(valid_options))
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

        return mock.DEFAULT

    return _mark_cmd_as_done_after_scancel


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


def make_call_logger_with_capture():
    call_order = []

    def call_logger(call: str):
        def _call_logger(*args, **kwargs):
            call_order.append(call)
            return mock.DEFAULT

        return _call_logger
    return call_order, call_logger
