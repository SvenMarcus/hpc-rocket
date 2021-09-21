import dataclasses
import os
from dataclasses import replace
from test.pyfilesystem_testdoubles import (ArbitraryArgsMemoryFS,
                                           OnlySubFSMemoryFS)
from test.sshclient_testdoubles import (ChannelFileStub, ChannelStub,
                                        CmdSpecificSSHClientStub,
                                        DelayedChannelSpy, SSHClientMock,
                                        mock_iterating_sshclient_side_effect)
from test.sshfilesystem_assertions import (
    assert_sshfs_connected_with_connection_data,
    assert_sshfs_connected_with_keyfile_from_connection_data,
    assert_sshfs_connected_with_password_from_connection_data)
from test.test__sshexecutor import (assert_channel_opened,
                                    assert_connected_with_data,
                                    proxy_mock_with_transport)
from unittest import mock
from unittest.mock import ANY, MagicMock, Mock, call, patch

import pytest
from fs.memoryfs import MemoryFS
from hpcrocket.core.application import Application
from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.core.slurmrunner import SlurmJob, SlurmTask
from hpcrocket.ssh.errors import SSHError
from hpcrocket.ssh.sshexecutor import ConnectionData


@pytest.fixture
def sshclient_type_mock():
    patcher = patch("paramiko.SSHClient")
    type_mock = patcher.start()

    yield type_mock

    patcher.stop()


@pytest.fixture
def valid_options():
    return LaunchOptions(
        connection=ConnectionData(
            hostname="example.com",
            username="myuser",
            password="mypassword",
            key="PRIVATE",
            keyfile="my_private_keyfile",
        ),
        sbatch="test.job",
        poll_interval=0
    )


@pytest.fixture
def options_with_proxy(valid_options):
    proxy_connection = ConnectionData(
        hostname="proxy1-host",
        username="proxy1-user",
        password="proxy1-pass",
        keyfile="~/proxy1-keyfile"
    )

    return replace(valid_options, proxyjumps=[proxy_connection])


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
    return make_successful_sshclient(sshclient_type_mock)


def make_successful_sshclient(sshclient_type_mock):
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
    osfs_type_mock = patcher.start()
    osfs_type_mock.return_value = Mock(
        spec=MemoryFS, wraps=ArbitraryArgsMemoryFS())
    yield osfs_type_mock

    patcher.stop()


@pytest.fixture(autouse=True)
def sshfs_type_mock():
    patcher = patch(
        "hpcrocket.ssh.chmodsshfs.PermissionChangingSSHFSDecorator")

    sshfs_type_mock = patcher.start()
    mem_fs = OnlySubFSMemoryFS()
    mem_fs.makedirs(HOME_DIR)
    sshfs_type_mock.return_value = Mock(spec=MemoryFS, wraps=mem_fs)
    sshfs_type_mock.return_value.homedir = lambda: HOME_DIR

    yield sshfs_type_mock

    patcher.stop()


@pytest.fixture
def fs_copy_file_mock():
    patcher = patch("fs.copy.copy_file")
    yield patcher.start()

    patcher.stop()


def make_options_with_files_to_copy(files_to_copy):
    options = LaunchOptions(
        connection=ConnectionData(
            hostname="example.com",
            username="myuser",
            password="mypassword",
            key="PRIVATE",
            keyfile="my_private_keyfile"),
        sbatch="test.job",
        poll_interval=0,
        copy_files=files_to_copy
    )

    return options


def make_options_with_files_to_copy_and_clean(files_to_copy, files_to_clean):
    return dataclasses.replace(
        make_options_with_files_to_copy(files_to_copy),
        clean_files=files_to_clean)


def make_options_with_files_to_copy_collect_and_clean(files_to_copy, files_to_collect, files_to_clean):
    return dataclasses.replace(
        make_options_with_files_to_copy_and_clean(
            files_to_copy,
            files_to_clean),
        collect_files=files_to_collect)


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

    connection = replace(valid_options.connection, keyfile=input_keyfile)
    valid_options = replace(valid_options, connection=connection)

    sshclient_mock = SSHClientMock(
        cmd_to_channels={
            "sbatch": ChannelFileStub(lines=["1234"]),
            "sacct": ChannelFileStub(lines=get_success_lines())
        },
        launch_options=valid_options,
        private_keyfile_abspath=expected_keyfile)

    sshclient_type_mock.return_value = sshclient_mock
    sut = Application(Mock())

    sut.run(valid_options)

    sshclient_mock.verify()


def test__given_options_with_proxy_jumps__when_running__should_connect_to_executor_through_proxies(
        sshclient_type_mock, successful_sshclient_stub, options_with_proxy: LaunchOptions):

    proxy_connection = options_with_proxy.proxyjumps[0]

    proxy_mock, transport_channel = proxy_mock_with_transport()
    main_mock = successful_sshclient_stub

    mock_iterating_sshclient_side_effect(sshclient_type_mock, [main_mock, proxy_mock])

    sut = Application(Mock())

    sut.run(options_with_proxy)

    proxy_connection_with_resolved_keyfile = replace(proxy_connection, keyfile=f"{HOME_DIR}/proxy1-keyfile")
    assert_connected_with_data(proxy_mock, proxy_connection_with_resolved_keyfile)
    assert_channel_opened(proxy_mock.get_transport(), options_with_proxy.connection)
    assert_connected_with_data(main_mock, options_with_proxy.connection, channel=transport_channel)


def test__given_ssh_connection_not_available_for_executor__when_running__should_log_error_and_exit(
        valid_options, sshclient_type_mock):
    ssh_client_mock = sshclient_type_mock.return_value
    ssh_client_mock.connect.side_effect = SSHError(valid_options.connection.hostname)

    ui_spy = Mock()
    sut = Application(ui_spy)

    sut.run(valid_options)

    ssh_client_mock.exec_command.assert_not_called()
    ui_spy.error.assert_called_once_with(f"SSHError: {valid_options.connection.hostname}")


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

    assert_sshfs_connected_with_connection_data(sshfs_type_mock, valid_options.connection)


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_ssh_connection_not_available_for_sshfs__when_running__should_log_error_and_exit(
        valid_options, sshfs_type_mock):
    sshfs_type_mock.side_effect = SSHError(valid_options.connection.hostname)

    ui_spy = Mock()
    sut = Application(ui_spy)

    sut.run(valid_options)

    ui_spy.error.assert_called_once_with(f"SSHError: {valid_options.connection.hostname}")


@pytest.mark.usefixtures("successful_sshclient_stub")
@pytest.mark.parametrize(["input_keyfile", "expected_keyfile"], INPUT_AND_EXPECTED_KEYFILE_PATHS)
def test__given_config_with_only_private_keyfile__when_running__should_login_to_sshfs_with_correct_credentials(
        sshfs_type_mock, input_keyfile, expected_keyfile):

    os.environ['HOME'] = HOME_DIR
    valid_options = LaunchOptions(
        connection=ConnectionData(
            hostname="example.com",
            username="myuser",
            keyfile=input_keyfile),
        sbatch="test.job",
        poll_interval=0
    )

    sut = Application(Mock())

    sut.run(valid_options)

    connection_with_resolved_keyfile = replace(valid_options.connection, keyfile=expected_keyfile)
    assert_sshfs_connected_with_keyfile_from_connection_data(sshfs_type_mock, connection_with_resolved_keyfile)


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_config_with_only_password__when_running__should_login_to_sshfs_with_correct_credentials(sshfs_type_mock):
    valid_options = LaunchOptions(
        connection=ConnectionData(
            hostname="example.com",
            username="myuser",
            password="mypassword"),
        sbatch="test.job",
        poll_interval=0
    )

    sut = Application(Mock())

    sut.run(valid_options)

    assert_sshfs_connected_with_password_from_connection_data(sshfs_type_mock, valid_options.connection)


def test__given_config_with_proxy__when_running__should_login_to_sshfs_over_proxy(
        sshclient_type_mock, sshfs_type_mock, options_with_proxy):
    main_for_executor = make_successful_sshclient(sshclient_type_mock)

    proxy_connection = options_with_proxy.proxyjumps[0]
    proxy_mock, transport_channel = proxy_mock_with_transport()

    mock_iterating_sshclient_side_effect(sshclient_type_mock, [
        main_for_executor, Mock(),
        proxy_mock, Mock()  # create proxy client first, because sshfs will create its own client afterwards
    ])

    sut = Application(Mock())

    sut.run(options_with_proxy)

    proxy_connection_with_resolved_keyfile = replace(proxy_connection, keyfile=f"{HOME_DIR}/proxy1-keyfile")
    assert_connected_with_data(proxy_mock, proxy_connection_with_resolved_keyfile)
    assert_channel_opened(proxy_mock.get_transport(), options_with_proxy.connection)
    assert_sshfs_connected_with_connection_data(sshfs_type_mock, options_with_proxy.connection, transport_channel)


@pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_config__when_running__should_open_sshfs_in_home_dir(sshfs_type_mock: MagicMock,
                                                                    valid_options: LaunchOptions):
    sut = Application(Mock())

    sut.run(valid_options)

    sshfs_mock: MagicMock = sshfs_type_mock.return_value
    calls = sshfs_mock.mock_calls

    assert call.opendir(HOME_DIR, factory=ANY) in calls


@ pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_config_with_files_to_copy__when_running__should_copy_files_to_remote_filesystem(osfs_type_mock,
                                                                                                sshfs_type_mock):
    options = make_options_with_files_to_copy([
        CopyInstruction("myfile.txt", "mycopy.txt"),
        CopyInstruction("otherfile.gif", "copy.gif")
    ])

    osfs_type_mock.create("myfile.txt")
    osfs_type_mock.create("otherfile.gif")

    sut = Application(Mock())

    sut.run(options)

    assert sshfs_type_mock.exists(f"{HOME_DIR}/mycopy.txt")
    assert sshfs_type_mock.exists(f"{HOME_DIR}/copy.gif")


@ pytest.mark.usefixtures("sshfs_type_mock")
def test__given_config_with_files_to_copy__when_running__should_copy_files_to_remote_filesystem_before_running_job(
        osfs_type_mock, fs_copy_file_mock, successful_sshclient_stub):

    sut = Application(Mock())
    osfs_type_mock.return_value.create("myfile.txt")

    options = make_options_with_files_to_copy(
        [CopyInstruction("myfile.txt", "mycopy.txt")])

    call_order, call_logger = make_call_logger_with_capture()
    fs_copy_file_mock.side_effect = call_logger("copy_file")
    successful_sshclient_stub.exec_command.side_effect = call_logger(
        "exec_command")

    sut.run(options)

    first_two_calls = call_order[:2]
    assert first_two_calls == ["copy_file", "exec_command"]


@ pytest.mark.usefixtures("sshclient_type_mock")
def test__given_config_with_non_existing_file_to_copy__when_running__should_perform_rollback_and_exit(osfs_type_mock,
                                                                                                      sshfs_type_mock):
    options = make_options_with_files_to_copy([
        CopyInstruction("myfile.txt", "mycopy.txt"),
        CopyInstruction("otherfile.gif", "copy.gif")
    ])

    osfs_type_mock.return_value.create("myfile.txt")

    sut = Application(Mock())

    exit_code = sut.run(options)

    assert not sshfs_type_mock.return_value.exists(f"{HOME_DIR}/mycopy.txt")
    assert not sshfs_type_mock.return_value.exists(f"{HOME_DIR}/copy.gif")
    assert exit_code == 1


@ pytest.mark.usefixtures("sshclient_type_mock", "sshfs_type_mock")
def test__given_config_with_non_existing_file_to_copy__when_running__should_print_to_ui(osfs_type_mock):

    options = make_options_with_files_to_copy([
        CopyInstruction("myfile.txt", "mycopy.txt"),
        CopyInstruction("otherfile.gif", "copy.gif")
    ])

    osfs_type_mock.return_value.create("myfile.txt")

    ui_spy = Mock()
    sut = Application(ui_spy)

    sut.run(options)

    assert call.error(
        "FileNotFoundError: otherfile.gif") in ui_spy.method_calls


@ pytest.mark.usefixtures("sshclient_type_mock")
def test__given_config_with_already_existing_file_to_copy__when_running__should_perform_rollback_and_exit(
        osfs_type_mock, sshfs_type_mock):
    options = make_options_with_files_to_copy([
        CopyInstruction("myfile.txt", "mycopy.txt"),
        CopyInstruction("otherfile.gif", "copy.gif")
    ])

    osfs_type_mock.return_value.create("myfile.txt")
    osfs_type_mock.return_value.create("otherfile.gif")

    sshfs_type_mock.return_value.create(f"{HOME_DIR}/copy.gif")

    sut = Application(Mock())

    exit_code = sut.run(options)

    assert not sshfs_type_mock.return_value.exists(f"{HOME_DIR}/mycopy.txt")
    assert sshfs_type_mock.return_value.exists(f"{HOME_DIR}/copy.gif")
    assert exit_code == 1


@ pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_config_with_files_to_clean__when_running__should_remove_files_from_remote_filesystem(osfs_type_mock,
                                                                                                     sshfs_type_mock):
    options = make_options_with_files_to_copy_and_clean(
        [CopyInstruction("myfile.txt", "mycopy.txt")],
        ["mycopy.txt"]
    )

    osfs_type_mock.return_value.create("myfile.txt")

    sut = Application(Mock())

    sut.run(options)

    assert not sshfs_type_mock.return_value.exists(f"{HOME_DIR}/mycopy.txt")


def test__given_config_with_files_to_clean__when_running__should_clean_files_to_remote_filesystem_after_completing_job(
        osfs_type_mock, sshfs_type_mock, successful_sshclient_stub):
    options = make_options_with_files_to_copy_and_clean(
        [CopyInstruction("myfile.txt", "mycopy.txt")],
        ["mycopy.txt"]
    )

    call_order, call_logger = make_call_logger_with_capture()

    osfs_type_mock.return_value.create("myfile.txt")

    sshfs = sshfs_type_mock.return_value
    sshfs.opendir.return_value = sshfs
    sshfs.remove.side_effect = call_logger("remove")
    successful_sshclient_stub.exec_command.side_effect = call_logger(
        "exec_command")

    sut = Application(Mock())

    sut.run(options)

    assert call_order == ["exec_command", "exec_command", "remove"]


@ pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_config_with_files_to_collect__when_running__should_collect_files_from_remote_filesystem_after_completing_job_and_before_cleaning(osfs_type_mock,
                                                                                                                                                 sshfs_type_mock):
    options = make_options_with_files_to_copy_collect_and_clean(
        files_to_copy=[CopyInstruction("myfile.txt", "mycopy.txt")],
        files_to_clean=["mycopy.txt"],
        files_to_collect=[CopyInstruction("mycopy.txt", "mycopy.txt")]
    )

    local_fs = osfs_type_mock.return_value
    local_fs.create("myfile.txt")

    sut = Application(Mock())

    sut.run(options)

    sshfs = sshfs_type_mock.return_value
    assert local_fs.exists("mycopy.txt")
    assert not sshfs.exists("mycopy.txt")


@ pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_valid_config__when_sbatch_job_succeeds__should_return_exit_code_zero(valid_options: LaunchOptions):
    sut = Application(Mock())

    actual = sut.run(valid_options)

    assert actual == 0


@ pytest.mark.usefixtures("failing_sshclient_stub")
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


@ pytest.mark.usefixtures("successful_sshclient_stub")
def test__given_ui__when_running__should_update_ui_after_polling(valid_options: LaunchOptions):
    ui_spy = Mock()
    sut = Application(ui_spy)

    _ = sut.run(valid_options)

    ui_spy.update.assert_called_with(completed_slurm_job())


def test__given_running_application__when_canceling_after_polling_job__should_cancel_job(
        sshclient_type_mock, valid_options: LaunchOptions):
    from threading import Thread

    sut = Application(Mock())

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
