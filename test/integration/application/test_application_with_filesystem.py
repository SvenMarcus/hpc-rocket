from __future__ import annotations

import os
from dataclasses import replace
from typing import Callable, Generator
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import pytest

from hpcrocket.core.application import Application
from hpcrocket.core.filesystem.progressive import CopyInstruction
from hpcrocket.core.launchoptions import LaunchOptions, Options
from hpcrocket.pyfilesystem.factory import PyFilesystemFactory
from hpcrocket.ssh.connectiondata import ConnectionData
from hpcrocket.ssh.errors import SSHError
from hpcrocket.ui import UI
from test.application.optionbuilders import (
    launch_options,
    launch_options_with_proxy_only_password,
    main_connection,
    main_connection_only_password,
    proxy_connection_only_password,
)
from test.integration.application.fixtures import (
    HOME_DIR,
    INPUT_AND_EXPECTED_KEYFILE_PATHS,
    sshclient_type_mock,  # noqa: F401
    osfs_type_mock,  # noqa: F401
    sshfs_type_mock,  # noqa: F401
)
from test.integration.pyfilesystem.sshfilesystem_assertions import (
    assert_sshfs_connected_with_connection_data,
    assert_sshfs_connected_with_keyfile_from_connection_data,
    assert_sshfs_connected_with_password_from_connection_data,
)
from test.testdoubles.executor import SlurmJobExecutorSpy
from test.testdoubles.filesystem import sshfs_with_connection_fake
from test.testdoubles.sshclient import ProxyJumpVerifyingSSHClient


@pytest.fixture(autouse=True)
def patch_cwd() -> Generator[Callable[[], str], None, None]:
    def getcwd() -> str:
        return "/"

    with patch("os.getcwd", getcwd) as patched:
        yield patched


def make_sut(options: Options, ui: UI | None = None) -> Application:
    return Application(SlurmJobExecutorSpy(), PyFilesystemFactory(options), ui or Mock())


def test__given_valid_config__when_running__should_login_to_sshfs_with_correct_credentials(
    sshfs_type_mock: MagicMock | AsyncMock,
) -> None:
    sut = make_sut(launch_options())

    sut.run(launch_options())

    assert_sshfs_connected_with_connection_data(sshfs_type_mock, main_connection())


def test__given_ssh_connection_not_available_for_sshfs__when_running__should_log_error_and_exit(
    sshfs_type_mock: MagicMock | AsyncMock,
) -> None:
    sshfs_type_mock.side_effect = SSHError(main_connection().hostname)

    ui_spy = Mock()
    sut = make_sut(launch_options(), ui_spy)

    sut.run(launch_options())

    ui_spy.error.assert_called_once_with(f"SSHError: {main_connection().hostname}")


@pytest.mark.parametrize(["input_keyfile", "expected_keyfile"], INPUT_AND_EXPECTED_KEYFILE_PATHS)
def test__given_config_with_only_private_keyfile__when_running__should_login_to_sshfs_with_correct_credentials(
    sshfs_type_mock: MagicMock | AsyncMock, input_keyfile: str, expected_keyfile: str
) -> None:
    os.environ["HOME"] = HOME_DIR
    valid_options = LaunchOptions(
        connection=ConnectionData(hostname="example.com", username="myuser", keyfile=input_keyfile),
        sbatch="test.job",
        poll_interval=0,
    )

    sut = make_sut(valid_options)

    sut.run(valid_options)

    connection_with_resolved_keyfile = replace(valid_options.connection, keyfile=expected_keyfile)
    assert_sshfs_connected_with_keyfile_from_connection_data(sshfs_type_mock, connection_with_resolved_keyfile)


def test__given_config_with_only_password__when_running__should_login_to_sshfs_with_correct_credentials(
    sshfs_type_mock: MagicMock | AsyncMock,
) -> None:
    valid_options = LaunchOptions(
        connection=ConnectionData(hostname="example.com", username="myuser", password="mypassword"),
        sbatch="test.job",
        poll_interval=0,
    )

    sut = make_sut(valid_options)

    sut.run(valid_options)

    assert_sshfs_connected_with_password_from_connection_data(sshfs_type_mock, valid_options.connection)


def test__given_config_with_proxy__when_running__should_login_to_sshfs_over_proxy(
    sshclient_type_mock: MagicMock | AsyncMock,
) -> None:
    # NOTE: We're using only password authentication here, because SSHFS combines key and keyfile into a single option
    #       so we cannot compare against connection data with keyfile AND key as SSHFS will only be called with one of them.

    mock = Mock(wraps=ProxyJumpVerifyingSSHClient(main_connection_only_password(), [proxy_connection_only_password()]))
    sshclient_type_mock.return_value = mock

    with sshfs_with_connection_fake(sshclient_type_mock.return_value):
        sut = make_sut(launch_options_with_proxy_only_password())

        sut.run(launch_options_with_proxy_only_password())

        mock.verify()


def test__given_config_with_files_to_copy__when_running__should_copy_files_to_remote_filesystem(
    osfs_type_mock: MagicMock | AsyncMock, sshfs_type_mock: MagicMock | AsyncMock
) -> None:
    opts = launch_options(
        copy=[
            CopyInstruction("myfile.txt", "mycopy.txt"),
            CopyInstruction("otherfile.gif", "copy.gif"),
        ]
    )

    osfs_type_mock.create("myfile.txt")
    osfs_type_mock.create("otherfile.gif")

    sut = make_sut(opts)

    sut.run(opts)

    assert sshfs_type_mock.exists(f"{HOME_DIR}/mycopy.txt")
    assert sshfs_type_mock.exists(f"{HOME_DIR}/copy.gif")


def test__given_config_with_files_to_clean__when_running__should_remove_files_from_remote_filesystem(
    osfs_type_mock: MagicMock | AsyncMock, sshfs_type_mock: MagicMock | AsyncMock
) -> None:
    opts = launch_options(
        watch=True,
        copy=[CopyInstruction("myfile.txt", "mycopy.txt")],
        clean=["mycopy.txt"],
    )

    osfs_type_mock.return_value.create("myfile.txt")

    sut = make_sut(opts)

    sut.run(opts)

    assert not sshfs_type_mock.return_value.exists(f"{HOME_DIR}/mycopy.txt")


def test__given_config_with_files_to_collect__when_running__should_collect_files_from_remote_filesystem_after_completing_job_and_before_cleaning(
    osfs_type_mock: MagicMock | AsyncMock, sshfs_type_mock: MagicMock | AsyncMock
) -> None:
    opts = launch_options(
        watch=True,
        copy=[CopyInstruction("myfile.txt", "mycopy.txt")],
        clean=["mycopy.txt"],
        collect=[CopyInstruction("mycopy.txt", "mycopy.txt")],
    )

    local_fs = osfs_type_mock.return_value
    local_fs.create("myfile.txt")

    sut = make_sut(opts)

    sut.run(opts)

    sshfs = sshfs_type_mock.return_value
    assert local_fs.exists("mycopy.txt")
    assert not sshfs.exists("mycopy.txt")


@pytest.mark.usefixtures("sshclient_type_mock")
def test__given_config_with_non_existing_file_to_copy__when_running__should_perform_rollback_and_exit(
    osfs_type_mock: MagicMock | AsyncMock, sshfs_type_mock: MagicMock | AsyncMock
) -> None:
    opts = launch_options(
        watch=True,
        copy=[
            CopyInstruction("myfile.txt", "mycopy.txt"),
            CopyInstruction("otherfile.gif", "copy.gif"),
        ],
    )

    osfs_type_mock.return_value.create("myfile.txt")

    sut = make_sut(opts)

    exit_code = sut.run(opts)

    assert not sshfs_type_mock.return_value.exists(f"{HOME_DIR}/mycopy.txt")
    assert not sshfs_type_mock.return_value.exists(f"{HOME_DIR}/copy.gif")
    assert exit_code == 1


@pytest.mark.usefixtures("sshclient_type_mock", "sshfs_type_mock")
def test__given_config_with_non_existing_file_to_copy__when_running__should_print_to_ui(
    osfs_type_mock: MagicMock | AsyncMock,
) -> None:
    opts = launch_options(
        copy=[
            CopyInstruction("myfile.txt", "mycopy.txt"),
            CopyInstruction("otherfile.gif", "copy.gif"),
        ]
    )

    osfs_type_mock.return_value.create("myfile.txt")

    ui_spy = Mock()
    sut = make_sut(opts, ui_spy)

    sut.run(opts)

    assert call.error("FileNotFoundError: otherfile.gif") in ui_spy.method_calls


@pytest.mark.usefixtures("sshclient_type_mock")
def test__given_config_with_already_existing_file_to_copy__when_running__should_perform_rollback_and_exit(
    osfs_type_mock: MagicMock | AsyncMock, sshfs_type_mock: MagicMock | AsyncMock
) -> None:
    opts = launch_options(
        copy=[
            CopyInstruction("myfile.txt", "mycopy.txt"),
            CopyInstruction("otherfile.gif", "copy.gif"),
        ]
    )

    osfs_type_mock.return_value.create("myfile.txt")
    osfs_type_mock.return_value.create("otherfile.gif")

    sshfs_type_mock.return_value.create(f"{HOME_DIR}/copy.gif")

    sut = make_sut(opts)

    exit_code = sut.run(opts)

    assert not sshfs_type_mock.return_value.exists(f"{HOME_DIR}/mycopy.txt")
    assert sshfs_type_mock.return_value.exists(f"{HOME_DIR}/copy.gif")
    assert exit_code == 1
