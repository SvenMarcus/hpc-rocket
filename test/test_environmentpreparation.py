from typing import List, Optional
from unittest.mock import MagicMock, call

import pytest
from hpcrocket.core.environmentpreparation import (
    CopyInstruction,
    EnvironmentCleaner,
    EnvironmentCollector,
    EnvironmentPreparation,
)
from hpcrocket.core.filesystem import Filesystem
from test.testdoubles.filesystem import MemoryFilesystemFake


def new_filesystem(files: Optional[List[str]] = None) -> Filesystem:
    return MemoryFilesystemFake(files or [])


def test__given_files_to_copy__but_not_preparing__should_not_do_anything() -> None:
    source_fs = new_filesystem(["file1.txt"])
    target_fs = new_filesystem()

    _ = EnvironmentPreparation(
        source_fs, target_fs, [CopyInstruction("file1.txt", "file2.txt")]
    )

    assert target_fs.exists("file2.txt") is False


def test__given_files_to_copy__when_preparing__should_copy_files() -> None:
    source_fs = new_filesystem(["file.txt", "funny.gif"])
    target_fs = new_filesystem(["evenfunnier.gif"])

    sut = EnvironmentPreparation(
        source_fs,
        target_fs,
        [
            CopyInstruction("file.txt", "filecopy.txt"),
            CopyInstruction("funny.gif", "evenfunnier.gif", overwrite=True),
        ],
    )

    sut.prepare()

    assert target_fs.exists("filecopy.txt")
    assert target_fs.exists("evenfunnier.gif")


def test__given_files_to_copy_with_non_existing_file__when_preparing_then_rollback__should_remove_copied_files_from_target_fs() -> None:
    source_fs_spy = new_filesystem(["funny.gif"])
    target_fs = new_filesystem()

    sut = EnvironmentPreparation(
        source_fs_spy,
        target_fs,
        [
            CopyInstruction("file.txt", "filecopy.txt"),
            CopyInstruction("funny.gif", "evenfunnier.gif"),
        ],
    )

    with pytest.raises(FileNotFoundError):
        sut.prepare()

    sut.rollback()

    assert target_fs.exists("filecopy.txt") is False


def test__given_copied_file_not_on_target_fs__when_rolling_back__should_remove_remaining_copied_files_from_target_fs() -> None:
    source_fs_spy = new_filesystem(["file.txt", "funny.gif"])
    target_fs = new_filesystem()

    sut = EnvironmentPreparation(
        source_fs_spy,
        target_fs,
        [
            CopyInstruction("file.txt", "filecopy.txt"),
            CopyInstruction("funny.gif", "evenfunnier.gif"),
        ],
    )

    sut.prepare()
    target_fs.delete("filecopy.txt")

    sut.rollback()

    assert target_fs.exists("evenfunnier.gif") is False


def test__given_copied_with_glob_but_file_exists_on_target__when_rolling_back__it_removes_copied_files_from_target() -> None:
    exists_on_target = "exists_on_target.txt"
    source_fs = new_filesystem(["file.txt", exists_on_target])
    target_fs = new_filesystem([exists_on_target])

    sut = EnvironmentPreparation(source_fs, target_fs, [CopyInstruction("*.txt", "")])

    with pytest.raises(FileExistsError):
        sut.prepare()

    sut.rollback()

    assert target_fs.exists("file.txt") is False


def test__given_rollback_done__when_rolling_back_again__should_not_do_anything() -> None:
    source_fs_spy = new_filesystem(["file.txt", "funny.gif"])
    target_fs = MagicMock(wraps=new_filesystem())

    sut = EnvironmentPreparation(
        source_fs_spy,
        target_fs,
        [
            CopyInstruction("file.txt", "filecopy.txt"),
            CopyInstruction("funny.gif", "evenfunnier.gif"),
        ],
    )

    sut.prepare()
    sut.rollback()
    target_fs.reset_mock()

    sut.rollback()

    target_fs.delete.assert_not_called()


def test__given_rollback_done_with_file_not_found__when_rolling_back_again__should_try_to_delete_remaining_files() -> None:
    source_fs_spy = new_filesystem(["file.txt", "funny.gif"])
    target_fs_spy = MagicMock(wraps=new_filesystem())

    sut = EnvironmentPreparation(
        source_fs_spy,
        target_fs_spy,
        [
            CopyInstruction("file.txt", "filecopy.txt"),
            CopyInstruction("funny.gif", "evenfunnier.gif"),
        ],
    )

    sut.prepare()
    target_fs_spy.delete("filecopy.txt")
    sut.rollback()

    target_fs_spy.reset_mock()
    sut.rollback()

    target_fs_spy.delete.assert_has_calls([call("filecopy.txt")])


def test__given_files_to_clean__but_not_cleaning__should_not_do_anything() -> None:
    target_fs_spy = new_filesystem(["file1.txt"])

    _ = EnvironmentCleaner(target_fs_spy, ["file1.txt"])

    assert target_fs_spy.exists("file1.txt")


def test__given_files_to_clean__when_cleaning__should_delete_files() -> None:
    target_fs_spy = new_filesystem(["file.txt", "funny.gif"])

    sut = EnvironmentCleaner(
        target_fs_spy,
        [
            "file.txt",
            "funny.gif",
        ],
    )

    sut.clean()

    assert target_fs_spy.exists("file.txt") is False
    assert target_fs_spy.exists("funny.gif") is False


def test__given_files_to_clean_with_non_existing_files__when_cleaning__should_still_clean_remaining_files() -> None:
    target_fs = new_filesystem(["funny.gif"])

    sut = EnvironmentCleaner(
        target_fs,
        [
            "file.txt",
            "funny.gif",
        ],
    )

    sut.clean()

    assert target_fs.exists("funny.gif") is False


def test__given_files_to_clean_with_non_existing_files__when_cleaning__should_log_error_to_ui() -> None:
    target_fs = new_filesystem(["funny.gif"])

    ui_spy = MagicMock()
    sut = EnvironmentCleaner(
        target_fs,
        [
            "file.txt",
            "funny.gif",
        ],
        ui_spy,
    )

    sut.clean()

    ui_spy.error.assert_called_with("FileNotFoundError: Cannot delete file 'file.txt'")


def test__given_files_to_collect__when_collect__should_copy_to_source_fs() -> None:
    local_fs = new_filesystem(["copy_file.txt"])
    remote_fs = new_filesystem(["file.txt", "funny.gif"])

    sut = EnvironmentCollector(
        remote_fs,
        local_fs,
        [
            CopyInstruction("file.txt", "copy_file.txt", True),
            CopyInstruction("funny.gif", "copy_funny.gif", False),
        ],
    )

    sut.collect()

    assert local_fs.exists("copy_file.txt")
    assert local_fs.exists("copy_funny.gif")


def test__given_files_to_collect_with_non_existing_file__when_collecting__should_collect_remaining_files() -> None:
    local_fs = new_filesystem()
    remote_fs = new_filesystem(["funny.gif"])

    sut = EnvironmentCollector(
        remote_fs,
        local_fs,
        [
            CopyInstruction("file.txt", "copy_file.txt"),
            CopyInstruction("funny.gif", "copy_funny.gif"),
        ],
    )

    sut.collect()

    assert local_fs.exists("copy_funny.gif")


def test__given_files_to_collect_with_non_existing_file__when_collecting__should_log_error_to_ui() -> None:
    local_fs = new_filesystem()
    remote_fs = new_filesystem(["funny.gif"])

    ui_spy = MagicMock()
    sut = EnvironmentCollector(
        remote_fs,
        local_fs,
        [
            CopyInstruction("file.txt", "copy_file.txt", True),
            CopyInstruction("funny.gif", "copy_funny.gif", False),
        ],
        ui_spy,
    )

    sut.collect()

    ui_spy.error.assert_called_with("FileNotFoundError: Cannot copy file 'file.txt'")


def test__given_files_to_collect_with_file_already_existing_on_source_fs__when_collecting__should_still_collect_remaining_files() -> None:
    local_fs = new_filesystem(["copy_file.txt"])
    remote_fs = new_filesystem(["file.txt", "funny.gif"])

    sut = EnvironmentCollector(
        remote_fs,
        local_fs,
        [
            CopyInstruction("file.txt", "copy_file.txt", False),
            CopyInstruction("funny.gif", "copy_funny.gif", False),
        ],
    )

    sut.collect()

    assert local_fs.exists("copy_funny.gif")


def test__given_files_to_collect_with_file_already_existing_on_source_fs__when_collecting__should_log_error_to_ui() -> None:
    local_fs = new_filesystem(["copy_file.txt"])
    remote_fs = new_filesystem(["file.txt", "funny.gif"])

    ui_spy = MagicMock()
    sut = EnvironmentCollector(
        remote_fs,
        local_fs,
        [
            CopyInstruction("file.txt", "copy_file.txt", False),
            CopyInstruction("funny.gif", "copy_funny.gif", False),
        ],
        ui_spy,
    )

    sut.collect()

    ui_spy.error.assert_called_with("FileExistsError: Cannot copy file 'file.txt'")


def test__given_files_to_collect_with_glob_and_existing_file_on_source_fs__when_collecting__collects_remaining_files() -> None:
    local_fs = new_filesystem(["existing.txt"])
    remote_fs = new_filesystem(["existing.txt", "funny.txt"])

    sut = EnvironmentCollector(
        remote_fs, local_fs, [CopyInstruction("*.txt", "", False)]
    )

    sut.collect()

    assert local_fs.exists("funny.txt")
