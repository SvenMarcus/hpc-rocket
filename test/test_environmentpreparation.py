from test.testdoubles.filesystem import MemoryFilesystemFake
from typing import List, Optional
from unittest.mock import MagicMock

from hpcrocket.core.environmentpreparation import (
    CopyInstruction,
    clean,
    collect,
    prepare,
)
from hpcrocket.core.filesystem import Filesystem


def new_filesystem(files: Optional[List[str]] = None) -> Filesystem:
    return MemoryFilesystemFake(files or [])


def test__given_files_to_copy__when_preparing__should_copy_files() -> None:
    source_fs = new_filesystem(["file.txt", "funny.gif"])
    target_fs = new_filesystem(["evenfunnier.gif"])

    result = prepare(
        source_fs,
        target_fs,
        [
            CopyInstruction("file.txt", "filecopy.txt"),
            CopyInstruction("funny.gif", "evenfunnier.gif", overwrite=True),
        ],
    )

    assert target_fs.exists("filecopy.txt")
    assert target_fs.exists("evenfunnier.gif")
    assert result.copied_files == ["filecopy.txt", "evenfunnier.gif"]


def test__given_files_to_copy_with_non_existing_file__when_preparing__returns_copied_files_and_error() -> None:
    source_fs_spy = new_filesystem(["file.txt"])
    target_fs = new_filesystem()

    result = prepare(
        source_fs_spy,
        target_fs,
        [
            CopyInstruction("file.txt", "filecopy.txt"),
            CopyInstruction("funny.gif", "evenfunnier.gif"),
        ],
    )

    assert result.copied_files == ["filecopy.txt"]
    assert isinstance(result.error, FileNotFoundError)


def test__given_files_to_copy_with_existing_file_on_target_fs__when_preparing__returns_copied_files_and_error() -> None:
    source_fs_spy = new_filesystem(["file.txt", "funny.gif"])
    target_fs = new_filesystem(["evenfunnier.gif"])

    result = prepare(
        source_fs_spy,
        target_fs,
        [
            CopyInstruction("file.txt", "filecopy.txt"),
            CopyInstruction("funny.gif", "evenfunnier.gif"),
        ],
    )

    assert result.copied_files == ["filecopy.txt"]
    assert isinstance(result.error, FileExistsError)


def test__given_files_to_clean__when_cleaning__should_delete_files() -> None:
    target_fs_spy = new_filesystem(["file.txt", "funny.gif"])

    clean(target_fs_spy, ["file.txt", "funny.gif"])

    assert target_fs_spy.exists("file.txt") is False
    assert target_fs_spy.exists("funny.gif") is False


def test__given_files_to_clean_with_non_existing_files__when_cleaning__should_still_clean_remaining_files() -> None:
    target_fs = new_filesystem(["funny.gif"])

    clean(target_fs, ["file.txt", "funny.gif"])

    assert target_fs.exists("funny.gif") is False


def test__given_files_to_clean_with_non_existing_files__when_cleaning__should_log_error_to_ui() -> None:
    target_fs = new_filesystem(["funny.gif"])

    ui_spy = MagicMock()
    clean(target_fs, ["file.txt", "funny.gif"], ui_spy)

    ui_spy.error.assert_called_with("FileNotFoundError: Cannot delete file 'file.txt'")


def test__given_files_to_collect__when_collect__should_copy_to_source_fs() -> None:
    local_fs = new_filesystem(["copy_file.txt"])
    remote_fs = new_filesystem(["file.txt", "funny.gif"])

    collect(
        remote_fs,
        local_fs,
        [
            CopyInstruction("file.txt", "copy_file.txt", True),
            CopyInstruction("funny.gif", "copy_funny.gif", False),
        ],
    )

    assert local_fs.exists("copy_file.txt")
    assert local_fs.exists("copy_funny.gif")


def test__given_files_to_collect_with_non_existing_file__when_collecting__should_collect_remaining_files() -> None:
    local_fs = new_filesystem()
    remote_fs = new_filesystem(["funny.gif"])

    collect(
        remote_fs,
        local_fs,
        [
            CopyInstruction("file.txt", "copy_file.txt"),
            CopyInstruction("funny.gif", "copy_funny.gif"),
        ],
    )

    assert local_fs.exists("copy_funny.gif")


def test__given_files_to_collect_with_non_existing_file__when_collecting__should_log_error_to_ui() -> None:
    local_fs = new_filesystem()
    remote_fs = new_filesystem(["funny.gif"])

    ui_spy = MagicMock()
    collect(
        remote_fs,
        local_fs,
        [
            CopyInstruction("file.txt", "copy_file.txt", True),
            CopyInstruction("funny.gif", "copy_funny.gif", False),
        ],
        ui_spy,
    )

    ui_spy.error.assert_called_with("FileNotFoundError: Cannot copy file 'file.txt'")


def test__given_files_to_collect_with_file_already_existing_on_source_fs__when_collecting__should_still_collect_remaining_files() -> None:
    local_fs = new_filesystem(["copy_file.txt"])
    remote_fs = new_filesystem(["file.txt", "funny.gif"])

    collect(
        remote_fs,
        local_fs,
        [
            CopyInstruction("file.txt", "copy_file.txt", False),
            CopyInstruction("funny.gif", "copy_funny.gif", False),
        ],
    )

    assert local_fs.exists("copy_funny.gif")


def test__given_files_to_collect_with_file_already_existing_on_source_fs__when_collecting__should_log_error_to_ui() -> None:
    local_fs = new_filesystem(["copy_file.txt"])
    remote_fs = new_filesystem(["file.txt", "funny.gif"])

    ui_spy = MagicMock()
    collect(
        remote_fs,
        local_fs,
        [
            CopyInstruction("file.txt", "copy_file.txt", False),
            CopyInstruction("funny.gif", "copy_funny.gif", False),
        ],
        ui_spy,
    )

    ui_spy.error.assert_called_with("FileExistsError: Cannot copy file 'file.txt'")


def test__given_files_to_collect_with_glob_and_existing_file_on_source_fs__when_collecting__collects_remaining_files() -> None:
    local_fs = new_filesystem(["existing.txt"])
    remote_fs = new_filesystem(["existing.txt", "funny.txt"])

    collect(remote_fs, local_fs, [CopyInstruction("*.txt", "", False)])

    assert local_fs.exists("funny.txt")
