from unittest.mock import MagicMock, call

import pytest
from hpcrocket.core.environmentpreparation import (CopyInstruction,
                                                   EnvironmentPreparation)
from hpcrocket.core.filesystem import Filesystem


def new_mock_filesystem() -> Filesystem:
    return MagicMock(
        spec="hpcrocket.filesystem.Filesystem").return_value


def test__given_files_to_copy__but_not_preparing__should_not_do_anything():
    source_fs_spy = new_mock_filesystem()
    target_fs = new_mock_filesystem()

    sut = EnvironmentPreparation(source_fs_spy, target_fs)
    sut.files_to_copy([
        CopyInstruction("file1.txt", "file2.txt")
    ])

    source_fs_spy.copy.assert_not_called()


def test__given_files_to_copy__when_preparing__should_copy_files():
    source_fs_spy = new_mock_filesystem()
    target_fs = new_mock_filesystem()

    sut = EnvironmentPreparation(source_fs_spy, target_fs)

    sut.files_to_copy([
        CopyInstruction("file.txt", "filecopy.txt"),
        CopyInstruction("funny.gif", "evenfunnier.gif", overwrite=True)
    ])

    sut.prepare()

    source_fs_spy.copy.assert_has_calls([
        call("file.txt", "filecopy.txt", False, filesystem=target_fs),
        call("funny.gif", "evenfunnier.gif", True, filesystem=target_fs)
    ])


def test__given_files_to_copy_with_non_existing_file__when_preparing_then_rollback__should_remove_copied_files_from_target_fs():
    source_fs_spy = new_mock_filesystem()
    target_fs = new_mock_filesystem()

    source_fs_spy.copy.side_effect = raise_file_not_found_on_given_call(2)
    sut = EnvironmentPreparation(source_fs_spy, target_fs)

    sut.files_to_copy([
        CopyInstruction("file.txt", "filecopy.txt"),
        CopyInstruction("funny.gif", "evenfunnier.gif")
    ])

    with pytest.raises(FileNotFoundError):
        sut.prepare()

    sut.rollback()

    target_fs.delete.assert_called_with("filecopy.txt")


def test__given_copied_file_not_on_target_fs__when_rolling_back__should_remove_remaining_copied_files_from_target_fs():
    source_fs_spy = new_mock_filesystem()
    target_fs = new_mock_filesystem()
    target_fs.delete.side_effect = raise_file_not_found_on_given_call(1)

    sut = EnvironmentPreparation(source_fs_spy, target_fs)
    sut.files_to_copy([
        CopyInstruction("file.txt", "filecopy.txt"),
        CopyInstruction("funny.gif", "evenfunnier.gif")
    ])

    sut.prepare()
    sut.rollback()

    target_fs.delete.assert_has_calls([
        call("evenfunnier.gif")
    ])


def test__given_rollback_done__when_rolling_back_again__should_not_do_anything():
    source_fs_spy = new_mock_filesystem()
    target_fs = new_mock_filesystem()

    sut = EnvironmentPreparation(source_fs_spy, target_fs)
    sut.files_to_copy([
        CopyInstruction("file.txt", "filecopy.txt"),
        CopyInstruction("funny.gif", "evenfunnier.gif")
    ])
    sut.prepare()
    sut.rollback()
    target_fs.reset_mock()

    sut.rollback()

    target_fs.delete.assert_not_called()


def test__given_rollback_done_with_file_not_found__when_rolling_back_again__should_try_to_delete_remaining_files():
    source_fs_spy = new_mock_filesystem()
    target_fs_spy = new_mock_filesystem()
    target_fs_spy.delete.side_effect = raise_file_not_found_on_given_call(1)

    sut = EnvironmentPreparation(source_fs_spy, target_fs_spy)
    sut.files_to_copy([
        CopyInstruction("file.txt", "filecopy.txt"),
        CopyInstruction("funny.gif", "evenfunnier.gif")
    ])

    sut.prepare()
    sut.rollback()
    target_fs_spy.reset_mock()

    sut.rollback()

    target_fs_spy.delete.assert_has_calls([
        call("filecopy.txt")
    ])


def test__given_files_to_clean__but_not_cleaning__should_not_do_anything():
    source_fs = new_mock_filesystem()
    target_fs_spy = new_mock_filesystem()

    sut = EnvironmentPreparation(source_fs, target_fs_spy)
    sut.files_to_clean(["file1.txt"])

    target_fs_spy.delete.assert_not_called()


def test__given_files_to_clean__when_cleaning__should_delete_files():
    source_fs = new_mock_filesystem()
    target_fs_spy = new_mock_filesystem()

    sut = EnvironmentPreparation(source_fs, target_fs_spy)

    sut.files_to_clean([
        "file.txt",
        "funny.gif",
    ])

    sut.clean()

    target_fs_spy.delete.assert_has_calls([
        call("file.txt"),
        call("funny.gif")
    ])


def test__given_files_to_clean_with_non_existing_files__when_cleaning__should_still_clean_remaining_files():
    source_fs_spy = new_mock_filesystem()
    target_fs = new_mock_filesystem()
    target_fs.delete.side_effect = raise_file_not_found_on_given_call(1)

    sut = EnvironmentPreparation(source_fs_spy, target_fs)
    sut.files_to_clean([
        "file.txt",
        "funny.gif",
    ])

    sut.clean()

    target_fs.delete.assert_called_with("funny.gif")


def test__given_files_to_clean_with_non_existing_files__when_cleaning__should_log_error_to_ui():
    source_fs_spy = new_mock_filesystem()
    target_fs = new_mock_filesystem()
    target_fs.delete.side_effect = raise_file_not_found_on_given_call(1)

    ui_spy = MagicMock()
    sut = EnvironmentPreparation(source_fs_spy, target_fs, ui_spy)
    sut.files_to_clean([
        "file.txt",
        "funny.gif",
    ])

    sut.clean()

    ui_spy.error.assert_called_with(
        "FileNotFoundError: Cannot delete file 'file.txt'")


def test__given_files_to_collect__when_collect__should_copy_to_source_fs():
    source_fs_spy = new_mock_filesystem()
    target_fs = new_mock_filesystem()

    sut = EnvironmentPreparation(source_fs_spy, target_fs)

    sut.files_to_collect([
        CopyInstruction("file.txt", "copy_file.txt", True),
        CopyInstruction("funny.gif", "copy_funny.gif", False),
    ])

    sut.collect()

    target_fs.copy.assert_has_calls([
        call("file.txt", "copy_file.txt", True, filesystem=source_fs_spy),
        call("funny.gif", "copy_funny.gif", False, filesystem=source_fs_spy)
    ])


def test__given_files_to_collect_with_non_existing_file__when_collecting__should_collect_remaining_files():
    source_fs_spy = new_mock_filesystem()
    target_fs = new_mock_filesystem()
    target_fs.copy.side_effect = raise_file_not_found_on_given_call(1)

    sut = EnvironmentPreparation(source_fs_spy, target_fs)

    sut.files_to_collect([
        CopyInstruction("file.txt", "copy_file.txt"),
        CopyInstruction("funny.gif", "copy_funny.gif"),
    ])

    sut.collect()

    target_fs.copy.assert_has_calls([
        call("funny.gif", "copy_funny.gif", False, filesystem=source_fs_spy),
    ])


def test__given_files_to_collect_with_non_existing_file__when_collecting__should_log_error_to_ui():
    source_fs_spy = new_mock_filesystem()
    target_fs = new_mock_filesystem()
    target_fs.copy.side_effect = raise_file_not_found_on_given_call(1)

    ui_spy = MagicMock()
    sut = EnvironmentPreparation(source_fs_spy, target_fs, ui_spy)

    sut.files_to_collect([
        CopyInstruction("file.txt", "copy_file.txt", True),
        CopyInstruction("funny.gif", "copy_funny.gif", False),
    ])

    sut.collect()

    ui_spy.error.assert_called_with(
        "FileNotFoundError: Cannot copy file 'file.txt'")


def test__given_files_to_collect_with_file_already_existing_on_source_fs__when_collecting__should_still_collect_remaining_files():
    source_fs_spy = new_mock_filesystem()

    target_fs = new_mock_filesystem()
    target_fs.copy.side_effect = raise_file_exists_on_given_call(1)

    sut = EnvironmentPreparation(source_fs_spy, target_fs)

    sut.files_to_collect([
        CopyInstruction("file.txt", "copy_file.txt", False),
        CopyInstruction("funny.gif", "copy_funny.gif", False),
    ])

    sut.collect()

    target_fs.copy.assert_has_calls([
        call("funny.gif", "copy_funny.gif", False, filesystem=source_fs_spy),
    ])


def test__given_files_to_collect_with_file_already_existing_on_source_fs__when_collecting__should_log_error_to_ui():
    source_fs_spy = new_mock_filesystem()
    target_fs = new_mock_filesystem()
    target_fs.copy.side_effect = raise_file_exists_on_given_call(1)

    ui_spy = MagicMock()
    sut = EnvironmentPreparation(source_fs_spy, target_fs, ui_spy)

    sut.files_to_collect([
        CopyInstruction("file.txt", "copy_file.txt", False),
        CopyInstruction("funny.gif", "copy_funny.gif", False),
    ])

    sut.collect()

    ui_spy.error.assert_called_with(
        "FileExistsError: Cannot copy file 'file.txt'")


def raise_file_not_found_on_given_call(call: int = 1):
    call_count = 0

    def raise_file_not_found(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == call:
            raise FileNotFoundError(*args)

    return raise_file_not_found


def raise_file_exists_on_given_call(call: int = 1):
    call_count = 0

    def raise_file_exists(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == call:
            raise FileExistsError(*args)

    return raise_file_exists
