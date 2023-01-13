from test.testdoubles.filesystem import MemoryFilesystemFake
from typing import List, Optional, Generator, Tuple, Type

from hpcrocket.core.filesystem.progressive import (
    CopyInstruction,
    CopyResult,
    progressive_clean,
    progressive_copy,
)
from hpcrocket.core.filesystem import Filesystem


def new_filesystem(files: Optional[List[str]] = None) -> Filesystem:
    return MemoryFilesystemFake(files or [])


def copied_files(results: Generator[CopyResult, None, None]) -> List[str]:
    files = []
    for cr in results:
        files.extend(cr.copied_files)

    return files


def copied_files_and_errors(
    results: Generator[CopyResult, None, None]
) -> Tuple[List[str], List[Exception]]:
    files = []
    errors = []
    for cr in results:
        files.extend(cr.copied_files)
        errors.extend(cr.errors)

    return files, errors


def assert_error_types_equal(
    errors: List[Exception], expected: List[Type[Exception]]
) -> None:
    for i, ex in enumerate(expected):
        error = errors[i]
        assert isinstance(error, ex)


def test__given_files_to_copy__when_preparing__should_copy_files() -> None:
    source_fs = new_filesystem(["file.txt", "funny.gif"])
    target_fs = new_filesystem(["evenfunnier.gif"])

    copy_instructions = [
        CopyInstruction("file.txt", "filecopy.txt"),
        CopyInstruction("funny.gif", "evenfunnier.gif", overwrite=True),
    ]

    result = copied_files(progressive_copy(source_fs, target_fs, copy_instructions))

    assert target_fs.exists("filecopy.txt")
    assert target_fs.exists("evenfunnier.gif")
    assert result == ["filecopy.txt", "evenfunnier.gif"]


def test__given_files_to_copy_with_non_existing_file__when_preparing__yields_copied_files_and_error() -> None:
    source_fs_spy = new_filesystem(["file.txt"])
    target_fs = new_filesystem()

    copy_instructions = [
        CopyInstruction("file.txt", "filecopy.txt"),
        CopyInstruction("funny.gif", "evenfunnier.gif"),
    ]

    files, errors = copied_files_and_errors(
        progressive_copy(source_fs_spy, target_fs, copy_instructions)
    )

    assert files == ["filecopy.txt"]
    assert_error_types_equal(errors, [FileNotFoundError])


def test__given_files_to_copy_with_existing_file_on_target_fs__when_preparing__returns_copied_files_and_error() -> None:
    source_fs_spy = new_filesystem(["file.txt", "funny.gif"])
    target_fs = new_filesystem(["evenfunnier.gif"])

    copy_instructions = [
        CopyInstruction("file.txt", "filecopy.txt"),
        CopyInstruction("funny.gif", "evenfunnier.gif"),
    ]

    files, errors = copied_files_and_errors(
        progressive_copy(source_fs_spy, target_fs, copy_instructions)
    )

    assert files == ["filecopy.txt"]
    assert_error_types_equal(errors, [FileExistsError])


def test__given_copy_instructions_with_glob_and_file_already_on_remote__when_copying__aborts_on_error() -> None:
    copy_instructions = [CopyInstruction("*.txt", "")]

    source_fs = new_filesystem(["myfile.txt", "other.txt"])
    target_fs = new_filesystem(["myfile.txt"])

    files, errors = copied_files_and_errors(
        progressive_copy(source_fs, target_fs, copy_instructions)
    )

    assert target_fs.exists("other.txt") is False
    assert files == []
    assert_error_types_equal(errors, [FileExistsError])


def test__given_copy_instructions_with_glob_for_non_existing_dir__when_copying__returns_error() -> None:
    copy_instructions = [CopyInstruction("dir/*.txt", "")]

    source_fs = new_filesystem()
    target_fs = new_filesystem()

    files, errors = copied_files_and_errors(
        progressive_copy(source_fs, target_fs, copy_instructions)
    )

    assert files == []
    assert_error_types_equal(errors, [FileNotFoundError])


def test__given_files_to_copy__when_copying_without_abort_on_error__copies_remaining_files_after_error() -> None:
    source_fs_spy = new_filesystem(["file.txt", "funny.gif", "other.gif"])
    target_fs = new_filesystem(["funny.gif"])

    copy_instructions = [
        CopyInstruction("file.txt", "filecopy.txt"),
        CopyInstruction("*.gif", ""),
    ]

    files, errors = copied_files_and_errors(
        progressive_copy(
            source_fs_spy, target_fs, copy_instructions, abort_on_error=False
        )
    )

    assert files == ["filecopy.txt", "other.gif"]
    assert_error_types_equal(errors, [FileExistsError])


def test__given_files_to_clean__when_cleaning__should_delete_files() -> None:
    target_fs_spy = new_filesystem(["file.txt", "funny.gif"])

    _ = list(progressive_clean(target_fs_spy, ["file.txt", "funny.gif"]))

    assert target_fs_spy.exists("file.txt") is False
    assert target_fs_spy.exists("funny.gif") is False


def test__given_files_to_clean_with_non_existing_files__when_cleaning__should_still_clean_remaining_files() -> None:
    target_fs = new_filesystem(["funny.gif"])

    errors = list(progressive_clean(target_fs, ["file.txt", "funny.gif"]))

    assert target_fs.exists("funny.gif") is False
    assert_error_types_equal(errors, [FileNotFoundError])
