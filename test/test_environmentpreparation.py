from ssh_slurm_runner.filesystem import Filesystem
from ssh_slurm_runner.environmentpreparation import EnvironmentPreparation
from unittest.mock import MagicMock, call


def new_mock_filesystem() -> Filesystem:
    return MagicMock(
        spec="ssh_slurm_runner.filesyste.Filesystem").return_value


def test__given_files_to_copy__but_not_preparing__should_not_do_anything():
    source_fs_spy = new_mock_filesystem()
    target_fs = new_mock_filesystem()

    sut = EnvironmentPreparation(source_fs_spy, target_fs)
    sut.files_to_copy([
        ("file1.txt", "file2.txt")
    ])

    source_fs_spy.copy.assert_not_called()


def test__given_files_to_copy__when_preparing__should_copy_files():
    source_fs_spy: MagicMock = new_mock_filesystem()
    target_fs = new_mock_filesystem()

    sut = EnvironmentPreparation(source_fs_spy, target_fs)

    sut.files_to_copy([
        ("file.txt", "filecopy.txt"),
        ("funny.gif", "evenfunnier.gif")
    ])

    sut.prepare()

    source_fs_spy.copy.assert_has_calls([
        call("file.txt", "filecopy.txt", target_fs),
        call("funny.gif", "evenfunnier.gif", target_fs)
    ])


def test__given_files_to_clean__but_not_cleaning__should_not_do_anything():
    source_fs: MagicMock = new_mock_filesystem()
    target_fs_spy = new_mock_filesystem()

    sut = EnvironmentPreparation(source_fs, target_fs_spy)
    sut.files_to_clean(["file1.txt"])

    target_fs_spy.delete.assert_not_called()


def test__given_files_to_clean__when_cleaning__should_delete_files():
    source_fs: MagicMock = new_mock_filesystem()
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


def test__given_files_to_collect__when_collect__should_copy_to_source_fs():
    source_fs_spy: MagicMock = new_mock_filesystem()
    target_fs = new_mock_filesystem()

    sut = EnvironmentPreparation(source_fs_spy, target_fs)

    sut.files_to_collect([
        "file.txt",
        "funny.gif",
    ])

    sut.collect()

    target_fs.copy.assert_has_calls([
        call("file.txt", "file.txt", source_fs_spy),
        call("funny.gif", "funny.gif", source_fs_spy)
    ])