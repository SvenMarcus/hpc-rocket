from ssh_slurm_runner.environmentpreparation import EnvironmentPreparation
from unittest.mock import MagicMock, call


def test__given_files_to_copy__but_not_preparing__should_not_do_anything():
    fs_spy = MagicMock(
        spec="ssh_slurm_runner.filesyste.Filesystem").return_value

    sut = EnvironmentPreparation(fs_spy)
    sut.files_to_copy([
        ("file1.txt", "file2.txt")
    ])

    fs_spy.copy.assert_not_called()


def test__given_files_to_copy__when_preparing__should_copy_files():
    fs_spy: MagicMock = MagicMock(
        spec="ssh_slurm_runner.filesyste.Filesystem").return_value

    sut = EnvironmentPreparation(fs_spy)

    sut.files_to_copy([
        ("file.txt", "filecopy.txt"),
        ("funny.gif", "evenfunnier.gif")
    ])

    sut.prepare()

    fs_spy.copy.assert_has_calls([
        call("file.txt", "filecopy.txt"),
        call("funny.gif", "evenfunnier.gif")
    ])


def test__given_files_to_clean__but_not_cleaning__should_not_do_anything():
    fs_spy = MagicMock(
        spec="ssh_slurm_runner.filesyste.Filesystem").return_value

    sut = EnvironmentPreparation(fs_spy)
    sut.files_to_clean(["file1.txt"])

    fs_spy.delete.assert_not_called()


def test__given_files_to_clean__when_cleaning__should_delete_files():
    fs_spy: MagicMock = MagicMock(
        spec="ssh_slurm_runner.filesyste.Filesystem").return_value

    sut = EnvironmentPreparation(fs_spy)

    sut.files_to_clean([
        "file.txt",
        "funny.gif",
    ])

    sut.clean()

    fs_spy.delete.assert_has_calls([
        call("file.txt"),
        call("funny.gif")
    ])
