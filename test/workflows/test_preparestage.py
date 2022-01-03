from test.testdoubles.filesystem import (DummyFilesystemFactory,
                                         MemoryFilesystemFactoryStub)
from unittest.mock import DEFAULT, Mock, create_autospec

import pytest
from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.filesystem import Filesystem
from hpcrocket.core.workflows.stages import PrepareStage
from hpcrocket.ui import UI


def run_prepare_stage(filesystem_factory, files_to_copy):
    sut = PrepareStage(filesystem_factory, files_to_copy)
    return sut(Mock(spec=UI))


def test__run_prepare_stage__should_return_true():
    actual = run_prepare_stage(DummyFilesystemFactory(), [])

    assert actual is True


def test__given_copy_instructions__when_running__should_copy_files_to_remote_with_given_overwrite_settings():
    copy_instructions = [CopyInstruction("myfile.txt", "mycopy.txt", True)]

    local_fs_mock = create_autospec(spec=Filesystem)
    factory = MemoryFilesystemFactoryStub(local_fs=local_fs_mock)

    run_prepare_stage(factory, copy_instructions)

    local_fs_mock.copy.assert_called_with(
        source="myfile.txt",
        target="mycopy.txt",
        overwrite=True,
        filesystem=factory.ssh_filesystem)


@pytest.mark.parametrize("error_type", (FileNotFoundError, FileExistsError))
def test__given_copy_instructions__when_error_during_copy__should_rollback_copied_files(error_type):
    copy_instructions = [
        CopyInstruction("myfile.txt", "mycopy.txt"),
        CopyInstruction("myfile.txt", "mycopy.txt"),
    ]

    local_fs = filesystem_raising_on_copy(error_type, required_copy_calls=2)
    ssh_fs = create_autospec(spec=Filesystem)
    factory = MemoryFilesystemFactoryStub(local_fs, ssh_fs)

    run_prepare_stage(factory, copy_instructions)

    ssh_fs.delete.assert_called_with("mycopy.txt")


def test__given_copy_instructions__when_error_during_copy__should_return_false():
    copy_instructions = [CopyInstruction("myfile.txt", "mycopy.txt")]
    local_fs = filesystem_raising_on_copy(FileNotFoundError, required_copy_calls=1)
    factory = MemoryFilesystemFactoryStub(local_fs)

    actual = run_prepare_stage(factory, copy_instructions)

    assert actual == False


def filesystem_raising_on_copy(error_type, required_copy_calls: int = 1):
    local_fs = Mock(spec=Filesystem)
    local_fs.copy.side_effect = raise_on_nth_call(error_type, call=required_copy_calls)
    return local_fs


def raise_on_nth_call(error, call: int = 1):
    calls = 0

    def raise_on_second_call(*args, **kwargs):
        nonlocal calls
        calls += 1

        if calls == call:
            raise error

        return DEFAULT

    return raise_on_second_call
