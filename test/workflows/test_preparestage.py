from typing import List
from hpcrocket.core.filesystem import FilesystemFactory
from test.testdoubles.filesystem import (
    DummyFilesystemFactory,
    MemoryFilesystemFactoryStub,
)
from unittest.mock import Mock

from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.workflows.stages import PrepareStage
from hpcrocket.ui import UI


def run_prepare_stage(
    filesystem_factory: FilesystemFactory, files_to_copy: List[CopyInstruction]
) -> bool:
    sut = PrepareStage(filesystem_factory, files_to_copy)
    return sut(Mock(spec=UI))


def test__run_prepare_stage__should_return_true() -> None:
    actual = run_prepare_stage(DummyFilesystemFactory(), [])

    assert actual is True


def test__given_copy_instructions__when_running__should_copy_files_to_remote_with_given_overwrite_settings() -> None:
    copy_instructions = [CopyInstruction("myfile.txt", "mycopy.txt", True)]

    factory = MemoryFilesystemFactoryStub()
    factory.create_local_files("myfile.txt")

    run_prepare_stage(factory, copy_instructions)

    assert factory.ssh_filesystem.exists("mycopy.txt")


def test__given_copy_instructions__when_file_exists_error_during_copy__should_rollback_copied_files() -> None:
    copy_instructions = [
        CopyInstruction("myfile.txt", "mycopy.txt"),
        CopyInstruction("myfile.txt", "mycopy.txt"),
    ]

    factory = MemoryFilesystemFactoryStub()
    factory.create_local_files("myfile.txt")

    run_prepare_stage(factory, copy_instructions)

    assert factory.ssh_filesystem.exists("mycopy.txt") is False


def test__given_copy_instructions__when_file_not_found_during_copy__should_rollback_copied_files() -> None:
    copy_instructions = [
        CopyInstruction("myfile.txt", "mycopy.txt"),
        CopyInstruction("other.txt", "othercopy.txt"),
    ]

    factory = MemoryFilesystemFactoryStub()
    factory.create_local_files("myfile.txt")

    run_prepare_stage(factory, copy_instructions)

    assert factory.ssh_filesystem.exists("mycopy.txt") is False


def test__given_copy_instructions__when_error_during_copy__should_return_false() -> None:
    copy_instructions = [CopyInstruction("myfile.txt", "mycopy.txt")]
    factory = MemoryFilesystemFactoryStub()

    actual = run_prepare_stage(factory, copy_instructions)

    assert actual == False
