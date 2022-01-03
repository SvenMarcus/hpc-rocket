from test.testdoubles.filesystem import (DummyFilesystemFactory,
                                         MemoryFilesystemFactoryStub,
                                         MemoryFilesystemFake)
from unittest.mock import Mock

from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.core.workflows.stages import FinalizeStage


def run_finalize_stage(factory, collect, clean):
    sut = FinalizeStage(factory, collect, clean)

    return sut(Mock())


def test__when_running_successfully__should_return_true():
    actual = run_finalize_stage(DummyFilesystemFactory(), [], [])

    assert actual is True


def test__given_collect_instruction__when_running__should_copy_collect_item_to_local_filesystem():
    ssh_fs = MemoryFilesystemFake(files=["myfile.txt"])
    factory = MemoryFilesystemFactoryStub(ssh_fs=ssh_fs)

    run_finalize_stage(factory, [CopyInstruction("myfile.txt", "collected.txt")], [])

    local_fs = factory.local_filesystem
    assert local_fs.exists("collected.txt") is True


def test__given_clean_instruction__when_running__should_clean_file_from_remote_filesystem():
    ssh_fs = MemoryFilesystemFake(files=["myfile.txt"])
    factory = MemoryFilesystemFactoryStub(ssh_fs=ssh_fs)

    run_finalize_stage(factory, [], ["myfile.txt"])

    assert ssh_fs.exists("myfile.txt") is False


def test__given_collect_instructions__when_file_not_found__should_still_collect_remaining_files():
    ssh_fs = MemoryFilesystemFake(files=["myfile.txt"])
    factory = MemoryFilesystemFactoryStub(ssh_fs=ssh_fs)

    files_to_collect = [CopyInstruction("invalid", "_"),
                        CopyInstruction("myfile.txt", "collected.txt")]

    run_finalize_stage(factory, files_to_collect, [])

    local_fs = factory.local_filesystem
    assert local_fs.exists("collected.txt") is True


def test__given_collect_instructions__when_file_exists__should_still_collect_remaining_files():
    local_fs = MemoryFilesystemFake(files=["existing.txt"])
    ssh_fs = MemoryFilesystemFake(files=["myfile.txt"])
    factory = MemoryFilesystemFactoryStub(local_fs, ssh_fs)

    files_to_collect = [CopyInstruction("myfile.txt", "existing.txt"),
                        CopyInstruction("myfile.txt", "collected.txt")]

    run_finalize_stage(factory, files_to_collect, [])

    assert local_fs.exists("collected.txt") is True
