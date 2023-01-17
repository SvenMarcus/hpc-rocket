from test.testdoubles.filesystem import MemoryFilesystemFactoryStub


def assert_exists_locally(fs_factory: MemoryFilesystemFactoryStub, file: str) -> None:
    assert fs_factory.local_filesystem.exists(file)


def assert_does_not_exist_locally(
    fs_factory: MemoryFilesystemFactoryStub, file: str
) -> None:
    assert not fs_factory.local_filesystem.exists(file)


def assert_exists_on_remote(fs_factory: MemoryFilesystemFactoryStub, file: str) -> None:
    assert fs_factory.ssh_filesystem.exists(file)


def assert_does_not_exist_on_remote(
    fs_factory: MemoryFilesystemFactoryStub, file: str
) -> None:
    assert not fs_factory.ssh_filesystem.exists(file)
