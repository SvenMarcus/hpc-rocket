from hpcrocket.core.filesystem import FilesystemFactory, Filesystem


class DummyFilesystemFactory(FilesystemFactory):

    def create_local_filesystem(self) -> 'Filesystem':
        return DummyFilesystem()

    def create_ssh_filesystem(self) -> 'Filesystem':
        return DummyFilesystem()


class DummyFilesystem(Filesystem):

    def exists(self, path: str) -> bool:
        return False

    def copy(self, source: str, target: str, overwrite: bool = False, filesystem: 'Filesystem' = None) -> None:
        pass

    def delete(self, path: str) -> None:
        pass
