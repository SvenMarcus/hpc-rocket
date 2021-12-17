from hpcrocket.core.filesystem import Filesystem, FilesystemFactory
from hpcrocket.core.launchoptions import Options
from hpcrocket.pyfilesystem.localfilesystem import LocalFilesystem
from hpcrocket.pyfilesystem.sshfilesystem import SSHFilesystem


class PyFilesystemFactory(FilesystemFactory):

    def __init__(self, options: Options) -> None:
        self._options = options

    def create_local_filesystem(self) -> Filesystem:
        return LocalFilesystem(".")

    def create_ssh_filesystem(self) -> Filesystem:
        connection = self._options.connection
        proxyjumps = self._options.proxyjumps
        return SSHFilesystem(connection, proxyjumps)
