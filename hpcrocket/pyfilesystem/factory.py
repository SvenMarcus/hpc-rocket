import os
from hpcrocket.core.filesystem import Filesystem, FilesystemFactory
from hpcrocket.core.launchoptions import Options
from hpcrocket.pyfilesystem.localfilesystem import localfilesystem
from hpcrocket.pyfilesystem.sshfilesystem import sshfilesystem


class PyFilesystemFactory(FilesystemFactory):
    def __init__(self, options: Options) -> None:
        self._options = options

    def create_local_filesystem(self) -> Filesystem:
        return localfilesystem(os.getcwd())

    def create_ssh_filesystem(self) -> Filesystem:
        connection = self._options.connection
        proxyjumps = self._options.proxyjumps
        return sshfilesystem(connection, proxyjumps)
