import dataclasses
from hpcrocket.ssh.sshexecutor import ConnectionData
import os

from typing import Optional

from hpcrocket.core.filesystem import Filesystem, FilesystemFactory
from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.local.localfilesystem import LocalFilesystem
from hpcrocket.ssh.sshfilesystem import SSHFilesystem


class PyFilesystemFactory(FilesystemFactory):

    def __init__(self, options: LaunchOptions) -> None:
        self._options = options

    def create_local_filesystem(self) -> Filesystem:
        return LocalFilesystem(".")

    def create_ssh_filesystem(self) -> Filesystem:
        connection = ConnectionData.with_resolved_keyfile(self._options.connection)
        proxyjumps = [ConnectionData.with_resolved_keyfile(proxy) for proxy in self._options.proxyjumps]
        return SSHFilesystem(connection, proxyjumps)
