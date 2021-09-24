import fs.base
import fs.osfs

from hpcrocket.pyfilesystem.pyfilesystembased import PyFilesystemBased


class LocalFilesystem(PyFilesystemBased):
    """
    A PyFilesystem2 based filesystem that uses the computer's local filesystem
    """

    def __init__(self, rootpath: str) -> None:
        """
        Args:
            rootpath (str): The path the filesystem should be opened in
        """
        self._internal_fs = fs.osfs.OSFS(rootpath)

    @property
    def internal_fs(self) -> fs.base.FS:
        return self._internal_fs
