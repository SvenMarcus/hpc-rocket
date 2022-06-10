import fs.base
import fs.osfs

from hpcrocket.pyfilesystem.pyfilesystembased import PyFilesystemBased


def localfilesystem(workdir: str) -> PyFilesystemBased:
    """
    A PyFilesystem2 based filesystem that uses the computer's local filesystem

    Args:
        workdir (str): The path the filesystem should be opened in
    """

    return PyFilesystemBased(fs.osfs.OSFS("/"), workdir)
