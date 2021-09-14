import stat
from typing import IO, BinaryIO, Collection, Iterator, List, Optional, Text, Tuple

import fs.base
import fs.sshfs as sshfs
import fs.subfs
import fs.wrapfs
from fs.info import Info
from fs.permissions import Permissions


class PermissionChangingSSHFSDecorator(fs.base.FS):
    """
    A subclass of SSHFS that changes the permissions of the remote file after upload.
    """

    def __init__(self, *args, **kwargs):
        super().__init__()
        self._internal_fs = sshfs.SSHFS(*args, **kwargs)

    def upload(self, path: str, file: IO, *args, **kwargs):
        self._internal_fs.upload(path, file, *args, **kwargs)
        self._internal_fs._sftp.chmod(
            path, stat.ST_MODE | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)
    
    def download(self, path: Text, file: IO, chunk_size = None, **options) -> None:
        self._internal_fs.download(path, file, chunk_size, **options)

    def listdir(self, path: Text) -> List[Text]:
        return self._internal_fs.listdir(path)

    def openbin(self, path: Text, mode: Text = "r", buffering=-1, **options) -> BinaryIO:
        return self._internal_fs.openbin(path, mode, buffering, **options)

    def opendir(self, path: Text, factory = None) -> fs.subfs.SubFS[fs.base.FS]:
        return super().opendir(path, factory)

    def remove(self, path: Text) -> None:
        self._internal_fs.remove(path)

    def removedir(self, path: Text) -> None:
        self._internal_fs.removedir(path)

    def getinfo(self, path: Text, namespaces: Optional[Collection[Text]] = None) -> Info:
        return self._internal_fs.getinfo(path, namespaces=namespaces)

    def setinfo(self, path: Text, info) -> None:
        self._internal_fs.setinfo(path, info)

    def geturl(self, path: Text, purpose: Text = 'download') -> Text:
        return self._internal_fs.geturl(path, purpose)

    def islink(self, path: Text) -> bool:
        return self._internal_fs.islink(path)

    def scandir(self, path: Text, namespaces: Optional[Collection[Text]] = None, page: Optional[Tuple[int, int]] = None) -> Iterator[Info]:
        return self._internal_fs.scandir(path, namespaces, page)

    def makedir(self, path: Text, permissions: Optional[Permissions] = None, recreate: bool = False) -> fs.subfs.SubFS[fs.base.FS]:
        return self._internal_fs.makedir(path, permissions, recreate)

    def move(self, src_path: Text, dst_path: Text, overwrite: bool = False) -> None:
        self._internal_fs.move(src_path, dst_path, overwrite)

    