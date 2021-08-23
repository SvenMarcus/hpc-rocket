from typing import Collection, IO, List, Optional, Text, Tuple, Iterator
import fs.base
from fs.info import Info
from fs.permissions import Permissions
import fs.sshfs as sshfs
import stat

import fs.subfs

class PermissionChangingSSHFSDecorator(fs.base.FS):
    """
    A subclass of SSHFS that changes the permissions of the remote file after upload.
    """

    def __init__(self, *args, **kwargs):
        self._internal_fs = sshfs.SSHFS(*args, **kwargs)

    def upload(self, path: str, file: IO, *args, **kwargs):
        self._internal_fs.upload(path, file, *args, **kwargs)
        self._internal_fs._sftp.chmod(
            path, stat.S_IEXEC | stat.S_IREAD | stat.S_IWRITE)
    
    def download(self, path: Text, file: IO) -> None:
        self._internal_fs.download(path, file)

    def listdir(self, path: Text) -> List[Text]:
        return self._internal_fs.listdir(path)

    def makedir(self, path: Text, permissions: int, exist_ok: bool = False) -> None:
        self._internal_fs.makedir(path, permissions, exist_ok=exist_ok)

    def openbin(self, path: Text, mode: Text) -> IO:
        return self._internal_fs.openbin(path, mode)

    def opendir(self, path: Text, factory = None) -> fs.subfs.SubFS[fs.base.FS]:
        return self._internal_fs.opendir(path, factory=factory)

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

    def scandir(self, path: Text, namespaces: Optional[Collection[Text]], page: Optional[Tuple[int, int]] = None) -> Iterator[Info]:
        return self._internal_fs.scandir(path, namespaces, page)

    def makedir(self, path: Text, permissions: Optional[Permissions], recreate: bool) -> fs.subfs.SubFS[fs.base.FS]:
        return self._internal_fs.makedir(path, permissions, recreate)

    def move(self, src_path: Text, dst_path: Text, overwrite: bool = False) -> None:
        self._internal_fs.move(src_path, dst_path, overwrite)

    