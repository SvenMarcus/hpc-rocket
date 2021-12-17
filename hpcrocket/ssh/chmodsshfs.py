import stat
from typing import (TYPE_CHECKING, Any, BinaryIO, Collection, Iterator, List, Mapping,
                    Optional, Text, Tuple, cast)

import fs.sshfs.sshfs as sshfs
from fs.base import FS
from fs.info import Info
from fs.permissions import Permissions
from fs.subfs import SubFS

if TYPE_CHECKING:
    from fs.base import _OpendirFactory


class PermissionChangingSSHFSDecorator(FS):
    """
    A subclass of SSHFS that changes the permissions of the remote file after upload.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self._internal_fs: FS = sshfs.SSHFS(*args, **kwargs)  # type: ignore

    def homedir(self) -> Text:
        internal_sshfs = cast(sshfs.SSHFS, self._internal_fs)
        return internal_sshfs._sftp.normalize(".")

    def upload(self, path: str, file: BinaryIO, chunk_size: Optional[int] = None, **options: Any) -> None:
        self._internal_fs.upload(path, file, **options)
        internal_sshfs = cast(sshfs.SSHFS, self._internal_fs)
        internal_sshfs._sftp.chmod(
            path,
            stat.ST_MODE | stat.S_IRUSR | stat.S_IWUSR |
            stat.S_IXUSR | stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

    def download(self, path: Text, file: BinaryIO, chunk_size: Optional[int] = None, **options: Any) -> None:
        self._internal_fs.download(path, file, chunk_size, **options)

    def listdir(self, path: Text) -> List[Text]:
        return self._internal_fs.listdir(path)

    def openbin(self, path: Text, mode: Text = "r", buffering: int = -1, **options: Any) -> BinaryIO:
        return self._internal_fs.openbin(path, mode, buffering, **options)

    def opendir(self, path: Text, factory: Optional['_OpendirFactory[FS]'] = None) -> SubFS[FS]:
        return self._internal_fs.opendir(path, factory)

    def remove(self, path: Text) -> None:
        self._internal_fs.remove(path)

    def removedir(self, path: Text) -> None:
        self._internal_fs.removedir(path)

    def getinfo(self, path: Text, namespaces: Optional[Collection[Text]] = None) -> Info:
        return self._internal_fs.getinfo(path, namespaces=namespaces)

    def setinfo(self, path: Text, info: Mapping[str, Mapping[str, object]]) -> None:
        self._internal_fs.setinfo(path, info)

    def geturl(self, path: Text, purpose: Text = 'download') -> Text:
        return self._internal_fs.geturl(path, purpose)

    def islink(self, path: Text) -> bool:
        return self._internal_fs.islink(path)

    def scandir(self, path: Text, namespaces: Optional[Collection[Text]] = None,
                page: Optional[Tuple[int, int]] = None) -> Iterator[Info]:
        return self._internal_fs.scandir(path, namespaces, page)

    def makedir(self, path: Text, permissions: Optional[Permissions] = None,
                recreate: bool = False) -> SubFS[FS]:
        return self._internal_fs.makedir(path, permissions, recreate)

    def move(self, src_path: Text, dst_path: Text,
             overwrite: bool = False, preserve_time: bool = False) -> None:
        self._internal_fs.move(src_path, dst_path, overwrite)
