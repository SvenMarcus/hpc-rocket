import os
from io import TextIOWrapper
from pathlib import PurePath
from typing import Generator, List, Optional, cast

import fs.base
import fs.copy as fscp
import fs.errors
import fs.glob

from hpcrocket.core.filesystem import Filesystem
from hpcrocket.core.filesystem.glob import (
    is_glob,
    path_after_wildcard,
    split_at_first_wildcard,
)


class PyFilesystemBased(Filesystem):
    """
    A Filesystem based on PyFilesystem2
    """

    def __init__(
        self, internal_fs: fs.base.FS, dir: str = "/", home: str = "/"
    ) -> None:
        self._internal_fs = internal_fs
        self._curdir = PurePath(dir)
        self._homedir = PurePath(home)

    @property
    def current_dir(self) -> PurePath:
        return self._curdir

    @property
    def home(self) -> PurePath:
        return self._homedir

    @property
    def internal_fs(self) -> fs.base.FS:
        """Returns the internally used PyFilesystem

        Returns:
            fs.base.FS: The internal PyFilesystem
        """
        return self._internal_fs

    def glob(self, pattern: str) -> List[str]:
        pattern = self._expandhome(pattern, self)
        sub_fs = self._open_fs(self, pattern)
        return list(self._glob_with_pyfs(sub_fs, pattern))

    def _expandhome(self, path: str, filesystem: "PyFilesystemBased") -> str:
        return path.replace("~", str(filesystem.home))

    def openread(self, path: str) -> TextIOWrapper:
        path = self._expandhome(path, self)
        path = str(self._curdir.joinpath(path))
        try:
            return cast(TextIOWrapper, self.internal_fs.open(path, mode="r"))
        except fs.errors.ResourceNotFound:
            raise FileNotFoundError(path)
        except fs.errors.FileExpected:
            raise FileNotFoundError(path)

    def copy(
        self,
        source: str,
        target: str,
        overwrite: bool = False,
        filesystem: Optional["Filesystem"] = None,
    ) -> None:
        self._raise_if_no_pyfilesystem(filesystem)
        other_pyfs_based = cast(PyFilesystemBased, filesystem) or self
        source = self._expandhome(source, self)
        target = self._expandhome(target, other_pyfs_based)
        source_fs = self._open_fs(self, source)
        target_fs = self._open_fs(other_pyfs_based, target)

        if is_glob(source):
            self._copy_glob(source_fs, source, target_fs, target, overwrite)
            return

        self._copy_single_file(source_fs, source, target_fs, target, overwrite)

    def _open_fs(self, fs: "PyFilesystemBased", path: str) -> fs.base.FS:
        if os.path.isabs(path):
            return fs.internal_fs

        return fs.internal_fs.opendir(str(fs.current_dir))

    def _glob_with_pyfs(
        self, fs: fs.base.FS, pattern: str
    ) -> Generator[str, None, None]:
        dir, pattern = split_at_first_wildcard(pattern)

        if pattern.endswith("*"):
            pattern += "*"

        self._raise_if_does_not_exist(dir, fs)

        fs = fs.opendir(dir)
        for match in fs.glob(pattern):
            joined_path = os.path.join(dir, match.path.lstrip(os.path.sep))
            yield joined_path

    def _copy_glob(
        self,
        source_fs: fs.base.FS,
        source: str,
        target_fs: fs.base.FS,
        target: str,
        overwrite: bool,
    ) -> None:
        glob = self._glob_with_pyfs(source_fs, source)
        for match in glob:
            if source_fs.isdir(match):
                continue

            filename = path_after_wildcard(source, match)
            target_path = os.path.join(target, filename)
            self._copy_single_file(source_fs, match, target_fs, target_path, overwrite)

    def _copy_single_file(
        self,
        source_fs: fs.base.FS,
        source: str,
        target_fs: fs.base.FS,
        target: str,
        overwrite: bool = False,
    ) -> None:
        self._raise_if_does_not_exist(source, source_fs)
        self._raise_if_target_exists(target, overwrite, target_fs)
        self._create_missing_target_dirs(target, target_fs)
        self._try_copy_to_filesystem(source_fs, source, target_fs, target)

    def _create_missing_target_dirs(self, target: str, target_fs: fs.base.FS) -> None:
        target_parent_dir = os.path.dirname(target)
        if not target_fs.exists(target_parent_dir):
            target_fs.makedirs(target_parent_dir, recreate=True)

    def delete(self, path: str) -> None:
        fs = self.internal_fs.opendir(str(self.current_dir))
        if is_glob(path):
            self._delete_glob(path, fs)
            return

        self._delete_path(path, fs)

    def _delete_glob(self, path: str, fs: fs.base.FS) -> None:
        glob = self._glob_with_pyfs(fs, path)
        for match in glob:
            self._delete_path(match, fs)

    def _delete_path(self, path: str, _fs: fs.base.FS) -> None:
        if not _fs.exists(path):
            raise FileNotFoundError(path)

        if _fs.isdir(path):
            self._delete_dir(path, _fs)
            return

        _fs.remove(path)

    def _delete_dir(self, path: str, _fs: fs.base.FS) -> None:
        # NOTE:
        # _fs.removetree raises a ResourceNotFoundError for nested directories
        # Therefore, we open the parent directory first and then delete the lower directory
        # from there
        _fs = _fs.opendir(os.path.dirname(path))
        path = os.path.basename(path)
        _fs.removetree(path)

    def exists(self, path: str) -> bool:
        path = self._expandhome(path, self)
        path = str(self.current_dir.joinpath(path))
        return self.internal_fs.exists(path)

    def _try_copy_to_filesystem(
        self, source_fs: fs.base.FS, source: str, target_fs: fs.base.FS, target: str
    ) -> None:
        if source_fs.isdir(source):
            fscp.copy_dir(source_fs, source, target_fs, target)
            return

        target = self._append_filename_if_target_is_dir(target_fs, source, target)
        fscp.copy_file(source_fs, source, target_fs, target)

    def _append_filename_if_target_is_dir(
        self, fs: fs.base.FS, source: str, target: str
    ) -> str:
        if fs.isdir(target):
            target = os.path.join(target, os.path.basename(source))

        return target

    def _raise_if_does_not_exist(self, source: str, source_fs: fs.base.FS) -> None:
        if not source_fs.exists(source):
            raise FileNotFoundError(source)

    def _raise_if_target_exists(
        self, target: str, overwrite: bool, target_fs: fs.base.FS
    ) -> None:
        if overwrite:
            return

        if target_fs.exists(target) and not target_fs.isdir(target):
            raise FileExistsError(target)

    def _raise_if_no_pyfilesystem(self, filesystem: Optional[Filesystem]) -> None:
        if filesystem and not isinstance(filesystem, PyFilesystemBased):
            raise RuntimeError(
                f"{str(type(self))} currently only works with PyFilesystem2 based Filesystems"
            )
