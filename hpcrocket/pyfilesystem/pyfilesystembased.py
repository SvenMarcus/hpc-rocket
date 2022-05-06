import os
from abc import ABC, abstractmethod
from io import TextIOWrapper
from typing import List, Optional, cast

import fs.base
import fs.copy as fscp
import fs.errors
import fs.glob
from hpcrocket.core.filesystem import Filesystem


def _is_glob(path: str) -> bool:
    """
    Checks if a wildcard operator is used in the path

    Args:
        path (str): The filepath

    Returns:
        bool
    """

    return "*" in path


class PyFilesystemBased(Filesystem, ABC):
    """
    Abstract base class for Filesystems based on PyFilesystem2
    """

    @property
    @abstractmethod
    def internal_fs(self) -> fs.base.FS:
        """Returns the internally used PyFilesystem

        Returns:
            fs.base.FS: The internal PyFilesystem
        """

    def glob(self, pattern: str) -> List[str]:
        return [match.path.lstrip("/") for match in self.internal_fs.glob(pattern)]

    def openread(self, path: str) -> TextIOWrapper:
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
        if _is_glob(source):
            self._copy_glob(source, target, overwrite, filesystem)
            return

        self._copy_single_file(source, target, overwrite, filesystem)

    def _copy_glob(
        self,
        source: str,
        target: str,
        overwrite: bool,
        filesystem: Optional["Filesystem"],
    ) -> None:
        glob = self.internal_fs.glob(source)
        for match in glob:
            filename = os.path.basename(match.path)
            target_path = os.path.join(target, filename)
            self._copy_single_file(match.path, target_path, overwrite, filesystem)

    def _copy_single_file(
        self,
        source: str,
        target: str,
        overwrite: bool = False,
        filesystem: Optional["Filesystem"] = None,
    ) -> None:
        self._raise_if_source_does_not_exist(source)
        self._raise_if_target_exists(target, overwrite, filesystem)
        self._raise_if_no_pyfilesystem(filesystem)
        self._create_missing_target_dirs(target, filesystem)

        if filesystem:
            self._try_copy_to_filesystem(source, target, filesystem)
            return

        self._try_copy(source, target, overwrite)

    def _create_missing_target_dirs(
        self, target: str, filesystem: Optional[Filesystem]
    ) -> None:
        target_fs = cast(PyFilesystemBased, filesystem) or self
        target_parent_dir = os.path.dirname(target)
        if not target_fs.exists(target_parent_dir):
            target_fs.internal_fs.makedirs(target_parent_dir)

    def delete(self, path: str) -> None:
        if _is_glob(path):
            self._delete_glob(path)
            return

        self._delete_path(path)

    def _delete_glob(self, path: str) -> None:
        glob = self.internal_fs.glob(path)
        for match in glob:
            self._delete_path(match.path)

    def _delete_path(self, path: str) -> None:
        if not self.exists(path):
            raise FileNotFoundError(path)

        if self.internal_fs.isdir(path):
            self.internal_fs.removetree(path)
            return

        self.internal_fs.remove(path)

    def exists(self, path: str) -> bool:
        return self.internal_fs.exists(path)

    def _try_copy_to_filesystem(
        self, source: str, target: str, filesystem: Optional[Filesystem]
    ) -> None:
        other_filesystem = cast(PyFilesystemBased, filesystem).internal_fs
        if self.internal_fs.isdir(source):
            fscp.copy_dir(self.internal_fs, source, other_filesystem, target)
            return

        target = self._append_filename_if_target_is_dir(
            other_filesystem, source, target
        )
        fscp.copy_file(self.internal_fs, source, other_filesystem, target)

    def _try_copy(self, source: str, target: str, overwrite: bool) -> None:
        if self.internal_fs.isdir(source):
            self.internal_fs.copydir(source, target, create=True)
            return

        target = self._append_filename_if_target_is_dir(
            self.internal_fs, source, target
        )
        self.internal_fs.copy(source, target, overwrite=overwrite)

    def _append_filename_if_target_is_dir(
        self, fs: fs.base.FS, source: str, target: str
    ) -> str:
        if fs.isdir(target):
            target = os.path.join(target, os.path.basename(source))

        return target

    def _raise_if_source_does_not_exist(self, source: str) -> None:
        if not self.exists(source):
            raise FileNotFoundError(source)

    def _raise_if_target_exists(
        self, target: str, overwrite: bool, filesystem: Optional[Filesystem]
    ) -> None:
        if overwrite:
            return

        target_filesystem = cast(PyFilesystemBased, filesystem) or self
        if target_filesystem.exists(target) and not target_filesystem.internal_fs.isdir(
            target
        ):
            raise FileExistsError(target)

    def _raise_if_no_pyfilesystem(self, filesystem: Optional[Filesystem]) -> None:
        if filesystem and not isinstance(filesystem, PyFilesystemBased):
            raise RuntimeError(
                f"{str(type(self))} currently only works with PyFilesystem2 based Filesystems"
            )
