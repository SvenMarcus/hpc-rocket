from dataclasses import dataclass
import os
from typing import Callable, List, NamedTuple, Optional, Tuple

from hpcrocket.core.errors import error_type
from hpcrocket.core.filesystem import Filesystem
from hpcrocket.ui import UI, NullUI

try:
    from typing import Protocol
except ImportError:  # pragma: no cover
    from typing_extensions import Protocol  # type: ignore


def _join_dest_and_src(src: str, dest: str) -> str:
    return os.path.join(dest, os.path.basename(src))


class CopyInstruction(NamedTuple):
    """
    Copy instruction for a file.
    """

    source: str
    destination: str
    overwrite: bool = False

    def unglob(self, filesystem: Filesystem) -> List["CopyInstruction"]:
        if "*" in self.source:
            files = filesystem.glob(self.source)
            return [
                CopyInstruction(
                    file,
                    _join_dest_and_src(file, self.destination),
                    self.overwrite,
                )
                for file in files
            ]

        return [self]


class _CopyFunction(Protocol):
    def __call__(
        self,
        copy_instruction: CopyInstruction,
    ) -> None:
        ...


@dataclass(frozen=True)
class _CopyResult:
    copied_files: List[str]
    error: Optional[Exception] = None


class _Copier:
    def __init__(self, src_fs: Filesystem, copy_function: _CopyFunction) -> None:
        self._src_fs = src_fs
        self._copy_function = copy_function

    def copy(self, copy_instruction: CopyInstruction) -> _CopyResult:
        unpacked_instructions = copy_instruction.unglob(self._src_fs)
        return self._copy_all(unpacked_instructions)

    def _copy_all(
        self,
        files: List[CopyInstruction],
    ) -> _CopyResult:
        copied_files: List[str] = []
        for instruction in files:
            result = self._copy_file(instruction, copied_files)
            if result.error:
                return result

        return _CopyResult(copied_files)

    def _copy_file(
        self, instruction: CopyInstruction, copied_files: List[str]
    ) -> _CopyResult:
        try:
            self._copy_function(instruction)
            copied_files.append(instruction.destination)
        except (FileNotFoundError, FileExistsError) as err:
            return _CopyResult(copied_files, err)

        return _CopyResult(copied_files)


def _make_failure_logging_copy_function(
    src_fs: Filesystem, target_fs: Filesystem, ui: UI
) -> _CopyFunction:
    copy_function = _make_copy_function(src_fs, target_fs)

    def _copy_file(copy_instruction: CopyInstruction) -> None:
        try:
            copy_function(copy_instruction)
        except (FileNotFoundError, FileExistsError) as err:
            ui.error(f"{error_type(err)}: Cannot copy file '{copy_instruction.source}'")

    return _copy_file


def _make_copy_function(src_fs: Filesystem, target_fs: Filesystem) -> _CopyFunction:
    def _copy_file(copy_instruction: CopyInstruction) -> None:
        src_fs.copy(*copy_instruction, filesystem=target_fs)

    return _copy_file


def _make_delete_function(filesystem: Filesystem, ui: UI) -> Callable[[str], bool]:
    def _try_delete(file: str) -> bool:
        try:
            filesystem.delete(file)
        except FileNotFoundError as err:
            ui.error(f"{error_type(err)}: Cannot delete file '{file}'")
            return False

        return True

    return _try_delete


class EnvironmentPreparation:
    """
    This class is responsible for copying files to the remote filesystem.
    Can perform a rollback of copied files in case copying fails.
    """

    def __init__(
        self,
        source_filesystem: Filesystem,
        target_filesystem: Filesystem,
        ui: Optional[UI] = None,
    ) -> None:
        copy_function = _make_copy_function(source_filesystem, target_filesystem)
        self._src_to_target_copier = _Copier(source_filesystem, copy_function)
        self._delete = _make_delete_function(target_filesystem, ui or NullUI())
        self._copy: List[CopyInstruction] = list()
        self._copied_files: List[str] = list()

    def files_to_copy(self, copy_instructions: List[CopyInstruction]) -> None:
        """
        Sets the files to copy to the target filesystem.

        Args:
            copy_instructions: A list of copy instructions (essentially tuples) of the form (src, dest, overwrite)

        Returns:
            None
        """
        self._copy = list(copy_instructions)

    def prepare(self) -> None:
        """
        Copies the files to the target filesystem.

        Args:
            None

        Returns:
            None

        Raises:
            FileNotFoundError: If a file to copy is not found on the source filesystem
            FileExistsError: If a file to copy already exists on the target filesystem
        """
        for copy_instruction in self._copy:
            result = self._src_to_target_copier.copy(copy_instruction)
            self._copied_files.extend(result.copied_files)
            if result.error:
                raise result.error

    def rollback(self) -> None:
        """
        Rolls back the files that were copied to the target filesystem.

        Args:
            None

        Returns:
            None

        Raises:
            FileNotFoundError: If a file to rollback is not found on the target filesystem
        """
        deleted_files: List[str] = []
        for file in self._copied_files:
            deleted = self._delete(file)
            if deleted:
                deleted_files.append(file)

        for file in deleted_files:
            self._copied_files.remove(file)


class EnvironmentCleaner:
    def __init__(self, filesystem: Filesystem, ui: Optional[UI] = None) -> None:
        self._delete = _make_delete_function(filesystem, ui or NullUI())
        self._clean_files: List[str] = []

    def files_to_clean(self, files: List[str]) -> None:
        """
        Sets the files to delete from the target filesystem.

        Args:
            files: A list of files to delete

        Returns:
            None
        """
        self._clean_files = list(files)

    def clean(self) -> None:
        """
        Deletes the files from the target filesystem. Files that are not found are ignored.

        Args:
            None

        Returns:
            None

        Raises:
            None
        """
        for file in self._clean_files:
            self._delete(file)


class EnvironmentCollector:
    def __init__(
        self,
        remote_filesystem: Filesystem,
        local_filesystem: Filesystem,
        ui: Optional[UI] = None,
    ) -> None:
        copy_function = _make_failure_logging_copy_function(
            remote_filesystem, local_filesystem, ui or NullUI()
        )

        self._target_to_src_copier = _Copier(
            remote_filesystem,
            copy_function,
        )

        self._collect: List[CopyInstruction] = []

    def files_to_collect(self, copy_instructions: List[CopyInstruction]) -> None:
        """
        Sets the files to collect from the target filesystem.

        Args:
            copy_instructions: A list of copy instructions (essentially tuples) of the form (src, dest, overwrite)

        Returns:
            None
        """
        self._collect = list(copy_instructions)

    def collect(self) -> None:
        """
        Collects the files from the target filesystem.
        Files that are not found or already present on the source filesystem are ignored.

        Args:
            None

        Returns:
            None
        """
        for copy_instruction in self._collect:
            self._target_to_src_copier.copy(copy_instruction)
