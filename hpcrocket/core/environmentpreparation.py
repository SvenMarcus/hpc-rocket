import functools
import os
from dataclasses import dataclass
from typing import Callable, List, NamedTuple, Optional

from hpcrocket.core.errors import error_type
from hpcrocket.core.filesystem import Filesystem
from hpcrocket.ui import UI, NullUI


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
            return [self._unglobbed_sub_instruction(file) for file in files]

        return [self]

    def _unglobbed_sub_instruction(self, file: str) -> "CopyInstruction":
        return CopyInstruction(
            file, _join_dest_and_src(file, self.destination), self.overwrite
        )


@dataclass(frozen=True)
class _CopyResult:
    copied_files: List[str]
    error: Optional[Exception] = None

    @classmethod
    def empty(cls) -> "_CopyResult":
        return cls([])


_CopyErrorCallback = Callable[[Exception, CopyInstruction], None]


class _Copier:
    def __init__(
        self,
        src_fs: Filesystem,
        target_fs: Filesystem,
        *,
        abort_on_error: bool = True,
        error_callback: Optional[_CopyErrorCallback] = None,
    ) -> None:
        self._src_fs = src_fs
        self._target_fs = target_fs
        self._abort_on_error = abort_on_error
        self._error_callback = error_callback or (lambda _, __: None)

    def __call__(self, copy_instruction: CopyInstruction) -> _CopyResult:
        unpacked_instructions = copy_instruction.unglob(self._src_fs)
        return functools.reduce(
            self._reduce_to_copy_result, unpacked_instructions, _CopyResult([])
        )

    def _reduce_to_copy_result(
        self, current_result: _CopyResult, instruction: CopyInstruction
    ) -> _CopyResult:
        if current_result.error and self._abort_on_error:
            return current_result

        error = self._try_copy(instruction)

        if error is None:
            current_result.copied_files.append(instruction.destination)

        return _CopyResult(current_result.copied_files, error)

    def _try_copy(self, instruction: CopyInstruction) -> Optional[Exception]:
        try:
            self._src_fs.copy(*instruction, filesystem=self._target_fs)
        except (FileNotFoundError, FileExistsError) as err:
            self._error_callback(err, instruction)
            return err

        return None


class Deleter:
    def __init__(self, filesystem: Filesystem, ui: UI) -> None:
        self._filesystem = filesystem
        self._ui = ui or NullUI()

    def __call__(self, files: List[str]) -> List[str]:
        deleted_files = []
        for file in files:
            try:
                self._filesystem.delete(file)
                deleted_files.append(file)
            except FileNotFoundError as err:
                self._ui.error(f"{error_type(err)}: Cannot delete file '{file}'")

        return deleted_files


class EnvironmentPreparation:
    """
    This class is responsible for copying files to the remote filesystem.
    Can perform a rollback of copied files in case copying fails.
    """

    def __init__(
        self,
        source_filesystem: Filesystem,
        target_filesystem: Filesystem,
        files_to_copy: List[CopyInstruction],
        ui: Optional[UI] = None,
    ) -> None:
        self._copier = _Copier(source_filesystem, target_filesystem)
        self._delete = Deleter(target_filesystem, ui or NullUI())
        self._copy: List[CopyInstruction] = files_to_copy
        self._copied_files: List[str] = list()

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
            result = self._copier(copy_instruction)
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
        deleted_files = self._delete(self._copied_files)
        for file in deleted_files:
            self._copied_files.remove(file)


class EnvironmentCleaner:
    def __init__(
        self, filesystem: Filesystem, files_to_clean: List[str], ui: Optional[UI] = None
    ) -> None:
        self._delete = Deleter(filesystem, ui or NullUI())
        self._clean_files: List[str] = files_to_clean

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
        self._delete(self._clean_files)


class EnvironmentCollector:
    def __init__(
        self,
        remote_filesystem: Filesystem,
        local_filesystem: Filesystem,
        files_to_collect: List[CopyInstruction],
        ui: Optional[UI] = None,
    ) -> None:
        _ui = ui or NullUI()

        def log_error(error: Exception, instruction: CopyInstruction) -> None:
            _ui.error(f"{error_type(error)}: Cannot copy file '{instruction.source}'")

        self._copier = _Copier(
            remote_filesystem,
            local_filesystem,
            abort_on_error=False,
            error_callback=log_error,
        )

        self._collect: List[CopyInstruction] = files_to_collect

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
            self._copier(copy_instruction)
