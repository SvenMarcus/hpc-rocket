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


@dataclass
class CopyResult:
    copied_files: List[str]
    error: Optional[Exception] = None

    @classmethod
    def empty(cls) -> "CopyResult":
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

    def __call__(self, copy_instruction: CopyInstruction) -> CopyResult:
        unpacked_instructions = copy_instruction.unglob(self._src_fs)
        return functools.reduce(
            self._reduce_to_copy_result, unpacked_instructions, CopyResult([])
        )

    def _reduce_to_copy_result(
        self, current_result: CopyResult, instruction: CopyInstruction
    ) -> CopyResult:
        if current_result.error and self._abort_on_error:
            return current_result

        error = self._try_copy(instruction)

        if error is None:
            current_result.copied_files.append(instruction.destination)

        return CopyResult(current_result.copied_files, error)

    def _try_copy(self, instruction: CopyInstruction) -> Optional[Exception]:
        try:
            self._src_fs.copy(*instruction, filesystem=self._target_fs)
        except (FileNotFoundError, FileExistsError) as err:
            self._error_callback(err, instruction)
            return err

        return None


def prepare(
    source_filesystem: Filesystem,
    target_filesystem: Filesystem,
    files: List[CopyInstruction],
) -> CopyResult:
    """
    Copies the files to the target filesystem.

    Args:
        source_filesystem (Filesystem): The filesystem to copy FROM
        target_filesystem (Filesystem): The filesystem to copy TO
        files (list[CopyInstruction]): A list of CopyInstructions

    Returns:
        CopyResult
    """
    copier = _Copier(source_filesystem, target_filesystem)
    result = CopyResult.empty()
    for copy_instruction in files:
        tmp_result = copier(copy_instruction)
        result.copied_files.extend(tmp_result.copied_files)
        if tmp_result.error:
            result.error = tmp_result.error
            break

    return result


def clean(
    filesystem: Filesystem, files: List[str], ui: Optional[UI] = None
) -> None:
    """
    Deletes the files from the target filesystem. Files that are not found are ignored.

    Args:
        filesystem (Filesystem): The filesystem to delete files from
        files (list[str]): A list of paths to delete
        ui (Optional[UI]): A UI instance

    Returns:
        None

    Raises:
        None
    """
    _ui = ui or NullUI()
    for file in files:
        try:
            filesystem.delete(file)
        except FileNotFoundError as err:
            _ui.error(f"{error_type(err)}: Cannot delete file '{file}'")


def collect(
    source_filesystem: Filesystem,
    target_filesystem: Filesystem,
    files: List[CopyInstruction],
    ui: Optional[UI] = None,
) -> None:
    """
    Collects the files from the target filesystem.
    Files that are not found or already present on the source filesystem are ignored.

    Args:
        source_filesystem (Filesystem): The filesystem to copy FROM
        target_filesystem (Filesystem): The filesystem to copy TO
        files (list[CopyInstruction]): A list of CopyInstructions
        ui (Optional[UI]): A UI instance

    Returns:
        None
    """
    _ui = ui or NullUI()

    def log_error(error: Exception, instruction: CopyInstruction) -> None:
        _ui.error(f"{error_type(error)}: Cannot copy file '{instruction.source}'")

    copier = _Copier(
        source_filesystem,
        target_filesystem,
        abort_on_error=False,
        error_callback=log_error,
    )

    for copy_instruction in files:
        copier(copy_instruction)
