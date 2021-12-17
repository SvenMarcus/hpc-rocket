from typing import List, NamedTuple, Optional

from hpcrocket.core.errors import error_type
from hpcrocket.core.filesystem import Filesystem
from hpcrocket.ui import UI, NullUI


class CopyInstruction(NamedTuple):
    """
    Copy instruction for a file.
    """
    source: str
    destination: str
    overwrite: bool = False


class EnvironmentPreparation:
    """
    This class is responsible for copying and deleting files from the source and target filesystems.
    """

    def __init__(self, source_filesystem: Filesystem, target_filesystem: Filesystem, ui: Optional[UI] = None) -> None:
        self._src_filesystem = source_filesystem
        self._target_filesystem = target_filesystem
        self._ui = ui or NullUI()
        self._copy: List[CopyInstruction] = list()
        self._collect: List[CopyInstruction] = list()
        self._delete: List[str] = list()
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
        for src, dest, overwrite in self._copy:
            self._src_filesystem.copy(src, dest, overwrite,
                                      filesystem=self._target_filesystem)

            self._copied_files.append(dest)

    def files_to_clean(self, files: List[str]) -> None:
        """
        Sets the files to delete from the target filesystem.

        Args:
            files: A list of files to delete

        Returns:
            None
        """
        self._delete = list(files)

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
        for file in self._delete:
            self._try_delete(file)

    def _try_delete(self, file: str) -> bool:
        try:
            self._target_filesystem.delete(file)
        except FileNotFoundError as err:
            self._ui.error(
                f"{error_type(err)}: Cannot delete file '{file}'")
            return False

        return True

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
        for src, dst, overwrite in self._collect:
            try:
                self._target_filesystem.copy(src, dst, overwrite,
                                             filesystem=self._src_filesystem)
            except (FileNotFoundError, FileExistsError) as err:
                self._ui.error(
                    f"{error_type(err)}: Cannot copy file '{src}'")

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
            deleted = self._try_delete(file)
            if deleted:
                deleted_files.append(file)

        for file in deleted_files:
            self._copied_files.remove(file)
