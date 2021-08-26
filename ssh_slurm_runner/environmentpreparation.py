from typing import List, Tuple
from ssh_slurm_runner.filesystem import Filesystem


class EnvironmentPreparation:

    def __init__(self, source_filesystem: Filesystem, target_filesystem: Filesystem = None) -> None:
        self._src_filesystem = source_filesystem
        self._target_filesystem = target_filesystem
        self._copy = list()
        self._delete = list()
        self._collect = list()

    def files_to_copy(self, src_dest_tuples: List[Tuple[str, str]]) -> None:
        self._copy = list(src_dest_tuples)

    def prepare(self) -> None:
        for src, dest in self._copy:
            self._src_filesystem.copy(src, dest, self._target_filesystem)

    def files_to_clean(self, files: List[str]) -> None:
        self._delete = list(files)

    def clean(self) -> None:
        for file in self._delete:
            self._target_filesystem.delete(file)

    def files_to_collect(self, files: List[str]) -> None:
        self._collect = list(files)

    def collect(self) -> None:
        for file in self._collect:
            self._target_filesystem.copy(file, file, self._src_filesystem)