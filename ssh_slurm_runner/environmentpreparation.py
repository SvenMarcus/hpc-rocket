from typing import List, Tuple
from ssh_slurm_runner.filesystem import Filesystem


class EnvironmentPreparation:

    def __init__(self, filesystem: Filesystem) -> None:
        self._filesystem = filesystem
        self._copy = list()
        self._delete = list()

    def files_to_copy(self, src_dest_tuples: List[Tuple[str, str]]) -> None:
        self._copy = list(src_dest_tuples)

    def prepare(self) -> None:
        for src, dest in self._copy:
            self._filesystem.copy(src, dest)

    def files_to_clean(self, files: List[str]) -> None:
        self._delete = list(files)

    def clean(self) -> None:
        for file in self._delete:
            self._filesystem.delete(file)
