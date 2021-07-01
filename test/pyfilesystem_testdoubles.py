from typing import List, Tuple
from unittest.mock import MagicMock

import fs.subfs


class PyFilesystemStub:

    def __init__(self, existing_files: List[str] = None, existing_dirs: List[str] = None) -> None:
        self.existing_files = existing_files or list()
        self.existing_dirs = existing_dirs or list()

    def isdir(self, path: str) -> bool:
        return path in self.existing_dirs

    def exists(self, path: str) -> bool:
        print(f"Get called with {path}")
        return path in self.existing_files or path in self.existing_dirs

    def makedirs(self, path: str) -> None:
        pass

    def remove(self, path: str) -> None:
        pass

    def opendir(self, path: str, factory=None) -> 'PyFilesystemStub':
        return self

class PyFilesystemFake(PyFilesystemStub):

    def __init__(self, existing_files: List[str] = None, existing_dirs: List[str] = None) -> None:
        super().__init__(existing_files=existing_files, existing_dirs=existing_dirs)

    def makedirs(self, path: str) -> None:
        return self.existing_dirs.append(path)

    def remove(self, path: str) -> None:
        self.existing_files.remove(path)

    def removetree(self, path: str) -> None:
        self.existing_dirs.remove(path)
        filtered = filter(lambda file: file.startswith(path),
                          self.existing_files)

        for file in filtered:
            self.existing_files.remove(file)


class VerifyDirsCreatedAndCopyPyFSMock(PyFilesystemStub):

    def __init__(self, expected_dirs: List[str],
                 expected_copies: List[Tuple[str, str]],
                 expected_calls: List[str] = None,
                 existing_files: List[str] = None,
                 existing_dirs: List[str] = None
                 ) -> None:
        super().__init__(
            existing_dirs=existing_dirs,
            existing_files=existing_files)

        self.expected_dirs = expected_dirs
        self.expected_copies = expected_copies
        self.expected_calls = expected_calls
        self.dirs_created = []
        self.copy_calls = list()
        self.calls = []

    def makedirs(self, path: str):
        self.calls.append("makedirs")
        self.dirs_created.append(path)
        return MagicMock(spec=fs.subfs.SubFS)

    def copy(self, src: str, dst: str):
        self.calls.append("copy")
        self.copy_calls.append((src, dst))

    def verify(self):
        assert self.expected_calls == self.calls
        assert self.expected_dirs == self.dirs_created
        assert self.expected_copies == self.copy_calls


def copy_file_between_filesystems_fake(origin_fs: PyFilesystemStub, origin_path: str, dest_fs: PyFilesystemStub, dest_path: str):
    assert origin_path in origin_fs.existing_files
    assert dest_path not in dest_fs.existing_files

    dest_fs.existing_files.append(dest_path)