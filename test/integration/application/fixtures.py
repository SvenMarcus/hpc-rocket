import os
from typing import Generator
from unittest.mock import Mock, patch

import pytest
from fs.memoryfs import MemoryFS

from test.testdoubles.pyfilesystem import ArbitraryArgsMemoryFS, OnlySubFSMemoryFS

HOME_DIR = "/home/myuser"

INPUT_AND_EXPECTED_KEYFILE_PATHS = [
    ("my_private_keyfile", "my_private_keyfile"),
    ("~/.ssh/private_keyfile", f"{HOME_DIR}/.ssh/private_keyfile"),
    ("~/~folder~/private_keyfile", f"{HOME_DIR}/~folder~/private_keyfile"),
    ("~folder~/private_keyfile", "~folder~/private_keyfile"),
]


@pytest.fixture
def sshclient_type_mock() -> Generator[Mock, None, None]:
    patcher = patch("paramiko.SSHClient")
    type_mock = patcher.start()

    yield type_mock

    patcher.stop()


@pytest.fixture(autouse=True)
def osfs_type_mock() -> Generator[Mock, None, None]:
    patcher = patch("fs.osfs.OSFS")
    osfs_type_mock = patcher.start()
    osfs_type_mock.return_value = Mock(spec=MemoryFS, wraps=ArbitraryArgsMemoryFS())
    yield osfs_type_mock

    patcher.stop()


@pytest.fixture(autouse=True)
def sshfs_type_mock() -> Generator[Mock, None, None]:
    patcher = patch("hpcrocket.ssh.chmodsshfs.PermissionChangingSSHFSDecorator")

    sshfs_type_mock = patcher.start()
    mem_fs = OnlySubFSMemoryFS()
    mem_fs.makedirs(HOME_DIR)
    sshfs_type_mock.return_value = Mock(spec=MemoryFS, wraps=mem_fs)
    sshfs_type_mock.return_value.homedir = lambda: HOME_DIR

    yield sshfs_type_mock

    patcher.stop()


@pytest.fixture
def fs_copy_file_mock() -> Generator[Mock, None, None]:
    patcher = patch("fs.copy.copy_file")
    yield patcher.start()

    patcher.stop()


@pytest.fixture(autouse=True)
def home_dir() -> None:
    os.environ["HOME"] = HOME_DIR
