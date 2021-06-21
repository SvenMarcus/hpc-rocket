from typing import List, Tuple
from unittest.mock import MagicMock, Mock, patch

import fs.base
import pytest
from fs import ResourceType
from ssh_slurm_runner.filesystem import Filesystem
from ssh_slurm_runner.sshfilesystem import PyFilesystemBased, SSHFilesystem


@pytest.fixture
def sshfs_type_mock():
    # The mocking does not work for some reason if only one of the paths is mocked
    patcher1 = patch("fs.sshfs.sshfs.SSHFS")
    patcher2 = patch("fs.sshfs.SSHFS")
    patcher1.start()
    mock = patcher2.start()

    yield mock

    patcher1.stop()
    patcher2.stop()


def test__given_ssh_client__when_creating_new_instance__should_create_sshfs_with_connection_data(sshfs_type_mock):
    sut = SSHFilesystem('user', 'host', 'privatekey')

    sshfs_type_mock.assert_called_with('host', user='user', pkey='privatekey')
