import os
from typing import Generator
from unittest.mock import patch

import pytest
from hpcrocket import ProductionServiceRegistry, main


@pytest.fixture(autouse=True)
def setup_teardown() -> Generator[None, None, None]:
    current_dir = os.getcwd()
    os.chdir("test/testconfig")
    yield

    try:
        os.remove("hello.txt")
        os.remove("test.txt")
        os.remove("a_created_file.txt")
    finally:
        os.chdir(current_dir)


@pytest.mark.acceptance
def test__launching__copies_files__launches_job__collects_and_cleans() -> None:
    args = [
        "hpc-rocket",
        "launch",
        "--watch",
        "integration_launch_config_fail_allowed.yml",
    ]

    registry = ProductionServiceRegistry()
    with patch("sys.exit") as sys_exit:
        main(args, registry)

    local_fs = registry.local_filesystem()

    assert local_fs.exists("hello.txt")
    assert local_fs.exists("test.txt")
    assert local_fs.exists("a_created_file.txt")
    sys_exit.assert_called_with(0)
