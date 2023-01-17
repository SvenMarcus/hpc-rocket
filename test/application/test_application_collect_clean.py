from typing import Union
import unittest

import pytest
from hpcrocket.core.application import Application
from hpcrocket.core.filesystem.progressive import CopyInstruction
from hpcrocket.core.launchoptions import FinalizeOptions, LaunchOptions, Options
from test.application import make_application
from test.application.assertions import (
    assert_does_not_exist_locally,
    assert_does_not_exist_on_remote,
    assert_exists_locally,
    assert_exists_on_remote,
)
from test.testdoubles.filesystem import MemoryFilesystemFactoryStub
from test.application.optionbuilders import (
    COLLECTED_FILE,
    NON_MATCHING_FILE,
    REMOTE_FILE,
    finalize_options,
    finalize_options_with_clean,
    finalize_options_with_collect,
    launch_options,
    launch_options_with_clean,
    launch_options_with_collect,
)


@pytest.fixture(autouse=True)
def fs_factory() -> MemoryFilesystemFactoryStub:
    return MemoryFilesystemFactoryStub()


@pytest.fixture(autouse=True)
def sut(fs_factory: MemoryFilesystemFactoryStub) -> Application:
    return make_application(filesystem_factory=fs_factory)


@pytest.mark.parametrize(
    "options", [launch_options_with_collect(), finalize_options_with_collect()]
)
def test__app_with_collect_settings__collects_files_from_remote(
    sut: Application, fs_factory: MemoryFilesystemFactoryStub, options: Options
) -> None:
    fs_factory.create_remote_files(REMOTE_FILE)

    sut.run(options)

    assert_exists_locally(fs_factory, COLLECTED_FILE)


@pytest.mark.parametrize("options", [launch_options(watch=True), finalize_options()])
def test__app_with_glob_collect_settings__collects_only_matching_files(
    sut: Application,
    fs_factory: MemoryFilesystemFactoryStub,
    options: Union[LaunchOptions, FinalizeOptions],
) -> None:
    options.collect_files = [CopyInstruction("*.txt", "store_dir")]
    fs_factory.create_remote_files(REMOTE_FILE, NON_MATCHING_FILE)

    sut.run(options)

    assert_exists_locally(fs_factory, f"store_dir/{REMOTE_FILE}")
    assert_does_not_exist_locally(fs_factory, f"store_dir/{NON_MATCHING_FILE}")


@pytest.mark.parametrize("options", [launch_options(watch=True), finalize_options()])
def test__app_with_glob_collect_settings__when_file_exists_locally__still_collects_other_files(
    sut: Application,
    fs_factory: MemoryFilesystemFactoryStub,
    options: Union[LaunchOptions, FinalizeOptions],
) -> None:
    existing_file = "existing.txt"
    options.collect_files = [CopyInstruction("*.txt", "store_dir")]
    fs_factory.create_remote_files(existing_file, REMOTE_FILE)
    fs_factory.create_local_files(f"store_dir/{existing_file}")

    sut.run(options)

    assert_exists_locally(fs_factory, f"store_dir/{REMOTE_FILE}")


@pytest.mark.parametrize(
    "options", [launch_options_with_clean(), finalize_options_with_clean()]
)
def test__app_with_clean_settings__cleans_files(
    sut: Application, fs_factory: MemoryFilesystemFactoryStub, options: Options
) -> None:
    sut.run(options)

    assert_does_not_exist_on_remote(fs_factory, REMOTE_FILE)


@pytest.mark.parametrize("options", [launch_options(watch=True), finalize_options()])
def test__app_with_glob_clean_settings__cleans_only_matching_files(
    sut: Application,
    fs_factory: MemoryFilesystemFactoryStub,
    options: Union[LaunchOptions, FinalizeOptions],
) -> None:
    non_matching_file = "NON_MATCHING_FILE.gif"
    fs_factory.create_remote_files(REMOTE_FILE, non_matching_file)
    options.clean_files = ["*.txt"]

    sut.run(options)

    assert_does_not_exist_on_remote(fs_factory, REMOTE_FILE)
    assert_exists_on_remote(fs_factory, non_matching_file)
