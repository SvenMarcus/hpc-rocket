from hpcrocket.core.filesystem.progressive import CopyInstruction
from hpcrocket.core.launchoptions import FinalizeOptions
from hpcrocket.ssh.connectiondata import ConnectionData
from test.application import make_application
from test.slurmoutput import DEFAULT_JOB_ID
from test.testdoubles.filesystem import MemoryFilesystemFactoryStub

OPTIONS = FinalizeOptions(
    connection=ConnectionData("host", "user", "password"),
    collect_files=[CopyInstruction("remote_file", "local_file")],
    clean_files=["remote_file"],
)


def test__given_finalize_options__when_running__collects_and_cleans_files() -> None:
    factory = MemoryFilesystemFactoryStub()
    factory.create_remote_files("remote_file")

    sut = make_application(filesystem_factory=factory)

    sut.run(OPTIONS)

    local_fs = factory.local_filesystem
    assert local_fs.exists("local_file")

    remote_fs = factory.ssh_filesystem
    assert not remote_fs.exists("remote_file")
