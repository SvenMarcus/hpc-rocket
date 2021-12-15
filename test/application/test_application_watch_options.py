from unittest.mock import Mock

from test.application.launchoptions import main_connection, proxy_connection
from test.slurm_assertions import assert_job_polled
from test.slurmoutput import DEFAULT_JOB_ID
from test.testdoubles.executor import FailedSlurmJobCommandStub, LongRunningSlurmJobExecutorSpy, SlurmJobExecutorSpy
from test.testdoubles.filesystem import DummyFilesystemFactory

from hpcrocket.core.application import Application
from hpcrocket.core.launchoptions import WatchOptions

WATCH_OPTIONS = WatchOptions(
    jobid=DEFAULT_JOB_ID,
    connection=main_connection(),
    proxyjumps=[proxy_connection()],
    poll_interval=0
)


def make_sut(executor):
    return Application(executor, DummyFilesystemFactory(), Mock())


def test__given_watch_options__when_running__should_poll_job_until_done():
    executor = LongRunningSlurmJobExecutorSpy()
    sut = make_sut(executor)

    sut.run(WATCH_OPTIONS)

    assert_job_polled(executor, command_index=0)
    assert_job_polled(executor, command_index=1)
    assert_job_polled(executor, command_index=2)


def test__given_watch_options__when_running_with_successful_job__should_exit_with_0():
    executor = SlurmJobExecutorSpy()
    sut = make_sut(executor)

    actual = sut.run(WATCH_OPTIONS)

    assert actual == 0


def test__given_watch_options__when_running_with_failing_job__should_exit_with_1():
    executor = SlurmJobExecutorSpy(sacct_cmd=FailedSlurmJobCommandStub())
    sut = make_sut(executor)

    actual = sut.run(WATCH_OPTIONS)

    assert actual == 1
