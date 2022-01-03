from test.application.launchoptions import launch_options
from test.slurm_assertions import assert_job_polled
from test.slurmoutput import (DEFAULT_JOB_ID, completed_slurm_job,
                              running_slurm_job)
from test.testdoubles.executor import (CommandExecutorStub,
                                       FailedSlurmJobCommandStub,
                                       LongRunningSlurmJobExecutorSpy,
                                       SlurmJobExecutorSpy)
from typing import List
from unittest.mock import Mock, call

import pytest
from hpcrocket.core.slurmbatchjob import SlurmBatchJob
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.stages import WatchStage
from hpcrocket.ui import UI
from hpcrocket.watcher.jobwatcher import (JobWatcher, JobWatcherFactory,
                                          NotWatchingError)


class WatcherSpy:

    def __init__(self) -> None:
        self._log: List[str] = []

    def was_awaited(self) -> bool:
        return self._log == ["watch", "wait"]

    def watch(self, callback, poll_interval):
        self._log.append("watch")

    def wait_until_done(self):
        self._log.append("wait")


class BatchJobProviderSpy:

    def __init__(self, controller: SlurmController, jobid: str, factory: JobWatcherFactory) -> None:
        self.controller = controller
        self.jobid = jobid
        self.factory = factory

        self.was_canceled = False

    def get_batch_job(self) -> SlurmBatchJob:
        return SlurmBatchJob(self.controller, self.jobid, self.factory)

    def cancel(self, ui: UI):
        self.was_canceled = True


def make_job_provider(executor, factory=None):
    controller = SlurmController(executor, factory)
    return BatchJobProviderSpy(controller, DEFAULT_JOB_ID, factory)


def make_sut(executor, factory=None):
    poll_interval = launch_options().poll_interval
    provider = make_job_provider(executor, factory)
    return WatchStage(provider, poll_interval)


def make_sut_with_provider(provider):
    poll_interval = launch_options().poll_interval
    return WatchStage(provider, poll_interval)


def test__given_jobid__when_running__should_poll_job():
    executor = SlurmJobExecutorSpy()
    sut = make_sut(executor)

    sut(Mock(spec=UI))

    assert_job_polled(executor, DEFAULT_JOB_ID)


def test__when_job_completes__should_return_true():
    executor = SlurmJobExecutorSpy()
    sut = make_sut(executor)

    result = sut(Mock(spec=UI))

    assert result is True


def test__when_job_completes_with_failure__should_return_false():
    executor = SlurmJobExecutorSpy(sacct_cmd=FailedSlurmJobCommandStub())
    sut = make_sut(executor)

    result = sut(Mock(spec=UI))

    assert result is False


def test__given_long_running_job__when_running__should_poll_job_until_done():
    executor = LongRunningSlurmJobExecutorSpy()
    sut = make_sut(executor)

    sut(Mock(spec=UI))

    assert_job_polled(executor, DEFAULT_JOB_ID, command_index=0)
    assert_job_polled(executor, DEFAULT_JOB_ID, command_index=1)
    assert_job_polled(executor, DEFAULT_JOB_ID, command_index=2)


def test__given_successful_job_and_watching__should_wait_until_job_is_done():
    watcher = WatcherSpy()

    def factory(job):
        return watcher

    executor = LongRunningSlurmJobExecutorSpy()
    sut = make_sut(executor, factory=factory)

    sut(Mock(spec=UI))

    assert watcher.was_awaited()


def test__when_running__should_update_ui_with_job_status():
    ui = Mock(spec=UI)
    executor = SlurmJobExecutorSpy()
    sut = make_sut(executor)

    sut(ui)

    ui.update.assert_called_once_with(completed_slurm_job())


def test__given_long_running_job__when_running__should_update_ui_with_job_updates():
    ui = Mock(spec=UI)
    executor = LongRunningSlurmJobExecutorSpy()
    sut = make_sut(executor)

    sut(ui)

    assert ui.update.mock_calls[0] == call(running_slurm_job())
    assert ui.update.mock_calls[-1] == call(completed_slurm_job())


def test__given_running_stage__when_canceling__should_stop_watcher():
    watcher = Mock(spec=JobWatcher)

    def factory(*args, **kwargs):
        return watcher

    executor = SlurmJobExecutorSpy()
    sut = make_sut(executor, factory=factory)
    sut(Mock(spec=UI))

    sut.cancel(Mock(spec=UI))

    assert watcher.stop.called is True


def test__given_running_stage__when_canceling__should_notify_batch_job_provider():
    executor = LongRunningSlurmJobExecutorSpy()
    provider_spy = make_job_provider(executor)
    sut = make_sut_with_provider(provider_spy)
    sut(Mock())

    sut.cancel(Mock())

    assert provider_spy.was_canceled is True


def test__when_canceling_before_running__should_raise_not_watching_error():
    executor = CommandExecutorStub()
    sut = make_sut(executor)

    with pytest.raises(NotWatchingError):
        sut.cancel(Mock(spec=UI))
