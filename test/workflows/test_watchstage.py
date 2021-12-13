from test.application.launchoptions import options
from test.slurm_assertions import assert_job_polled
from test.slurmoutput import (DEFAULT_JOB_ID, completed_slurm_job,
                              running_slurm_job)
from test.testdoubles.executor import (FailedSlurmJobCommandStub,
                                       LongRunningSlurmJobExecutorSpy,
                                       SlurmJobExecutorSpy)
from unittest.mock import Mock, call

from hpcrocket.core.slurmbatchjob import SlurmBatchJob
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.stages import WatchStage
from hpcrocket.ui import UI
from hpcrocket.watcher.jobwatcher import JobWatcher


def make_sut(executor, factory=None):
    controller = SlurmController(executor, factory)
    batch_job = SlurmBatchJob(controller, DEFAULT_JOB_ID, factory)
    poll_interval = options().poll_interval
    return WatchStage(batch_job, poll_interval)


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


def test__given_long_running_failing_job__when_running__should_poll_until_failed():
    executor = LongRunningSlurmJobExecutorSpy(sacct_cmd=FailedSlurmJobCommandStub())
    sut = make_sut(executor)

    sut(Mock(spec=UI))

    assert_job_polled(executor, DEFAULT_JOB_ID, command_index=0)
    assert_job_polled(executor, DEFAULT_JOB_ID, command_index=1)
    assert_job_polled(executor, DEFAULT_JOB_ID, command_index=2)


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
    sut  = make_sut(executor, factory)
    sut(Mock(spec=UI))
    
    sut.cancel(Mock(spec=UI))
    
    assert watcher.stop.called is True

