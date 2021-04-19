import time
import pytest

from ssh_slurm_runner.jobwatcher import JobWatcher
from unittest.mock import Mock, patch
from ssh_slurm_runner.slurmrunner import SlurmJob, SlurmRunner


@pytest.mark.timeout(1)
def test__when_watching_finished_job__should_trigger_callback():
    runner_mock = runner_returning_job_with_state("COMPLETED")

    sut = JobWatcher(runner_mock)

    callback_capture = {'finished': False}

    def callback(job):
        callback_capture['finished'] = True

    sut.watch("123456", callback, poll_interval=0.01)

    time.sleep(0.02)
    sut.stop()
    assert callback_capture['finished'] == True


@pytest.mark.timeout(1)
def test__when_watching_finished_job__is_done_should_be_true():
    runner_mock = runner_returning_job_with_state("COMPLETED")

    sut = JobWatcher(runner_mock)

    sut.watch("123456", lambda _: None, poll_interval=0.01)

    time.sleep(0.02)
    sut.stop()
    assert sut.is_done()


@pytest.mark.timeout(1)
def test__when_first_watching_running_job__should_trigger_callback():
    runner_mock = runner_returning_job_with_state("RUNNING")

    sut = JobWatcher(runner_mock)

    callback_capture = {'finished': False}

    def callback(job):
        callback_capture['finished'] = True

    sut.watch("123456", callback, poll_interval=0.01)

    time.sleep(0.02)
    sut.stop()
    assert callback_capture['finished'] == True


@pytest.mark.timeout(1)
def test__given_running_job__when_job_state_changes__should_trigger_callback_second_time():
    runner_mock = runner_returning_job_with_state("RUNNING")

    sut = JobWatcher(runner_mock)

    callback_capture = {'called': 0}

    def callback(job):
        callback_capture['called'] += 1

    sut.watch("123456", callback, poll_interval=0.01)

    time.sleep(0.1)
    runner_mock.configure_mock(poll_status=make_poll_status("COMPLETED"))

    time.sleep(0.01)
    sut.stop()
    assert callback_capture['called'] == 2


@pytest.mark.timeout(1)
def test__given_running_job__when_stopping_watcher__should_not_update_callback():
    runner_mock = runner_returning_job_with_state("RUNNING")

    sut = JobWatcher(runner_mock)

    callback_capture = {'called': 0}

    def callback(job):
        callback_capture['called'] += 1

    sut.watch("123456", callback, poll_interval=0.01)
    time.sleep(0.01)
    sut.stop()
    calls_at_stop = callback_capture['called']

    time.sleep(0.02)
    runner_mock.configure_mock(poll_status=make_poll_status("COMPLETED"))

    time.sleep(0.02)
    assert callback_capture['called'] == calls_at_stop


def make_poll_status(state: str):
    def poll_status(jobid: str):
        return SlurmJob(id="123456", name="MyJob", state=state, tasks=[])

    return poll_status


def runner_returning_job_with_state(state):
    runner_mock = Mock(spec=SlurmRunner)
    runner_mock.configure_mock(poll_status=make_poll_status(state))
    return runner_mock
