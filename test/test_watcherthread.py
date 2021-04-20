from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest
from ssh_slurm_runner.slurmrunner import SlurmJob, SlurmRunner
from ssh_slurm_runner.watcher.watcherthread import WatcherThread


def test__given_completed_job__when_polling__should_trigger_callback():
    runner = runner_returning_job_with_state("COMPLETED")
    callback, call_capture = callback_and_capture()

    sut = WatcherThread(runner, "123456", callback, interval=0)

    sut.poll()

    assert call_capture['calls'] == 1


def test__given_running_job__when_polling_and_job_completes_after_second_poll__should_trigger_callback_twice():
    runner = runner_with_changing_job_state(completed_after_calls=2)
    callback, call_capture = callback_and_capture()

    sut = WatcherThread(runner, "123456", callback, interval=0)

    sut.poll()

    assert call_capture['calls'] == 2


def test__given_running_job__when_polling_and_job_completes_after_third__should_trigger_callback_only_twice():
    runner = runner_with_changing_job_state(completed_after_calls=3)
    callback, call_capture = callback_and_capture()

    sut = WatcherThread(runner, "123456", callback, interval=0)

    sut.poll()

    assert call_capture['calls'] == 2


def test__when_stopping_then_polling__should_not_trigger_callback():
    runner = runner_with_changing_job_state(completed_after_calls=1)
    callback, call_capture = callback_and_capture()

    sut = WatcherThread(runner, "123456", callback, interval=0)

    sut.stop()
    sut.poll()

    assert call_capture['calls'] == 0


def test__after_polling_completed_job__is_done_should_be_true():
    runner = runner_returning_job_with_state("COMPLETED")

    sut = WatcherThread(runner, "123456", lambda _: None, interval=0)

    sut.poll()

    assert sut.is_done()


def test__given_running_job__when_checking_is_done_until_completion__should_be_false_then_true():
    runner = runner_with_changing_job_state(completed_after_calls=2)

    context = {'done': []}

    def callback(job):
        sut = context['sut']
        context['done'].append(sut.is_done())

    sut = WatcherThread(runner, "123456", callback, interval=0)
    context['sut'] = sut

    sut.poll()

    assert context['done'] == [False, True]


@pytest.mark.timeout(2)
def test__given_running_job__when_polling__should_only_poll_in_given_interval():
    call_times = []
    poll_status = make_poll_status_for_changing_job(
        {'calls': 0}, completed_after_calls=2)

    def timerecording_wrapper(jobid):
        call_times.append(datetime.now())
        return poll_status(jobid)

    runner = Mock(SlurmRunner)
    runner.configure_mock(poll_status=timerecording_wrapper)

    sut = WatcherThread(runner, "123456", lambda _: None, interval=0.1)

    sut.poll()

    diff: timedelta = call_times[1] - call_times[0]
    assert diff.microseconds >= .1e6


def callback_and_capture():
    call_capture = {'calls': 0}
    def callback(job): call_capture['calls'] += 1

    return callback, call_capture


def make_poll_status(state: str):
    def poll_status(jobid: str):
        return SlurmJob(id="123456", name="MyJob", state=state, tasks=[])

    return poll_status


def runner_returning_job_with_state(state):
    runner_mock = Mock(spec=SlurmRunner)
    runner_mock.configure_mock(poll_status=make_poll_status(state))
    return runner_mock


def runner_with_changing_job_state(completed_after_calls: int):
    runner_mock = Mock(spec=SlurmRunner)
    call_capture = {'calls': 0}

    def poll_status(jobid):
        calls = call_capture['calls'] + 1
        call_capture['calls'] = calls
        if calls == completed_after_calls:
            return completed_job()

        return running_job()

    runner_mock.configure_mock(poll_status=poll_status)
    return runner_mock


def make_poll_status_for_changing_job(call_capture, completed_after_calls: int):
    def poll_status(jobid):
        calls = call_capture['calls'] + 1
        call_capture['calls'] = calls
        if calls == completed_after_calls:
            return completed_job()

        return running_job()

    return poll_status


def completed_job():
    return SlurmJob(id="123456", name="MyJob", state="COMPLETED", tasks=[])


def running_job():
    return SlurmJob(id="123456", name="MyJob", state="RUNNING", tasks=[])
