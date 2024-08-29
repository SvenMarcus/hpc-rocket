from datetime import datetime, timedelta
from typing import Any, Callable, Union
from unittest.mock import Mock

import pytest

from hpcrocket.core.schedulers.base import BatchJob, JobStatus
from hpcrocket.core.schedulers.slurmstatus import SlurmJobStatus
from hpcrocket.watcher.watcherthread import WatcherThreadImpl


def test__given_completed_job__when_polling__should_trigger_callback() -> None:
    runner = runner_returning_job_with_state("COMPLETED")
    callback, call_capture = callback_and_capture()

    sut = WatcherThreadImpl(runner, callback, interval=0)

    sut.poll()

    assert call_capture["calls"] == 1


def test__given_running_job__when_polling_and_job_completes_after_second_poll__should_trigger_callback_twice() -> (
    None
):
    runner = runner_with_job_change_after_calls(
        calls=2, initial_job=running_job(), next_job=completed_job()
    )

    callback, call_capture = callback_and_capture()

    sut = WatcherThreadImpl(runner, callback, interval=0)

    sut.poll()

    assert call_capture["calls"] == 2


def test__given_running_job__when_polling_and_job_completes_after_third__should_trigger_callback_only_twice() -> (
    None
):
    runner = runner_with_job_change_after_calls(
        calls=3, initial_job=running_job(), next_job=completed_job()
    )

    callback, call_capture = callback_and_capture()
    sut = WatcherThreadImpl(runner, callback, interval=0)

    sut.poll()

    assert call_capture["calls"] == 2


def test__given_pending_job__when_polling_and_job_completes_after_second_poll__should_trigger_callback_twice() -> (
    None
):
    runner = runner_with_job_change_after_calls(
        calls=2, initial_job=pending_job(), next_job=completed_job()
    )

    callback, call_capture = callback_and_capture()
    sut = WatcherThreadImpl(runner, callback, interval=0)

    sut.poll()

    assert call_capture["calls"] == 2


def test__when_stopping_then_polling__should_not_trigger_callback() -> None:
    runner = runner_with_job_change_after_calls(
        calls=1, initial_job=running_job(), next_job=completed_job()
    )

    callback, call_capture = callback_and_capture()

    sut = WatcherThreadImpl(runner, callback, interval=0)

    sut.stop()
    sut.poll()

    assert call_capture["calls"] == 0


def test__after_polling_completed_job__is_done_should_be_true() -> None:
    runner = runner_returning_job_with_state("COMPLETED")

    sut = WatcherThreadImpl(runner, lambda _: None, interval=0)

    sut.poll()

    assert sut.is_done()


def test__given_running_job__when_checking_is_done_until_completion__should_be_false_then_true() -> (
    None
):
    runner = runner_with_job_change_after_calls(
        calls=2, initial_job=running_job(), next_job=completed_job()
    )

    context: dict[str, Any] = {"done": []}

    def callback(job: JobStatus) -> None:
        sut = context["sut"]
        context["done"].append(sut.is_done())

    sut = WatcherThreadImpl(runner, callback, interval=0)
    context["sut"] = sut

    sut.poll()

    assert context["done"] == [False, True]


def test__given_running_job__when_checking_is_done_until_canceled__should_be_false_then_true() -> (
    None
):
    runner = runner_with_job_change_after_calls(
        calls=2, initial_job=running_job(), next_job=canceled_job()
    )

    context: dict[str, Any] = {"done": []}

    def callback(job: JobStatus) -> None:
        sut = context["sut"]
        context["done"].append(sut.is_done())

    sut = WatcherThreadImpl(runner, callback, interval=0)
    context["sut"] = sut

    sut.poll()

    assert context["done"] == [False, True]


@pytest.mark.timeout(2)
def test__given_running_job__when_polling__should_only_poll_in_given_interval() -> None:
    call_times = []
    poll_status = make_poll_status_with_job_change_after_calls(
        call_count=2, next_job=completed_job()
    )

    def timerecording_wrapper() -> SlurmJobStatus:
        call_times.append(datetime.now())
        return poll_status()

    runner = Mock(BatchJob)
    runner.configure_mock(poll_status=timerecording_wrapper)

    sut = WatcherThreadImpl(runner, lambda _: None, interval=0.1)

    sut.poll()

    diff: timedelta = call_times[1] - call_times[0]
    assert diff.microseconds >= 0.1e6


def callback_and_capture() -> tuple[Callable[[Any], None], dict[str, int]]:
    call_capture = {"calls": 0}

    def callback(job: Any) -> None:
        call_capture["calls"] += 1

    return callback, call_capture


def make_poll_status(state: str) -> Callable[[], SlurmJobStatus]:
    def poll_status() -> SlurmJobStatus:
        return SlurmJobStatus(id="123456", name="MyJob", state=state, tasks=[])

    return poll_status


def runner_returning_job_with_state(state: str) -> Mock:
    runner_mock = Mock(spec=BatchJob)
    runner_mock.configure_mock(poll_status=make_poll_status(state))
    return runner_mock


def runner_with_job_change_after_calls(
    calls: int, initial_job: SlurmJobStatus, next_job: SlurmJobStatus
) -> Mock:
    runner_mock = Mock(spec=BatchJob)
    poll_status = make_poll_status_with_job_change_after_calls(
        calls, initial_job=initial_job, next_job=next_job
    )

    runner_mock.configure_mock(poll_status=poll_status)
    return runner_mock


def make_poll_status_with_job_change_after_calls(
    call_count: int,
    next_job: SlurmJobStatus,
    initial_job: Union[SlurmJobStatus | None] = None,
) -> Callable[[], SlurmJobStatus]:
    call_capture = {"calls": 0}

    def poll_status() -> SlurmJobStatus:
        calls = call_capture["calls"] + 1
        call_capture["calls"] = calls
        if calls == call_count:
            return next_job

        return initial_job or running_job()

    return poll_status


def completed_job() -> SlurmJobStatus:
    return SlurmJobStatus(id="123456", name="MyJob", state="COMPLETED", tasks=[])


def canceled_job() -> SlurmJobStatus:
    return SlurmJobStatus(id="123456", name="MyJob", state="CANCELED", tasks=[])


def running_job() -> SlurmJobStatus:
    return SlurmJobStatus(id="123456", name="MyJob", state="RUNNING", tasks=[])


def pending_job() -> SlurmJobStatus:
    return SlurmJobStatus(id="123456", name="MyJob", state="PENDING", tasks=[])
