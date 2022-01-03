from typing import List
from unittest.mock import Mock

import pytest
from hpcrocket.core.slurmbatchjob import SlurmBatchJob
from hpcrocket.watcher.jobwatcher import JobWatcherImpl, NotWatchingError, SlurmJobStatusCallback
from hpcrocket.watcher.watcherthread import WatcherThreadImpl


class WatcherThreadSpy:

    def __init__(self, job: SlurmBatchJob, callback: SlurmJobStatusCallback, poll_interval: int) -> None:
        self.job = job
        self.callback = callback
        self.poll_interval = poll_interval

        self.done = False
        self.running = False

        self.call_log: List[str] = []

    def is_running(self) -> bool:
        return self.running

    def start(self) -> None:
        self.running = True
        self.call_log.append("start")

    def join(self, timeout: float = 0) -> None:
        self.call_log.append("join")

    def stop(self) -> None:
        self.running = False
        self.call_log.append("stop")

    def is_done(self) -> bool:
        return self.done


class WatcherFactoryStub:

    def __init__(self, done_thread: bool = False) -> None:
        self.thread_spy: WatcherThreadSpy = None  # type: ignore
        self.done_thread = done_thread

    def __call__(self, job: SlurmBatchJob, callback: SlurmJobStatusCallback, interval: int) -> WatcherThreadImpl:
        self.thread_spy = WatcherThreadSpy(job, callback, interval)
        self.thread_spy.done = self.done_thread
        return self.thread_spy  # type: ignore


@pytest.fixture
def thread_factory_stub():
    return WatcherFactoryStub()


@pytest.fixture
def runner_dummy():
    return Mock(SlurmBatchJob)


def test__when_calling_watch__should_spawn_watcher_thread(thread_factory_stub: Mock, runner_dummy: Mock):
    wrapped = Mock(wraps=thread_factory_stub)
    sut = JobWatcherImpl(runner_dummy, wrapped)

    def callback(_): return None
    sut.watch(callback, poll_interval=1)

    wrapped.assert_called_with(runner_dummy, callback, 1)
    assert thread_factory_stub.thread_spy.is_running()


@pytest.mark.parametrize("thread_done", [True, False])
def test__given_watched_job__is_done_should_return_is_done_from_watcherthread(runner_dummy: Mock, thread_done: bool):
    factory_stub = WatcherFactoryStub(thread_done)
    sut = JobWatcherImpl(runner_dummy, factory_stub)
    sut.watch(lambda _: None, poll_interval=1)

    actual = sut.is_done()

    assert actual == thread_done


def test__when_calling_is_done_before_watching__should_raise_not_watching_error(runner_dummy: Mock):
    sut = JobWatcherImpl(runner_dummy, WatcherFactoryStub())

    with pytest.raises(NotWatchingError):
        sut.is_done()


def test__given_watched_job__when_stopping__should_stop_and_join_watch_thread(
        thread_factory_stub: Mock, runner_dummy: Mock):
    sut = JobWatcherImpl(runner_dummy, thread_factory_stub)
    sut.watch(lambda _: None, poll_interval=1)

    sut.stop()

    assert thread_factory_stub.thread_spy.call_log == ["start", "stop", "join"]


def test__given_watched_job__when_waiting_until_done__should_call_join(thread_factory_stub: Mock, runner_dummy: Mock):
    sut = JobWatcherImpl(runner_dummy, thread_factory_stub)
    sut.watch(lambda _: None, poll_interval=1)

    sut.wait_until_done()

    assert thread_factory_stub.thread_spy.call_log == ["start", "join"]


def test__when_calling_wait_until_done_before_watching__should_raise_not_watching_error(runner_dummy: Mock):
    sut = JobWatcherImpl(runner_dummy, WatcherFactoryStub())

    with pytest.raises(NotWatchingError):
        sut.wait_until_done()


def test__when_calling_stop_before_watching__should_raise_not_watching_error(runner_dummy: Mock):
    sut = JobWatcherImpl(runner_dummy, WatcherFactoryStub())

    with pytest.raises(NotWatchingError):
        sut.stop()
