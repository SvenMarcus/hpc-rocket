from unittest.mock import Mock, patch

import pytest
from hpcrocket.core.slurmbatchjob import SlurmBatchJob
from hpcrocket.watcher.jobwatcher import JobWatcher, NotWatchingError


@pytest.fixture
def watcherthread_stub():
    patcher = patch(
        "hpcrocket.watcher.watcherthread.WatcherThread")
    patched = patcher.start()

    yield patched

    patcher.stop()


@pytest.fixture
def runner_dummy():
    return Mock(SlurmBatchJob)


def test__when_calling_watch__should_spawn_watcher_thread(watcherthread_stub: Mock, runner_dummy: Mock):
    sut = JobWatcher(runner_dummy)

    def callback(_): return None
    sut.watch(callback, poll_interval=1)

    watcherthread_stub.assert_called_with(
        runner_dummy, callback, 1)

    watcherthread_mock: Mock = watcherthread_stub.return_value
    watcherthread_mock.start.assert_called_once()


def test__given_watched_job__is_done_should_return_is_done_from_watcherthread(watcherthread_stub: Mock, runner_dummy: Mock):
    watcherthread_mock: Mock = watcherthread_stub.return_value
    watcherthread_mock.is_done.return_value = False

    sut = JobWatcher(runner_dummy)
    sut.watch(lambda _: None, poll_interval=1)

    actual = sut.is_done()

    watcherthread_mock.is_done.assert_called_once()
    assert actual == False


def test__when_calling_is_done_before_watching__should_raise_not_watching_error(runner_dummy: Mock):
    sut = JobWatcher(runner_dummy)

    with pytest.raises(NotWatchingError):
        sut.is_done()


def test__given_watched_job__when_stopping__should_stop_and_join_watch_thread(watcherthread_stub: Mock, runner_dummy: Mock):
    calls = []
    watcherthread_mock: Mock = watcherthread_stub.return_value
    watcherthread_mock.configure_mock(
        stop=lambda: calls.append("stop"),
        join=lambda: calls.append("join")
    )

    sut = JobWatcher(runner_dummy)
    sut.watch(lambda _: None, poll_interval=1)

    sut.stop()

    assert calls == ["stop", "join"]


def test__given_watched_job__when_waiting_until_done__should_call_join(watcherthread_stub: Mock, runner_dummy: Mock):
    sut = JobWatcher(runner_dummy)
    sut.watch(lambda _: None, poll_interval=1)

    sut.wait_until_done()

    watcherthread_mock: Mock = watcherthread_stub.return_value
    watcherthread_mock.join.assert_called_once()


def test__when_calling_wait_until_done_before_watching__should_raise_not_watching_error(runner_dummy: Mock):
    sut = JobWatcher(runner_dummy)

    with pytest.raises(NotWatchingError):
        sut.wait_until_done()


def test__when_calling_stop_before_watching__should_raise_not_watching_error(runner_dummy: Mock):
    sut = JobWatcher(runner_dummy)

    with pytest.raises(NotWatchingError):
        sut.stop()
