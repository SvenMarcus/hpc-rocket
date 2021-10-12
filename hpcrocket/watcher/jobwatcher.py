from abc import abstractmethod
from typing import TYPE_CHECKING, Callable, Optional

import hpcrocket.watcher.watcherthread as wt


if TYPE_CHECKING:
    from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmJobStatus


SlurmJobStatusCallback = Callable[['SlurmJobStatus'], None]
WatcherThreadFactory = Callable[
    ['SlurmBatchJob', SlurmJobStatusCallback, int],
    wt.WatcherThread
]


def make_watcher_thread(
        job: 'SlurmBatchJob',
        callback: SlurmJobStatusCallback,
        poll_interval: int) -> wt.WatcherThread:

    return wt.WatcherThread(job, callback, poll_interval)


class NotWatchingError(RuntimeError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class JobWatcher:

    def __init__(self, runner: 'SlurmBatchJob', thread_factory: WatcherThreadFactory=make_watcher_thread) -> None:
        self.runner = runner
        self.factory = thread_factory
        self.watching_thread: wt.WatcherThread = None # type: ignore[assignment]

    def watch(self, callback: SlurmJobStatusCallback, poll_interval: int) -> None:
        self.watching_thread = self.factory(self.runner, callback, poll_interval)
        self.watching_thread.start()

    def is_done(self) -> bool:
        if self.watching_thread is None:
            raise NotWatchingError()

        return self.watching_thread.is_done()

    def wait_until_done(self) -> None:
        if self.watching_thread is None:
            raise NotWatchingError()

        self.watching_thread.join()

    def stop(self) -> None:
        if self.watching_thread is None:
            raise NotWatchingError()

        self.watching_thread.stop()
        self.watching_thread.join()
