from threading import Thread
from typing import TYPE_CHECKING, Callable

from hpcrocket.watcher.watcherthread import WatcherThread, WatcherThreadImpl

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore


if TYPE_CHECKING:
    from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmJobStatus


SlurmJobStatusCallback = Callable[['SlurmJobStatus'], None]
WatcherThreadFactory = Callable[
    ['SlurmBatchJob', SlurmJobStatusCallback, int],
    WatcherThread
]


def make_watcher_thread(
        job: 'SlurmBatchJob',
        callback: SlurmJobStatusCallback,
        poll_interval: int) -> WatcherThread:

    return WatcherThreadImpl(job, callback, poll_interval)


class NotWatchingError(RuntimeError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class JobWatcher(Protocol):

    def watch(self, callback: SlurmJobStatusCallback, poll_interval: int) -> None:
        pass

    def wait_until_done(self) -> None:
        pass

    def stop(self) -> None:
        pass


JobWatcherFactory = Callable[['SlurmBatchJob'], JobWatcher]


class JobWatcherImpl:

    def __init__(self, runner: 'SlurmBatchJob', thread_factory: WatcherThreadFactory = make_watcher_thread) -> None:
        self.runner = runner
        self.factory = thread_factory
        self.watching_thread: WatcherThread = None  # type: ignore[assignment]

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

        self._try_join()

    def stop(self) -> None:
        if self.watching_thread is None:
            raise NotWatchingError()

        self.watching_thread.stop()
        self._try_join()

    def _try_join(self):
        try:
            self.watching_thread.join()
        except RuntimeError as err:
            import logging
            logging.warning(err)
