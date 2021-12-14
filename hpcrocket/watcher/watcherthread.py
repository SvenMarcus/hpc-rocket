import threading
from typing import TYPE_CHECKING, Callable, Optional

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore


if TYPE_CHECKING:
    from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmJobStatus


class WatcherThread(Protocol):

    def start(self):
        pass

    def stop(self):
        pass

    def is_done(self) -> bool:
        pass

    def join(self, timeout: Optional[float]):
        pass


class WatcherThreadImpl(threading.Thread):

    def __init__(self, runner: 'SlurmBatchJob',
                 callback: Callable[['SlurmJobStatus'], None],
                 interval: float):
        super(WatcherThreadImpl, self).__init__(target=self.poll)
        self.runner = runner
        self.callback = callback
        self.interval = interval
        self.stop_event = threading.Event()
        self._done = False

    def poll(self) -> None:
        last_job = None
        while not self.stop_event.wait(self.interval):
            job = self.runner.poll_status()
            self._done = not (job.is_running or job.is_pending)

            if job != last_job:
                self.callback(job)
                last_job = job

            if self._done:
                break

    def stop(self) -> None:
        print("Setting stop event")
        self.stop_event.set()
        self._done = True

    def is_done(self) -> bool:
        return self._done
