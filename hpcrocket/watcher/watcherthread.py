import threading
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmJobStatus


class WatcherThread(threading.Thread):

    def __init__(self, runner: 'SlurmBatchJob',
                 callback: Callable[['SlurmJobStatus'], None],
                 interval: float):
        super(WatcherThread, self).__init__(target=self.poll)
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
        self.stop_event.set()

    def is_done(self) -> bool:
        return self._done
