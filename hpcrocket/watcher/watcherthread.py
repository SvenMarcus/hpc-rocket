import threading
from typing import TYPE_CHECKING, Callable, Optional

try:
    from typing import Protocol
except ImportError:  # pragma: no cover
    from typing_extensions import Protocol  # type: ignore


if TYPE_CHECKING:
    from hpcrocket.core.schedulers.base import BatchJob, JobStatus


class WatcherThread(Protocol):
    def start(self) -> None:
        """
        Start watching a job
        """
        ...

    def stop(self) -> None:
        """
        Stop watching a job
        """
        ...

    def is_done(self) -> bool:
        """
        Returns true if the job has finished running.

        Returns:
            bool
        """
        ...

    def join(self, timeout: Optional[float] = None) -> None:
        """
        Joins the WatcherThread.

        Args:
            timeout (float): An optional timeout for joining
        """


class WatcherThreadImpl(threading.Thread):
    def __init__(
        self,
        runner: "BatchJob",
        callback: Callable[["JobStatus"], None],
        interval: float,
    ):
        super().__init__(target=self.poll)
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
