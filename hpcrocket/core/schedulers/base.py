from dataclasses import dataclass
from typing import List, Optional, Protocol

from hpcrocket.watcher.jobwatcher import JobWatcher, JobWatcherFactory, JobWatcherImpl


@dataclass
class TaskStatus:
    id: str
    name: str
    state: str


class JobStatus(Protocol):
    """
    The status of a job submitted to a job scheduler
    """

    @property
    def id(self) -> str:
        """
        Returns the job id.
        """
        ...

    @property
    def name(self) -> str:
        """
        Returns the job name.
        """
        ...

    @property
    def state(self) -> str:
        """
        Returns the job state.
        """
        ...

    @property
    def tasks(self) -> List[TaskStatus]:
        """
        Returns the list of tasks.
        """
        ...

    @property
    def is_pending(self) -> bool:
        """
        Returns True if the job is pending.
        """
        ...

    @property
    def is_running(self) -> bool:
        """
        Returns True if the job is running.
        """
        ...

    @property
    def is_completed(self) -> bool:
        """
        Returns True if the job is completed.
        """
        ...

    @property
    def success(self) -> bool:
        """
        Returns True if the job was successful.
        """
        ...


class BatchJob:
    def __init__(
        self,
        controller: "Scheduler",
        jobid: str = "",
        watcher_factory: Optional[JobWatcherFactory] = None,
    ):
        self._controller = controller
        self._watcher_factory = watcher_factory or JobWatcherImpl
        self.jobid = jobid

    def cancel(self) -> None:
        self._controller.cancel(self.jobid)

    def poll_status(self) -> JobStatus:
        return self._controller.poll_status(self.jobid)

    def get_watcher(self) -> JobWatcher:
        return self._watcher_factory(self)


class Scheduler(Protocol):
    """
    A protocol for a scheduler
    """

    def submit(self, jobfile: str) -> BatchJob:
        """
        Submits a job to the scheduler and returns a Job object.
        """
        ...

    def poll_status(self, jobid: str) -> JobStatus:
        """
        Polls the status of a job with the given jobid.
        """
        ...

    def cancel(self, jobid: str) -> None:
        """
        Cancels the job with the given jobid.
        """
        ...
