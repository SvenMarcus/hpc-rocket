from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

from hpcrocket.watcher.jobwatcher import JobWatcherFactory, JobWatcher, JobWatcherImpl

if TYPE_CHECKING:
    from hpcrocket.core.slurmcontroller import SlurmController


class SlurmError(RuntimeError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


@dataclass
class SlurmTaskStatus:
    id: str
    name: str
    state: str


@dataclass
class SlurmJobStatus:

    @classmethod
    def empty(cls) -> 'SlurmJobStatus':
        return SlurmJobStatus("", "", "", [])

    @classmethod
    def from_output(cls, output: List[str]) -> 'SlurmJobStatus':
        tasks = [
            SlurmTaskStatus(*line.split()[:3])
            for line in output if line
        ]

        main_task = (tasks[0] if tasks
                     else SlurmTaskStatus("", "", ""))

        return SlurmJobStatus(id=main_task.id,
                              name=main_task.name,
                              state=main_task.state,
                              tasks=tasks)

    id: str
    name: str
    state: str
    tasks: List[SlurmTaskStatus]

    @property
    def is_pending(self) -> bool:
        return self.state == "PENDING"

    @property
    def is_running(self) -> bool:
        return self.state == "RUNNING"

    @property
    def is_completed(self) -> bool:
        return self.state == "COMPLETED"

    @property
    def success(self) -> bool:
        return (self.state == "COMPLETED" and
                all(task.state == "COMPLETED"
                    for task in self.tasks))


class SlurmBatchJob:

    def __init__(
            self, controller: 'SlurmController', jobid: str = "", watcher_factory: Optional[JobWatcherFactory] = None):
        self._controller = controller
        self._watcher_factory = watcher_factory or JobWatcherImpl
        self.jobid = jobid

    def cancel(self) -> None:
        self._controller.cancel(self.jobid)

    def poll_status(self) -> SlurmJobStatus:
        return self._controller.poll_status(self.jobid)

    def get_watcher(self) -> JobWatcher:
        return self._watcher_factory(self)
