from dataclasses import dataclass
from typing import List

from hpcrocket.core.schedulers.base import JobStatus, TaskStatus


class SlurmError(RuntimeError):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


@dataclass
class SlurmJobStatus:
    @classmethod
    def empty(cls) -> "SlurmJobStatus":
        return SlurmJobStatus("", "", "", [])

    @classmethod
    def from_output(cls, output: List[str]) -> JobStatus:
        tasks = [TaskStatus(*line.split()[:3]) for line in output if line]

        main_task = tasks[0] if tasks else TaskStatus("", "", "")

        return SlurmJobStatus(
            id=main_task.id, name=main_task.name, state=main_task.state, tasks=tasks
        )

    id: str
    name: str
    state: str
    tasks: List[TaskStatus]

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
        return self.state == "COMPLETED" and all(
            task.state == "COMPLETED" for task in self.tasks
        )
