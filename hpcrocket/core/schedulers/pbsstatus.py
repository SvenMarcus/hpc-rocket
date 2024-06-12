from dataclasses import dataclass
from typing import List

from hpcrocket.core.schedulers.base import TaskStatus


class PbsError(RuntimeError):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


@dataclass
class PbsJobStatus:
    @classmethod
    def empty(cls) -> "PbsJobStatus":
        return PbsJobStatus("", "", "", [])

    @classmethod
    def from_output(cls, output: List[str]) -> "PbsJobStatus":
        tasks = [TaskStatus(*line.split()[:3]) for line in output if line]

        main_task = tasks[0] if tasks else TaskStatus("", "", "")

        return PbsJobStatus(
            id=main_task.id, name=main_task.name, state=main_task.state, tasks=tasks
        )

    id: str
    name: str
    state: str
    tasks: List[TaskStatus]

    @property
    def is_pending(self) -> bool:
        return self.state == "Q"

    @property
    def is_running(self) -> bool:
        return self.state in ("R", "E")

    @property
    def is_completed(self) -> bool:
        return self.state == "F"

    @property
    def success(self) -> bool:
        return self.state == "F" and all(task.state == "F" for task in self.tasks)
