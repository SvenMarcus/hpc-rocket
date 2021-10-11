from dataclasses import dataclass
from hpcrocket.watcher.jobwatcher import JobWatcher
from typing import List

from hpcrocket.core.executor import CommandExecutor


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

    def __init__(self, executor: CommandExecutor, filename: str):
        self._executor = executor
        self._filename = filename
        self._job_id: str = ""

    def submit(self) -> str:
        cmd = self._executor.exec_command("sbatch " + self._filename)
        self._wait_for_success_or_raise(cmd)
        out = cmd.stdout()[0]
        self._job_id = out.split()[-1]

        return self._job_id

    def cancel(self) -> None:
        self._raise_if_not_submitted()
        cmd = self._executor.exec_command("scancel " + self._job_id)
        self._wait_for_success_or_raise(cmd)

    def poll_status(self) -> SlurmJobStatus:
        self._raise_if_not_submitted()
        cmd = self._executor.exec_command(
            f"sacct -j {self._job_id} -o jobid,jobname%30,state --noheader")
        cmd.wait_until_exit()

        return SlurmJobStatus.from_output(cmd.stdout())

    def get_watcher(self) -> JobWatcher:
        return JobWatcher(self)

    def _raise_if_not_submitted(self):
        if not self._job_id:
            raise SlurmError("Job has not been submitted")

    def _wait_for_success_or_raise(self, cmd):
        cmd.wait_until_exit()
        if cmd.exit_status != 0:
            raise SlurmError(cmd.stderr())
