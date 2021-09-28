from dataclasses import dataclass
from hpcrocket.watcher.jobwatcher import JobWatcher
from typing import List, Tuple

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
        main_task, tasks = self._collect_slurm_tasks(cmd.stdout())

        return SlurmJobStatus(id=main_task.id,
                              name=main_task.name,
                              state=main_task.state,
                              tasks=tasks)

    def get_watcher(self) -> JobWatcher:
        return JobWatcher(self)

    def _raise_if_not_submitted(self):
        if not self._job_id:
            raise SlurmError("Job has not been submitted")

    def _wait_for_success_or_raise(self, cmd):
        cmd.wait_until_exit()
        if cmd.exit_status != 0:
            raise SlurmError(cmd.stderr())

    def _collect_slurm_tasks(self, output: List[str]) -> Tuple[SlurmTaskStatus, List[SlurmTaskStatus]]:
        main_task: SlurmTaskStatus
        tasks: List[SlurmTaskStatus] = []
        for index, line in enumerate(output):
            if not line:
                continue

            task_str_list = line.split()
            task = SlurmTaskStatus(task_str_list[0],
                                   task_str_list[1], task_str_list[2])
            if index == 0:
                main_task = task

            tasks.append(task)

        return main_task, tasks
