from dataclasses import dataclass
from typing import List, Tuple

from hpcrocket.core.executor import CommandExecutor


class SlurmError(RuntimeError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


@dataclass
class SlurmTask:
    id: str
    name: str
    state: str


@dataclass
class SlurmJob(SlurmTask):
    tasks: List[SlurmTask]

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


class SlurmRunner:

    def __init__(self, executor: CommandExecutor):
        self._executor = executor
        self._active_jobs: List[str] = []

    def sbatch(self, filename: str) -> str:
        cmd = self._executor.exec_command("sbatch " + filename)
        self._wait_for_success_or_raise(cmd)
        out = cmd.stdout()[0]
        job_id = out.split()[-1]
        self._active_jobs.append(job_id)
        return job_id

    def scancel(self, jobid: str) -> None:
        cmd = self._executor.exec_command("scancel " + jobid)
        cmd.wait_until_exit()
        self._wait_for_success_or_raise(cmd)

        try:
            self._active_jobs.remove(jobid)
        except ValueError:
            pass

    def poll_status(self, jobid: str) -> SlurmJob:
        cmd = self._executor.exec_command(
            f"sacct -j {jobid} -o jobid,jobname%30,state --noheader")
        cmd.wait_until_exit()
        main_task, tasks = self._collect_slurm_tasks(cmd.stdout())

        return SlurmJob(id=main_task.id,
                        name=main_task.name,
                        state=main_task.state,
                        tasks=tasks)

    @property
    def active_jobs(self) -> List[str]:
        return self._active_jobs

    def _wait_for_success_or_raise(self, cmd):
        cmd.wait_until_exit()
        if cmd.exit_status != 0:
            raise SlurmError(cmd.stderr())

    def _collect_slurm_tasks(self, output: List[str]) -> Tuple[SlurmTask, List[SlurmTask]]:
        main_task: SlurmTask
        tasks: List[SlurmTask] = []
        for index, line in enumerate(output):
            if not line:
                continue

            task_str_list = line.split()
            task = SlurmTask(task_str_list[0],
                             task_str_list[1], task_str_list[2])
            if index == 0:
                main_task = task

            tasks.append(task)

        return main_task, tasks
