from ssh_slurm_runner.executor import CommandExecutor
from typing import List, Tuple
from dataclasses import dataclass, field
from enum import Enum
import ssh_slurm_runner.sshclient as ssh


class SlurmError(RuntimeError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class JobState(Enum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    CANCELED = "CANCELED"
    FAILED = "FAILED"


@dataclass
class SlurmTask:
    id: str
    name: str
    state: str


@dataclass
class SlurmJob(SlurmTask):
    tasks: List[SlurmTask]


class SlurmRunner:

    def __init__(self, executor: CommandExecutor):
        self._executor = executor
        self._active_jobs = []

    def sbatch(self, filename: str) -> str:
        cmd = self._executor.exec_command("sbatch " + filename)
        self._wait_for_success_or_raise(cmd)

        out = cmd.stdout()
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
        output = cmd.stdout()
        main_task, tasks = self._collect_slurm_tasks(output)

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

    def _collect_slurm_tasks(self, output: str) -> Tuple[SlurmTask, List[SlurmTask]]:
        main_task: SlurmTask
        tasks: List[SlurmTask] = []
        splitlines = output.splitlines()
        for index, line in enumerate(splitlines):
            if not line:
                continue

            task_str_list = line.split()
            task = SlurmTask(task_str_list[0],
                             task_str_list[1], task_str_list[2])
            if index == 0:
                main_task = task

            tasks.append(task)

        return main_task, tasks
