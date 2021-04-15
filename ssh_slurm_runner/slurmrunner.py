from ssh_slurm_runner.executor import CommandExecutor
from typing import List
from dataclasses import dataclass
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
class SlurmJob:
    id: str
    name: str
    state: str


class SlurmRunner:

    def __init__(self, executor: CommandExecutor):
        self._executor = executor
        self._active_jobs = []

    def sbatch(self, filename: str) -> str:
        cmd = self._executor.exec_command("sbatch " + filename)
        cmd.wait_until_exit()

        if cmd.exit_status != 0:
            raise SlurmError(cmd.stderr())

        out = cmd.stdout()
        job_id = out.split()[-1]
        self._active_jobs.append(job_id)
        return job_id

    def scancel(self, jobid: str) -> None:
        cmd = self._executor.exec_command("scancel " + jobid)
        cmd.wait_until_exit()
        if cmd.exit_status != 0:
            raise SlurmError(cmd.stderr())
        try:
            self._active_jobs.remove(jobid)
        except ValueError:
            pass

    def poll_status(self, jobid: str) -> SlurmJob:
        cmd = self._executor.exec_command(
            f"sacct -j {jobid} -o jobid,jobname%30,state --noheader")
        cmd.wait_until_exit()
        output = cmd.stdout()
        splitlines = output.splitlines()
        main_task = splitlines[0].split()
        return SlurmJob(id=main_task[0],
                        name=main_task[1],
                        state=main_task[2])

    @property
    def active_jobs(self) -> List[str]:
        return self._active_jobs
