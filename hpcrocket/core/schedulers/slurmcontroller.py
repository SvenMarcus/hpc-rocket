from typing import Optional

from hpcrocket.core.executor import CommandExecutor, RunningCommand
from hpcrocket.core.schedulers.base import BatchJob, JobStatus
from hpcrocket.core.schedulers.slurmstatus import SlurmError, SlurmJobStatus
from hpcrocket.watcher.jobwatcher import JobWatcherFactory, JobWatcherImpl


class SlurmController:
    def __init__(
        self,
        executor: CommandExecutor,
        watcher_factory: Optional[JobWatcherFactory] = None,
    ) -> None:
        self._executor = executor
        self._watcher_factory = watcher_factory or JobWatcherImpl

    def submit(self, jobfile: str) -> BatchJob:
        cmd = self._execute_and_wait_or_raise_on_error(f"sbatch {jobfile}")
        jobid = _parse_jobid(cmd)

        return BatchJob(self, jobid, self._watcher_factory)

    def poll_status(self, jobid: str) -> JobStatus:
        cmd = self._execute_and_wait_or_raise_on_error(
            f"sacct -j {jobid} -o jobid,jobname%30,state --noheader"
        )
        return SlurmJobStatus.from_output(cmd.stdout())

    def cancel(self, jobid: str) -> None:
        self._execute_and_wait_or_raise_on_error(f"scancel {jobid}")

    def _execute_and_wait_or_raise_on_error(self, command: str) -> RunningCommand:
        cmd = self._executor.exec_command(command)
        exit_code = cmd.wait_until_exit()
        if exit_code != 0:
            raise SlurmError(command)

        return cmd


def _parse_jobid(cmd: RunningCommand) -> str:
    first_line = cmd.stdout()[0]
    split_line = first_line.split()
    jobid = split_line[-1]

    return jobid
