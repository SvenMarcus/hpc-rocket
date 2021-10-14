from typing import Tuple
from hpcrocket.core.executor import CommandExecutor, RunningCommand
from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmError, SlurmJobStatus


class SlurmController:

    def __init__(self, executor: CommandExecutor) -> None:
        self._executor = executor

    def submit(self, jobfile: str) -> SlurmBatchJob:
        cmd = self._execute_and_wait_or_raise_on_error(f"sbatch {jobfile}")
        jobid = self._parse_jobid(cmd)

        return SlurmBatchJob(self._executor, jobfile, jobid)

    def poll_status(self, jobid: str) -> SlurmJobStatus:
        cmd = self._execute_and_wait_or_raise_on_error(f"sacct -j {jobid} -o jobid,jobname%30,state --noheader")
        return SlurmJobStatus.from_output(cmd.stdout())

    def cancel(self, jobid: str) -> None:
        cmd = self._execute_and_wait_or_raise_on_error(f"scancel {jobid}")

    def _execute_and_wait_or_raise_on_error(self, command: str) -> RunningCommand:
        cmd = self._executor.exec_command(command)
        exit_code = cmd.wait_until_exit()
        if exit_code != 0:
            raise SlurmError()

        return cmd
    
    def _parse_jobid(self, cmd: RunningCommand) -> str:
        first_line = cmd.stdout()[0]
        split_line = first_line.split()
        jobid = split_line[-1]
        
        return jobid