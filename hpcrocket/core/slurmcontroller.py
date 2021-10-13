from hpcrocket.core.executor import CommandExecutor, RunningCommand
from hpcrocket.core.slurmbatchjob import SlurmBatchJob


class SlurmController:

    def __init__(self, executor: CommandExecutor) -> None:
        self._executor = executor

    def submit(self, jobfile: str) -> SlurmBatchJob:
        cmd = self._executor.exec_command(f"sbatch {jobfile}")
        cmd.wait_until_exit()
        jobid = self._parse_jobid(cmd)
        return SlurmBatchJob(self._executor, jobfile, jobid)

    def _parse_jobid(self, cmd: RunningCommand) -> str:
        first_line = cmd.stdout()[0]
        split_line = first_line.split()
        jobid = split_line[-1]
        
        return jobid

    def poll_status(self, jobid: str) -> None:
        cmd = self._executor.exec_command(f"sacct -j {jobid}")