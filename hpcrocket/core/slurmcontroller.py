from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.slurmbatchjob import SlurmBatchJob


class SlurmController:

    def __init__(self, executor: CommandExecutor) -> None:
        self._executor = executor

    def submit(self, jobfile: str) -> SlurmBatchJob:
        self._executor.exec_command(f"sbatch {jobfile}")
        return SlurmBatchJob(self._executor, jobfile)
