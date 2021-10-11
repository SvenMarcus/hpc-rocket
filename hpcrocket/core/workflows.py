from abc import abstractmethod
from typing import Protocol

from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.core.slurmbatchjob import SlurmBatchJob


class Workflow(Protocol):

    @abstractmethod
    def run(self, executor: CommandExecutor) -> int:
        pass


class LaunchWorkflow(Workflow):

    def __init__(self, filesystem_factory: FilesystemFactory, options: LaunchOptions) -> None:
        self._filesystem_factory = filesystem_factory
        self._options = options
        self._job_status = None

    def run(self, executor: CommandExecutor) -> int:
        batch_job = SlurmBatchJob(executor, self._options.sbatch)
        batch_job.submit()
        batch_job.get_watcher().watch(self._callback, self._options.poll_interval)
        if self._job_status and self._job_status.success:
            return 0

        return 1

    def _callback(self, new_status):
        self._job_status = new_status
