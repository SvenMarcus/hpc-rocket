from typing import List, Optional

from hpcrocket.core.environmentpreparation import CopyInstruction, EnvironmentPreparation
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmJobStatus
from hpcrocket.core.slurmcontroller import SlurmController


class LaunchStage:

    def __init__(self, controller: SlurmController, options: LaunchOptions) -> None:
        self._controller = controller
        self._options = options
        self._job_status: Optional[SlurmJobStatus] = None

    def __call__(self) -> bool:
        batch_job = self._controller.submit(self._options.sbatch)
        if not self._options.watch:
            return True

        return self._wait_for_job_exit(batch_job)

    def _wait_for_job_exit(self, batch_job: SlurmBatchJob) -> bool:
        watcher = batch_job.get_watcher()
        watcher.watch(self._callback, self._options.poll_interval)
        # watcher.wait_until_done()

        if self._job_status and self._job_status.success:
            return True

        return False

    def _callback(self, new_status: SlurmJobStatus) -> None:
        self._job_status = new_status


class PrepareStage:

    def __init__(self, filesystem_factory: FilesystemFactory, copy_instructions: List[CopyInstruction]) -> None:
        self._env_prep = EnvironmentPreparation(
            filesystem_factory.create_local_filesystem(),
            filesystem_factory.create_ssh_filesystem()
        )
        self._files = copy_instructions

    def __call__(self) -> bool:
        self._env_prep.files_to_copy(self._files)

        try:
            self._env_prep.prepare()
        except (FileExistsError, FileNotFoundError):
            self._env_prep.rollback()
            return False

        return True
