from typing import List, Optional

from hpcrocket.core.environmentpreparation import (CopyInstruction,
                                                   EnvironmentPreparation)
from hpcrocket.core.errors import get_error_message
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmJobStatus
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.ui import UI
from hpcrocket.watcher.jobwatcher import JobWatcher, SlurmJobStatusCallback


class LaunchStage:

    def __init__(self, controller: SlurmController, options: LaunchOptions) -> None:
        self._controller = controller
        self._options = options
        self._batch_job: Optional[SlurmBatchJob] = None
        self._job_status: Optional[SlurmJobStatus] = None
        self._watcher: Optional[JobWatcher] = None

    def __call__(self, ui: UI) -> bool:
        self._batch_job = self._controller.submit(self._options.sbatch)
        if not self._options.watch:
            return True

        return self._wait_for_job_exit(self._batch_job, ui)

    def cancel(self, ui: UI):
        ui.info(f"Canceling job {self._batch_job.jobid}")
        self._batch_job.cancel()
        self._watcher.stop()
        ui.success(f"Canceled job {self._batch_job.jobid}")

    def _wait_for_job_exit(self, batch_job: SlurmBatchJob, ui: UI) -> bool:
        self._watcher = batch_job.get_watcher()
        self._watcher.watch(self._get_callback(ui), self._options.poll_interval)
        self._watcher.wait_until_done()

        return (self._job_status is not None
                and self._job_status.success)

    def _get_callback(self, ui: UI) -> SlurmJobStatusCallback:
        def callback(new_status: SlurmJobStatus):
            self._job_status = new_status
            ui.update(new_status)

        return callback


class PrepareStage:

    def __init__(self, filesystem_factory: FilesystemFactory, copy_instructions: List[CopyInstruction]) -> None:
        self._factory = filesystem_factory
        self._files = copy_instructions

    def __call__(self, ui: UI) -> bool:
        env_prep = self._create_env_prep(ui)
        return self._try_prepare(env_prep, ui)

    def cancel(self, ui: UI):
        pass

    def _try_prepare(self, env_prep: EnvironmentPreparation, ui: UI) -> bool:
        try:
            env_prep.prepare()
        except (FileExistsError, FileNotFoundError) as err:
            self._do_rollback(env_prep, err, ui)
            return False

        return True

    def _create_env_prep(self, ui):
        env_prep = EnvironmentPreparation(
            self._factory.create_local_filesystem(),
            self._factory.create_ssh_filesystem(),
            ui
        )

        env_prep.files_to_copy(self._files)
        return env_prep

    def _do_rollback(self, env_prep, err, ui):
        ui.error(get_error_message(err))
        ui.info("Performing rollback")
        env_prep.rollback()
        ui.success("Done")


class FinalizeStage:

    def __init__(
            self, filesystem_factory: FilesystemFactory, collect_instructions: List[CopyInstruction],
            clean_instructions: List[str]) -> None:
        self._factory = filesystem_factory
        self._collect = collect_instructions
        self._clean = clean_instructions

    def __call__(self, ui: UI) -> bool:
        env_prep = EnvironmentPreparation(
            self._factory.create_local_filesystem(),
            self._factory.create_ssh_filesystem(),
            ui
        )

        env_prep.files_to_collect(self._collect)
        env_prep.collect()

        env_prep.files_to_clean(self._clean)
        env_prep.clean()

        return True

    def cancel(self, ui: UI):
        pass


class StatusStage:

    def __init__(self, controller: SlurmController, jobid: str) -> None:
        self._controller = controller
        self._jobid = jobid

    def __call__(self, ui: UI) -> bool:
        ui.update(self._controller.poll_status(self._jobid))
        return True

    def cancel(self, ui: UI):
        pass