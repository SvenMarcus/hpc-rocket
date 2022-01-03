from typing import List, Optional, cast

from hpcrocket.core.environmentpreparation import (CopyInstruction,
                                                   EnvironmentPreparation)
from hpcrocket.core.errors import get_error_message
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmJobStatus
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.typesafety import get_or_raise
from hpcrocket.ui import UI
from hpcrocket.watcher.jobwatcher import (JobWatcher, NotWatchingError,
                                          SlurmJobStatusCallback)

try:
    from typing import Protocol
except ImportError:  # pragma: no cover
    from typing_extensions import Protocol  # type: ignore


class NoJobLaunchedError(Exception):
    pass


class LaunchStage:
    """
    Launches a batch job.
    Implements the BatchJobProvider protocol to work with WatchStage.
    """

    def __init__(self, controller: SlurmController, batch_script: str) -> None:
        self._controller = controller
        self._batch_script = batch_script
        self._batch_job: Optional[SlurmBatchJob] = None

    def __call__(self, ui: UI) -> bool:
        self._batch_job = self._controller.submit(self._batch_script)
        ui.launch(f"Launched job {self._batch_job.jobid}")

        return True

    def cancel(self, ui: UI) -> None:
        batch_job = get_or_raise(self._batch_job, self._no_job_launched())

        ui.info(f"Canceling job {batch_job.jobid}")
        batch_job.cancel()
        ui.success(f"Canceled job {batch_job.jobid}")

    def _no_job_launched(self) -> NoJobLaunchedError:
        return NoJobLaunchedError("Canceled before a job was started")

    def get_batch_job(self) -> SlurmBatchJob:
        return cast(SlurmBatchJob, self._batch_job)


class WatchStage:
    """
    Watches a batch job until it completes
    """

    class BatchJobProvider(Protocol):

        def get_batch_job(self) -> SlurmBatchJob:
            """
            Provides the watch stage with a batch job to watch

            Returns:
                SlurmBatchJob
            """
            pass

        def cancel(self, ui: UI) -> None:
            """
            Informs the BatchJobProvider that the WatchStage was canceled

            Args:
                ui (UI): The UI instance WatchStage was called with
            """
            pass

    def __init__(self, batch_job_provider: BatchJobProvider, poll_interval: int) -> None:
        self._poll_interval = poll_interval
        self._provider = batch_job_provider
        self._watcher: Optional[JobWatcher] = None
        self._job_status: Optional[SlurmJobStatus] = None

    def __call__(self, ui: UI) -> bool:
        batch_job = self._provider.get_batch_job()
        self._watcher = batch_job.get_watcher()
        self._watcher.watch(self._get_callback(ui), self._poll_interval)
        self._watcher.wait_until_done()

        return (self._job_status is not None
                and self._job_status.success)

    def _get_callback(self, ui: UI) -> SlurmJobStatusCallback:
        def callback(new_status: SlurmJobStatus) -> None:
            self._job_status = new_status
            ui.update(new_status)

        return callback

    def cancel(self, ui: UI) -> None:
        get_or_raise(self._watcher, NotWatchingError).stop()
        self._provider.cancel(ui)


class PrepareStage:
    """
    Copies the given files to the target filesystem.
    """

    def __init__(self, filesystem_factory: FilesystemFactory, copy_instructions: List[CopyInstruction]) -> None:
        self._factory = filesystem_factory
        self._files = copy_instructions

    def __call__(self, ui: UI) -> bool:
        env_prep = self._create_env_prep(ui)
        return self._try_prepare(env_prep, ui)

    def cancel(self, ui: UI) -> None:
        pass

    def _try_prepare(self, env_prep: EnvironmentPreparation, ui: UI) -> bool:
        try:
            ui.info("Copying files...")
            env_prep.prepare()
            ui.success("Done")
        except (FileExistsError, FileNotFoundError) as err:
            self._do_rollback(env_prep, err, ui)
            return False

        return True

    def _create_env_prep(self, ui: UI) -> EnvironmentPreparation:
        env_prep = EnvironmentPreparation(
            self._factory.create_local_filesystem(),
            self._factory.create_ssh_filesystem(),
            ui
        )

        env_prep.files_to_copy(self._files)
        return env_prep

    @staticmethod
    def _do_rollback(env_prep: EnvironmentPreparation,
                     err: Exception, ui: UI) -> None:
        ui.error(get_error_message(err))
        ui.info("Performing rollback")
        env_prep.rollback()
        ui.success("Done")


class FinalizeStage:
    """
    Collects result files from the remote filesystem and cleans it according to the given instructions.
    """

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

        self._collect_files(env_prep, ui)
        self._clean_files(env_prep, ui)

        return True

    def _collect_files(self, env_prep: EnvironmentPreparation, ui: UI) -> None:
        env_prep.files_to_collect(self._collect)
        ui.info("Collecting files...")
        env_prep.collect()
        ui.success("Done")

    def _clean_files(self, env_prep: EnvironmentPreparation, ui: UI) -> None:
        env_prep.files_to_clean(self._clean)
        ui.info("Cleaning files...")
        env_prep.clean()
        ui.success("Done")

    def cancel(self, ui: UI) -> None:
        pass


class StatusStage:
    """
    Checks a job's status.
    """

    def __init__(self, controller: SlurmController, jobid: str) -> None:
        self._controller = controller
        self._jobid = jobid

    def __call__(self, ui: UI) -> bool:
        ui.update(self._controller.poll_status(self._jobid))
        return True

    def cancel(self, ui: UI) -> None:
        pass


class CancelStage:
    """
    Cancels a running job
    """

    def __init__(self, controller: SlurmController, jobid: str):
        self._controller = controller
        self._jobid = jobid

    def __call__(self, ui: UI) -> bool:
        self._controller.cancel(self._jobid)
        return True

    def cancel(self, ui: UI) -> None:
        pass
