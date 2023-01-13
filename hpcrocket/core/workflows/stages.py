from typing import List, Optional, Tuple, cast

from hpcrocket.core.filesystem.progressive import (
    CopyInstruction,
    progressive_copy,
    progressive_clean,
)
from hpcrocket.core.errors import get_error_message
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmJobStatus
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.typesafety import get_or_raise
from hpcrocket.ui import UI
from hpcrocket.watcher.jobwatcher import (
    JobWatcher,
    NotWatchingError,
    SlurmJobStatusCallback,
)

try:
    from typing import Protocol
except ImportError:  # pragma: no cover
    from typing_extensions import Protocol  # type: ignore


class NoJobLaunchedError(Exception):
    pass


def _log_errors(errors: List[Exception], ui: UI) -> None:
    for error in errors:
        ui.error(get_error_message(error))


class LaunchStage:
    """
    Launches a batch job.
    Implements the BatchJobProvider protocol to work with WatchStage.
    """

    def __init__(self, controller: SlurmController, batch_script: str) -> None:
        self._controller = controller
        self._batch_script = batch_script
        self._batch_job: Optional[SlurmBatchJob] = None

    def allowed_to_fail(self) -> bool:
        return False

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
            ...

        def cancel(self, ui: UI) -> None:
            """
            Informs the BatchJobProvider that the WatchStage was canceled

            Args:
                ui (UI): The UI instance WatchStage was called with
            """
            ...

    def __init__(
        self,
        batch_job_provider: BatchJobProvider,
        poll_interval: int,
        allowed_to_fail: bool = False,
    ) -> None:
        self._poll_interval = poll_interval
        self._provider = batch_job_provider
        self._watcher: Optional[JobWatcher] = None
        self._job_status: Optional[SlurmJobStatus] = None

        self._allowed_to_fail = allowed_to_fail

    def allowed_to_fail(self) -> bool:
        return self._allowed_to_fail

    def __call__(self, ui: UI) -> bool:
        batch_job = self._provider.get_batch_job()
        self._watcher = batch_job.get_watcher()
        self._watcher.watch(self._get_callback(ui), self._poll_interval)
        self._watcher.wait_until_done()

        return self._job_status is not None and self._job_status.success

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

    def __init__(
        self,
        filesystem_factory: FilesystemFactory,
        copy_instructions: List[CopyInstruction],
    ) -> None:
        self._local_fs = filesystem_factory.create_local_filesystem()
        self._remote_fs = filesystem_factory.create_ssh_filesystem()
        self._files = copy_instructions

    def allowed_to_fail(self) -> bool:
        return False

    def __call__(self, ui: UI) -> bool:
        ui.info("Copying files...")
        copied_files, errors = self._try_copy_files()

        if errors:
            _log_errors(errors, ui)
            self._do_rollback(copied_files, ui)
            return False

        ui.success("Done")
        return True

    def cancel(self, ui: UI) -> None:
        pass

    def _try_copy_files(self) -> Tuple[List[str], List[Exception]]:
        copied_files: List[str] = []
        errors: List[Exception] = []
        for cr in progressive_copy(self._local_fs, self._remote_fs, self._files):
            copied_files.extend(cr.copied_files)
            if cr.errors:
                errors.extend(cr.errors)
                break

        return copied_files, errors

    def _do_rollback(self, files: List[str], ui: UI) -> None:
        ui.info("Performing rollback")
        errors = list(progressive_clean(self._remote_fs, files))
        _log_errors(errors, ui)
        ui.success("Done")


class FinalizeStage:
    """
    Collects result files from the remote filesystem and cleans it according to the given instructions.
    """

    def __init__(
        self,
        filesystem_factory: FilesystemFactory,
        collect_instructions: List[CopyInstruction],
        clean_instructions: List[str],
    ) -> None:
        self._local_fs = filesystem_factory.create_local_filesystem()
        self._remote_fs = filesystem_factory.create_ssh_filesystem()
        self._files = collect_instructions
        self._clean = clean_instructions

    def allowed_to_fail(self) -> bool:
        return False

    def __call__(self, ui: UI) -> bool:
        self._collect_files(ui)
        self._clean_files(ui)

        return True

    def _collect_files(self, ui: UI) -> None:
        ui.info("Collecting files...")
        for cr in progressive_copy(
            self._remote_fs, self._local_fs, self._files, abort_on_error=False
        ):
            _log_errors(cr.errors, ui)

        ui.success("Done")

    def _clean_files(self, ui: UI) -> None:
        ui.info("Cleaning files...")
        errors = list(progressive_clean(self._remote_fs, self._clean))
        _log_errors(errors, ui)
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

    def allowed_to_fail(self) -> bool:
        return False

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

    def allowed_to_fail(self) -> bool:
        return False

    def __call__(self, ui: UI) -> bool:
        self._controller.cancel(self._jobid)
        return True

    def cancel(self, ui: UI) -> None:
        pass
