from typing import Union

import hpcrocket.core.workflows as workflows
from hpcrocket.core.environmentpreparation import EnvironmentPreparation
from hpcrocket.core.errors import get_error_message
from hpcrocket.core.executor import CommandExecutorFactory
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import JobBasedOptions, LaunchOptions
from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmJobStatus
from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.ui import UI
from hpcrocket.watcher.jobwatcher import JobWatcher


class Application:

    def __init__(self, executor_factory: CommandExecutorFactory, filesystem_factory: FilesystemFactory, ui: UI) -> None:
        self._executor_factory = executor_factory
        self._fs_factory = filesystem_factory
        self._ui = ui
        self._latest_job_update = SlurmJobStatus.empty()
        self._env_prep: EnvironmentPreparation
        self._batchjob: SlurmBatchJob
        self._watcher: JobWatcher
        self._jobid: str

    def run(self, options: Union[LaunchOptions, JobBasedOptions]) -> int:
        exit_code = 0
        try:
            with self._executor_factory.create_executor() as executor:
                controller = SlurmController(executor)
                if isinstance(options, JobBasedOptions):
                    exit_code = self._run_status_workflow(controller, options)
                else:
                    exit_code = self._run_launch_workflow(controller, options)

        except Exception as err:
            self._ui.error(get_error_message(err))
            exit_code = 1

        return exit_code

    def _run_status_workflow(self, controller: SlurmController, options: JobBasedOptions) -> int:
        job_status = controller.poll_status(options.jobid)
        self._ui.update(job_status)

        return 0

    def _run_launch_workflow(self, controller: SlurmController, options: LaunchOptions) -> int:
        workflow = workflows.launchworkflow(self._fs_factory, controller, options)
        success = workflow.run(self._ui)

        return 0 if success else 1

    def cancel(self) -> int:
        try:
            self._ui.info(f"Canceling job {self._batchjob.jobid}")
            self._batchjob.cancel()
            self._watcher.stop()
            self._ui.error("Job canceled")
            job = self._batchjob.poll_status()
            self._ui.update(job)
        except Exception as err:
            self._ui.error("An error occured while canceling the job:")
            self._ui.error(f"\t{get_error_message(err)}")

        return 130
