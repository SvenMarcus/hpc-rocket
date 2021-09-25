from hpcrocket.core.environmentpreparation import EnvironmentPreparation
from hpcrocket.core.errors import get_error_message
from hpcrocket.core.executor import CommandExecutorFactory
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import LaunchOptions
from hpcrocket.core.slurmrunner import SlurmJob, SlurmRunner
from hpcrocket.ui import UI
from hpcrocket.watcher.jobwatcher import JobWatcher


class Application:

    def __init__(self, executor_factory: CommandExecutorFactory, filesystem_factory: FilesystemFactory, ui: UI) -> None:
        self._executor_factory = executor_factory
        self._fs_factory = filesystem_factory
        self._ui = ui
        self._latest_job_update: SlurmJob
        self._runner: SlurmRunner
        self._watcher: JobWatcher
        self._jobid: str

    def run(self, options: LaunchOptions) -> int:
        try:
            executor = self._executor_factory.create_executor()
            env_prep = self._create_env_preparation(options)
        except Exception as err:
            self._ui.error(get_error_message(err))
            return 1

        success = self._try_env_preparation(env_prep)
        if not success:
            return 1

        self._runner = SlurmRunner(executor)
        self._launch_job(self._runner, options)
        self._wait_for_job_completion(options)

        self._collect_results(env_prep)
        self._clean_remote_environment(env_prep)
        executor.close()

        return self._get_exit_code_for_job(self._latest_job_update)

    def _try_env_preparation(self, env_prep: EnvironmentPreparation) -> bool:
        try:
            self._ui.info("Preparing remote environment")
            env_prep.prepare()
            self._ui.success("Done")
        except (FileNotFoundError, FileExistsError) as err:
            self._ui.error(get_error_message(err))
            self._ui.info("Performing rollback")
            env_prep.rollback()
            self._ui.success("Done")
            return False

        return True

    def _launch_job(self, runner: SlurmRunner, options: LaunchOptions) -> None:
        self._ui.launch("Launching job")
        self._jobid = runner.sbatch(options.sbatch)
        self._ui.success(f"Job {self._jobid} launched")

    def _wait_for_job_completion(self, options: LaunchOptions) -> None:
        self._watcher = JobWatcher(self._runner)

        self._watcher.watch(self._jobid,
                            self._poll_callback,
                            options.poll_interval)

        self._watcher.wait_until_done()
        self._display_job_result()

    def _display_job_result(self):
        if self._latest_job_update.success:
            self._ui.success("Job completed successfully")
        else:
            self._ui.error("Job failed")

    def _clean_remote_environment(self, env_prep: EnvironmentPreparation):
        self._ui.info("Cleaning up remote environment")
        env_prep.clean()
        self._ui.success("Done")

    def _collect_results(self, env_prep: EnvironmentPreparation):
        self._ui.info("Collecting results")
        env_prep.collect()
        self._ui.success("Done")

    def _get_exit_code_for_job(self, job: SlurmJob) -> int:
        if job.success:
            return 0

        return 1

    def _create_env_preparation(self, options) -> EnvironmentPreparation:
        env_prep = EnvironmentPreparation(
            self._fs_factory.create_local_filesystem(),
            self._fs_factory.create_ssh_filesystem(),
            self._ui)

        env_prep.files_to_copy(options.copy_files)
        env_prep.files_to_clean(options.clean_files)
        env_prep.files_to_collect(options.collect_files)

        return env_prep

    def _poll_callback(self, job: SlurmJob) -> None:
        self._latest_job_update = job
        self._ui.update(job)

    def cancel(self) -> int:
        try:
            self._ui.info(f"Canceling job {self._jobid}")
            self._runner.scancel(self._jobid)
            self._watcher.stop()
            self._ui.error("Job canceled")
            job = self._runner.poll_status(self._jobid)
            self._ui.update(job)
        except Exception as err:
            self._ui.error("An error occured while canceling the job:")
            self._ui.error(f"\t{get_error_message(err)}")

        return 130
