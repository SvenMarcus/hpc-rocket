from hpcrocket.core.environmentpreparation import EnvironmentPreparation
from hpcrocket.core.errors import get_error_message
from hpcrocket.core.executor import CommandExecutor, CommandExecutorFactory
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.core.launchoptions import JobBasedOptions, LaunchOptions
from hpcrocket.core.slurmbatchjob import SlurmBatchJob, SlurmJobStatus
from hpcrocket.ui import UI
from hpcrocket.watcher.jobwatcher import JobWatcher


class Application:

    def __init__(self, executor_factory: CommandExecutorFactory, filesystem_factory: FilesystemFactory, ui: UI) -> None:
        self._executor_factory = executor_factory
        self._fs_factory = filesystem_factory
        self._ui = ui
        self._latest_job_update = SlurmJobStatus("", "", "", [])
        self._env_prep: EnvironmentPreparation
        self._batchjob: SlurmBatchJob
        self._watcher: JobWatcher
        self._jobid: str

    def run(self, options: LaunchOptions) -> int:
        exit_code = 0
        try:
            with self._executor_factory.create_executor() as executor:
                if isinstance(options, JobBasedOptions):
                    exit_code = self._run_status_workflow(executor, options)
                else:
                    exit_code = self._run_launch_workflow(executor, options)
        except Exception as err:
            self._ui.error(get_error_message(err))
            exit_code = 1

        return exit_code

    def _run_status_workflow(self, executor: CommandExecutor, options: JobBasedOptions) -> int:
        cmd = executor.exec_command(f"sacct -j {options.jobid}")
        return cmd.exit_status

    def _run_launch_workflow(self, executor: CommandExecutor, options: LaunchOptions) -> int:
        self._env_prep = self._create_env_preparation(options)
        exit_code = self._try_env_preparation()
        if exit_code != 0:
            return exit_code

        self._run_batchjob(options, executor)
        self._wait_for_job_if_watching(options)

        return self._get_exit_code_for_job()

    def _run_batchjob(self, options: LaunchOptions, executor: CommandExecutor) -> None:
        self._batchjob = SlurmBatchJob(executor, options.sbatch)
        self._launch_job(self._batchjob)

    def _try_env_preparation(self) -> int:
        try:
            self._ui.info("Preparing remote environment")
            self._env_prep.prepare()
            self._ui.success("Done")
        except (FileNotFoundError, FileExistsError) as err:
            self._perform_rollback_on_error(err)
            return 1

        return 0

    def _launch_job(self, runner: SlurmBatchJob) -> None:
        self._ui.launch("Launching job")
        self._jobid = runner.submit()
        self._ui.success(f"Job {self._jobid} launched")

    def _wait_for_job_if_watching(self, options: LaunchOptions) -> None:
        if not options.watch:
            return

        self._wait_for_job_completion(options.poll_interval)
        self._post_run_cleanup()

    def _wait_for_job_completion(self, poll_interval: int) -> None:
        self._watcher = self._batchjob.get_watcher()
        self._watcher.watch(self._poll_callback, poll_interval)
        self._watcher.wait_until_done()
        self._display_job_result()

    def _display_job_result(self) -> None:
        if self._latest_job_update.success:
            self._ui.success("Job completed successfully")
        else:
            self._ui.error("Job failed")

    def _perform_rollback_on_error(self, err: Exception) -> None:
        self._ui.error(get_error_message(err))
        self._ui.info("Performing rollback")
        self._env_prep.rollback()
        self._ui.success("Done")

    def _post_run_cleanup(self) -> None:
        self._collect_results(self._env_prep)
        self._clean_remote_environment(self._env_prep)

    def _collect_results(self, env_prep: EnvironmentPreparation) -> None:
        self._ui.info("Collecting results")
        env_prep.collect()
        self._ui.success("Done")

    def _clean_remote_environment(self, env_prep: EnvironmentPreparation) -> None:
        self._ui.info("Cleaning up remote environment")
        env_prep.clean()
        self._ui.success("Done")

    def _get_exit_code_for_job(self) -> int:
        status = self._latest_job_update
        if status and status.success:
            return 0

        return 1

    def _create_env_preparation(self, options: LaunchOptions) -> EnvironmentPreparation:
        env_prep = EnvironmentPreparation(
            self._fs_factory.create_local_filesystem(),
            self._fs_factory.create_ssh_filesystem(),
            self._ui)

        env_prep.files_to_copy(options.copy_files)
        env_prep.files_to_clean(options.clean_files)
        env_prep.files_to_collect(options.collect_files)

        return env_prep

    def _poll_callback(self, job: SlurmJobStatus) -> None:
        self._latest_job_update = job
        self._ui.update(job)

    def cancel(self) -> int:
        try:
            self._ui.info(f"Canceling job {self._jobid}")
            self._batchjob.cancel()
            self._watcher.stop()
            self._ui.error("Job canceled")
            job = self._batchjob.poll_status()
            self._ui.update(job)
        except Exception as err:
            self._ui.error("An error occured while canceling the job:")
            self._ui.error(f"\t{get_error_message(err)}")

        return 130
