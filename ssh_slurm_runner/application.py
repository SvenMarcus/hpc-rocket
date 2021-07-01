import os

from ssh_slurm_runner.environmentpreparation import EnvironmentPreparation
from ssh_slurm_runner.filesystemimpl import LocalFilesystem, SSHFilesystem
from ssh_slurm_runner.launchoptions import LaunchOptions
from ssh_slurm_runner.slurmrunner import SlurmError, SlurmJob, SlurmRunner
from ssh_slurm_runner.sshexecutor import SSHExecutor
from ssh_slurm_runner.ui import UI
from ssh_slurm_runner.watcher.jobwatcher import JobWatcher


class Application:

    def __init__(self, options: LaunchOptions, ui: UI) -> None:
        self._options = options
        self._latest_job_update: SlurmJob = None
        self._ui = ui
        self.runner = None
        self.watcher = None
        self.jobid = 0

    def run(self) -> int:
        env_prep = self._prepare_remote_environment()
        executor = self._create_sshexecutor()
        self.runner = SlurmRunner(executor)
        self.jobid = self.runner.sbatch(self._options.sbatch)
        self._wait_for_job_completion()

        env_prep.clean()
        executor.disconnect()
        if self._latest_job_update.success:
            return 0

        return 1

    def _prepare_remote_environment(self) -> EnvironmentPreparation:
        env_prep = EnvironmentPreparation(
            LocalFilesystem("."),
            self._make_ssh_filesystem())

        env_prep.files_to_copy(self._options.copy_files)
        env_prep.files_to_clean(self._options.clean_files)
        env_prep.prepare()

        return env_prep

    def _make_ssh_filesystem(self):
        return SSHFilesystem(self._options.user,
                             self._options.host,
                             self._options.password,
                             self._options.private_key,
                             self._options.private_keyfile)

    def _wait_for_job_completion(self):
        self.watcher = JobWatcher(self.runner)

        self.watcher.watch(self.jobid,
                           self._poll_callback,
                           self._options.poll_interval)

        self.watcher.wait_until_done()

    def _poll_callback(self, job: SlurmJob) -> None:
        self._latest_job_update = job
        self._ui.update(job)

    def _create_sshexecutor(self) -> SSHExecutor:
        home_dir = os.environ['HOME']

        executor = SSHExecutor(self._options.host)
        executor.load_host_keys_from_file(f"{home_dir}/.ssh/known_hosts")

        keyfile = self._resolve_home_dir(home_dir)

        executor.connect(self._options.user,
                         keyfile,
                         self._options.password,
                         self._options.private_key)

        return executor

    def _resolve_home_dir(self, home_dir):
        keyfile = self._options.private_keyfile
        if keyfile.startswith("~/"):
            keyfile = keyfile.replace("~/", home_dir + "/", 1)

        return keyfile

    def cancel(self):
        try:
            print(f"Canceling job {self.jobid}")
            self.runner.scancel(self.jobid)
            self.watcher.stop()
            job = self.runner.poll_status(self.jobid)
            self._ui.update(job)
        except SlurmError as err:
            print(err.args)

        return 1
