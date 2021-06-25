import os

from ssh_slurm_runner.launchoptions import LaunchOptions
from ssh_slurm_runner.sshexecutor import SSHExecutor
from ssh_slurm_runner.slurmrunner import SlurmError, SlurmJob, SlurmRunner
from ssh_slurm_runner.watcher.jobwatcher import JobWatcher
from ssh_slurm_runner.ui import UI


class Application:

    def __init__(self, options: LaunchOptions, ui: UI) -> None:
        self._options = options
        self._latest_job_update: SlurmJob = None
        self._ui = ui
        self.runner = None
        self.watcher = None
        self.jobid = 0

    def run(self) -> int:
        executor = self._create_sshexecutor()
        self.runner = SlurmRunner(executor)
        self.jobid = self.runner.sbatch(self._options.sbatch)
        self.watcher = JobWatcher(self.runner)

        self.watcher.watch(self.jobid,
                           self._poll_callback,
                           self._options.poll_interval)

        self.watcher.wait_until_done()

        executor.disconnect()
        if self._latest_job_update.success:
            return 0

        return 1

    def _poll_callback(self, job: SlurmJob) -> None:
        self._latest_job_update = job
        self._ui.update(job)

    def _create_sshexecutor(self) -> SSHExecutor:
        executor = SSHExecutor(self._options.host)
        executor.load_host_keys_from_file(
            f"{os.environ['HOME']}/.ssh/known_hosts")
        executor.connect(self._options.user,
                         self._options.private_keyfile,
                         self._options.password,
                         self._options.private_key)

        return executor

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
