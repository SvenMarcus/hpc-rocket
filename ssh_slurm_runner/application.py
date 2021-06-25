from ssh_slurm_runner.launchoptions import LaunchOptions
from ssh_slurm_runner.sshexecutor import SSHExecutor
from ssh_slurm_runner.slurmrunner import SlurmJob, SlurmRunner
from ssh_slurm_runner.watcher.jobwatcher import JobWatcher


class Application:

    def __init__(self, options: LaunchOptions) -> None:
        self._options = options
        self._latest_job_update: SlurmJob = None

    def run(self) -> int:
        executor = self._create_sshexecutor()
        runner = SlurmRunner(executor)
        jobid = runner.sbatch(self._options.sbatch)
        watcher = JobWatcher(runner)

        watcher.watch(jobid, self._poll_callback, self._options.poll_interval)
        watcher.wait_until_done()

        if self._latest_job_update.success:
            return 0

        return 1

    def _poll_callback(self, job: SlurmJob) -> None:
        self._latest_job_update = job

    def _create_sshexecutor(self) -> SSHExecutor:
        executor = SSHExecutor(self._options.host)
        executor.load_host_keys_from_file("~/.ssh/known_hosts")
        executor.connect(self._options.user,
                         self._options.private_keyfile,
                         self._options.password,
                         self._options.private_key)

        return executor
