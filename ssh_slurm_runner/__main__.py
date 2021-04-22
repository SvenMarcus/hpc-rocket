import os
import signal
from ssh_slurm_runner.watcher.jobwatcher import JobWatcher
import sys

from rich.live import Live
from rich.spinner import Spinner

from ssh_slurm_runner.cli import parse_cli_args
from ssh_slurm_runner.output import make_table
from ssh_slurm_runner.slurmrunner import SlurmError, SlurmJob, SlurmRunner

from .sshexecutor import SSHExecutor

cli_args = parse_cli_args(sys.argv[1:])

client = SSHExecutor(cli_args.host)
client.load_host_keys_from_file(f"{os.environ['HOME']}/.ssh/known_hosts")
client.connect(cli_args.user, keyfile=cli_args.keyfile)

runner = SlurmRunner(client)
jobid = runner.sbatch(cli_args.jobfile)

watcher_ctx = {"job": None}
watcher = JobWatcher(runner)


def handle_sigint(live: Live):
    try:
        print(f"Canceling job {jobid}")
        runner.scancel(jobid)
        job = runner.poll_status(jobid)
        watcher.stop()
        live.update(make_table(job))
    except SlurmError as err:
        print(err.args)
    sys.exit(1)


with Live(Spinner("bouncingBar", "Launching job"), refresh_per_second=8) as live:
    signal.signal(signal.SIGINT, lambda _, __: handle_sigint(live))

    def watcher_callback(job: SlurmJob):
        watcher_ctx["job"] = job
        live.update(make_table(job))

    watcher.watch(jobid, watcher_callback, poll_interval=5)

    while not watcher.is_done():
        continue

    watcher.stop()
    client.disconnect()

    job = watcher_ctx["job"]
    if job is None or not job.success:
        sys.exit(1)
