import os
import signal
import sys
import threading

from rich.console import RenderGroup
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


def handle_sigint(live: Live):
    try:
        print(f"Canceling job {jobid}")
        runner.scancel(jobid)
        job = runner.poll_status(jobid)
        live.update(make_table(job))
    except SlurmError as err:
        print(err.args)


with Live(Spinner("bouncingBar", "Launching job"), refresh_per_second=8) as live:
    signal.signal(signal.SIGINT, lambda _, __: handle_sigint(live))

    job: SlurmJob
    ticker = threading.Event()
    while not ticker.wait(5):
        job = runner.poll_status(jobid)
        live.update(make_table(job))

        if job.is_completed:
            break

    client.disconnect()
    live.update(RenderGroup(
        make_table(job),
        f"Job {'Completed' if job.success else 'Failed'}"
    ))

    if job is None or not job.success:
        sys.exit(1)
