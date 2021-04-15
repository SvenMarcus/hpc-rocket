import os
from ssh_slurm_runner.slurmrunner import SlurmRunner
from .sshclient import SSHClient

client = SSHClient("phoenix.hlr.rz.tu-bs.de")
client.connect("y0054816", os.environ["HOME"] + "/.ssh/y0054816_phoenix_ed25519", os.environ["HOME"] + "/.ssh/known_hosts")
runner = SlurmRunner(client)
print(runner.poll_status("1603353"))

client.disconnect()