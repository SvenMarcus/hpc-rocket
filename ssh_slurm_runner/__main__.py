import os
from .sshclient import SSHClient

client = SSHClient("phoenix.hlr.rz.tu-bs.de")
client.connect("y0054816", os.environ["HOME"] + "/.ssh/y0054816_phoenix_ed25519", os.environ["HOME"] + "/.ssh/known_hosts")
cmd = client.exec_command("cat asd.txt")
exitcode = cmd.wait_until_exit()

if exitcode == 0:
    print(cmd.stdout())
else:
    print(cmd.stderr())

client.disconnect()