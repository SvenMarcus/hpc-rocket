import fs.copy as fscp
import fs.sshfs as sshfs
from ssh_slurm_runner.filesystem import Filesystem


class SSHFilesystem(Filesystem):
    
    def __init__(self, user: str, host: str, private_key: str) -> None:
        self.fs = sshfs.SSHFS(host, user=user, pkey=private_key)

    def copy(self, source: str, target: str, filesystem: 'Filesystem' = None) -> None:
        if filesystem:
            fscp.copy_file(self.fs, source, filesystem.fs, target)
            return

        self.fs.copy(source, target)
        

    def delete(self, path: str) -> None:
        pass
