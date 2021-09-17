from dataclasses import dataclass, field
from typing import List

from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.ssh.sshexecutor import ConnectionData


@dataclass
class LaunchOptions:

    sbatch: str
    connection: ConnectionData
    proxyjumps: List[ConnectionData] = field(default_factory=lambda: [])
    copy_files: List[CopyInstruction] = field(default_factory=lambda: [])
    clean_files: List[str] = field(default_factory=lambda: [])
    collect_files: List[CopyInstruction] = field(default_factory=lambda: [])
    poll_interval: int = 5
