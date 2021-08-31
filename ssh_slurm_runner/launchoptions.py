from dataclasses import dataclass, field
from typing import List, Optional

from ssh_slurm_runner.environmentpreparation import CopyInstruction


@dataclass
class LaunchOptions:

    sbatch: str
    host: str
    user: str
    password: Optional[str] = None
    private_key: Optional[str] = None
    private_keyfile: Optional[str] = None
    poll_interval: Optional[int] = 5
    copy_files: List[CopyInstruction] = field(default_factory=lambda: [])
    clean_files: List[str] = field(default_factory=lambda: [])
    collect_files: List[CopyInstruction] = field(default_factory=lambda: [])
