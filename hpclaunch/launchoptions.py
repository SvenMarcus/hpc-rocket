from dataclasses import dataclass, field
from typing import List, Optional

from hpclaunch.environmentpreparation import CopyInstruction


@dataclass
class LaunchOptions:

    sbatch: str
    host: str
    user: str
    password: Optional[str] = None
    private_key: Optional[str] = None
    private_keyfile: Optional[str] = None
    copy_files: List[CopyInstruction] = field(default_factory=lambda: [])
    clean_files: List[str] = field(default_factory=lambda: [])
    collect_files: List[CopyInstruction] = field(default_factory=lambda: [])
    poll_interval: int = 5
