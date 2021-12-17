from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Union

from hpcrocket.core.environmentpreparation import CopyInstruction
from hpcrocket.ssh.connectiondata import ConnectionData


Options = Union['LaunchOptions', 'SimpleJobOptions', 'WatchOptions']
JobBasedOptions = Union['SimpleJobOptions', 'WatchOptions']


@dataclass
class SimpleJobOptions:

    class Action(Enum):
        status = auto()
        cancel = auto()

    jobid: str
    action: Action
    connection: ConnectionData
    proxyjumps: List[ConnectionData] = field(default_factory=lambda: [])


@dataclass
class LaunchOptions:
    sbatch: str
    connection: ConnectionData
    proxyjumps: List[ConnectionData] = field(default_factory=lambda: [])
    copy_files: List[CopyInstruction] = field(default_factory=lambda: [])
    clean_files: List[str] = field(default_factory=lambda: [])
    collect_files: List[CopyInstruction] = field(default_factory=lambda: [])
    poll_interval: int = 5
    watch: bool = False


@dataclass
class WatchOptions:
    jobid: str
    connection: ConnectionData
    proxyjumps: List[ConnectionData] = field(default_factory=lambda: [])
    poll_interval: int = 5
