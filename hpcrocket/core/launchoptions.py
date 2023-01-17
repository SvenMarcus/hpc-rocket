from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Union

from hpcrocket.core.filesystem.progressive import CopyInstruction
from hpcrocket.ssh.connectiondata import ConnectionData


Options = Union["LaunchOptions", "ImmediateCommandOptions", "WatchOptions", "FinalizeOptions"]
JobBasedOptions = Union["ImmediateCommandOptions", "WatchOptions"]


@dataclass
class ImmediateCommandOptions:
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
    continue_if_job_fails: bool = False


@dataclass
class WatchOptions:
    jobid: str
    connection: ConnectionData
    proxyjumps: List[ConnectionData] = field(default_factory=lambda: [])
    poll_interval: int = 5


@dataclass
class FinalizeOptions:
    connection: ConnectionData
    proxyjumps: List[ConnectionData] = field(default_factory=lambda: [])
    collect_files: List[CopyInstruction] = field(default_factory=lambda: [])
    clean_files: List[str] = field(default_factory=lambda: [])
