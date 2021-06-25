from dataclasses import dataclass
from typing import Optional

@dataclass
class LaunchOptions:

    sbatch: str
    host: str
    user: str
    password: Optional[str] = None
    private_key: Optional[str] = None
    private_keyfile: Optional[str] = None
    poll_interval: Optional[int] = 5