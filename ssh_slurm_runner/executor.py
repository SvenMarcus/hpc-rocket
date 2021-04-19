from abc import ABC, abstractmethod
from typing import List


class RunningCommand(ABC):

    @abstractmethod
    def wait_until_exit(self) -> int:
        pass

    @abstractmethod
    def exit_status(self) -> int:
        pass

    @abstractmethod
    def stdout(self) -> List[str]:
        pass

    @abstractmethod
    def stderr(self) -> List[str]:
        pass


class CommandExecutor(ABC):

    @abstractmethod
    def exec_command(self, cmd: str) -> RunningCommand:
        pass
