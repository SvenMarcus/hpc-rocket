from abc import ABC, abstractmethod
from typing import List


class RunningCommand(ABC):

    @abstractmethod
    def wait_until_exit(self) -> int:
        pass

    @property
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

    def __enter__(self) -> 'CommandExecutor':
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    @abstractmethod
    def exec_command(self, cmd: str) -> RunningCommand:
        pass

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class CommandExecutorFactory(ABC):

    @abstractmethod
    def create_executor(self) -> CommandExecutor:
        pass
