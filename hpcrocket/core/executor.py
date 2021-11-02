from abc import ABC, abstractmethod
from typing import List, Type


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

    def __exit__(self, exc_type: Type[Exception], exc_val: Exception, exc_tb: str) -> None:
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
