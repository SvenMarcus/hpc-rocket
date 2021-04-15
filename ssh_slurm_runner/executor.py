from abc import ABC, abstractmethod


class RunningCommand(ABC):

    @abstractmethod
    def wait_until_exit(self) -> int:
        pass

    @abstractmethod
    def exit_status(self) -> int:
        pass

    @abstractmethod
    def stdout(self) -> str:
        pass

    @abstractmethod
    def stderr(self) -> str:
        pass


class CommandExecutor(ABC):

    @abstractmethod
    def exec_command(self, cmd: str) -> RunningCommand:
        pass
