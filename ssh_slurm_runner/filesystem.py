from abc import ABC,abstractmethod

class Filesystem(ABC):

    @abstractmethod
    def copy(source: str, target: str) -> None:
        pass

    @abstractmethod
    def delete(path: str) -> None:
        pass