from abc import ABC, abstractmethod
from typing import List


class Filesystem(ABC):

    @abstractmethod
    def copy(self, source: str, target: str, filesystem: 'Filesystem' = None) -> None:
        pass

    @abstractmethod
    def delete(self, path: str) -> None:
        pass
