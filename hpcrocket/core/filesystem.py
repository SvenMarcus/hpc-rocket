from abc import ABC, abstractmethod
from typing import Optional


class FilesystemFactory(ABC):

    @abstractmethod
    def create_local_filesystem(self) -> 'Filesystem':
        pass

    @abstractmethod
    def create_ssh_filesystem(self) -> 'Filesystem':
        pass


class Filesystem(ABC):
    """
    Abstract base class for all Filesystems
    """

    @abstractmethod
    def copy(self, source: str, target: str,
             overwrite: bool = False,
             filesystem: Optional['Filesystem'] = None) -> None:
        """Copies the `source` file to the `target` location.
        Can transfer between filesystems if `filesystem` argument is specified.

        Args:
            source (str): The path to the file to be copied
            target (str): The path to the copy destination
            filesystem (Filesystem): An optional different filesystem to copy to

        Raises:
            FileNotFoundError: The `source` file does not exist
            FileExistsError: The `target` file already exists and overwrite is False
        """
        pass

    @abstractmethod
    def delete(self, path: str) -> None:
        """Deletes a file from the Filesystem

        Args:
            path (str): The path to the file to be deleted

        Raises:
            FileNotFoundError: The file does not exist
        """
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Checks if a file exists on the Filesystem

        Args:
            path (str): The path to a file

        Returns:
            bool: True if the file exists
        """
        pass
