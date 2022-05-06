import signal
import sys
from typing import Any, List
from hpcrocket.cli import parse_cli_args
from hpcrocket.core.application import Application
from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.filesystem import Filesystem, FilesystemFactory
from hpcrocket.core.launchoptions import Options
from hpcrocket.pyfilesystem.factory import PyFilesystemFactory
from hpcrocket.pyfilesystem.localfilesystem import LocalFilesystem
from hpcrocket.ssh.sshexecutor import SSHExecutor
from hpcrocket.ui import UI, RichUI

try:
    from typing import Protocol
except ImportError:  # pragma: no cover
    from typing_extensions import Protocol  # type: ignore


class ServiceRegistry(Protocol):
    """
    A container for dependencies to be used in HPC Rocket
    """

    def local_filesystem(self) -> Filesystem:
        """
        Returns the local filesystem
        """
        ...

    def get_executor(self, options: Options) -> CommandExecutor:
        """
        Returns the CommandExecutor to be used with the HPC Rocket application
        """
        ...

    def get_filesystem_factory(self, options: Options) -> FilesystemFactory:
        """
        Returns the FilesystemFactory to be used with the HPC Rocket application
        """
        ...


class ProductionServiceRegistry:
    """
    The default implementation for the ServiceRegistry protocol
    """

    def local_filesystem(self) -> Filesystem:
        return LocalFilesystem(".")

    def get_executor(self, options: Options) -> CommandExecutor:
        return SSHExecutor(options.connection, options.proxyjumps)

    def get_filesystem_factory(self, options: Options) -> FilesystemFactory:
        return PyFilesystemFactory(options)


def create_application(
    options: Options, service_registry: ServiceRegistry, ui: UI
) -> Application:
    executor = service_registry.get_executor(options)
    filesystem_factory = service_registry.get_filesystem_factory(options)
    return Application(executor, filesystem_factory, ui)


class RuntimeContainer:
    """
    A container to run and cancel the HPC Rocket application. Created to decouple running/canceling from sys.exit commands
    """

    def __init__(
        self, args: List[str], service_registry: ServiceRegistry, ui: UI
    ) -> None:
        self.options = parse_cli_args(args[1:], service_registry.local_filesystem())
        self.app = create_application(self.options, service_registry, ui)

    def run(self) -> int:
        return self.app.run(self.options)

    def cancel(self) -> int:
        return self.app.cancel()


def main(args: List[str], service_registry: ServiceRegistry) -> None:
    with RichUI() as ui:
        runtime = RuntimeContainer(args, service_registry, ui)

        def on_cancel(*args: Any, **kwargs: Any) -> None:
            sys.exit(runtime.cancel())

        signal.signal(signal.SIGINT, on_cancel)
        sys.exit(runtime.run())
