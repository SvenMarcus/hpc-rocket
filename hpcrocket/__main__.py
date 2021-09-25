import sys
import signal

from hpcrocket.core.application import Application
from hpcrocket.cli import parse_cli_args
from hpcrocket.pyfilesystem.factory import PyFilesystemFactory
from hpcrocket.ssh.sshexecutor import SSHExecutorFactory
from hpcrocket.ui import RichUI


def main():
    cli_args = parse_cli_args(sys.argv[1:])
    exit_code = 0
    with RichUI() as ui:
        executor_factory = SSHExecutorFactory(cli_args)
        filesystem_factory = PyFilesystemFactory(cli_args)
        app = Application(executor_factory, filesystem_factory, ui)

        def on_cancel(*args, **kwargs):
            sys.exit(app.cancel())

        signal.signal(signal.SIGINT, on_cancel)
        exit_code = app.run(cli_args)

    sys.exit(exit_code)


main()
