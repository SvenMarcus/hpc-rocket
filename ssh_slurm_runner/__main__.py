import sys
import signal

from ssh_slurm_runner.application import Application
from ssh_slurm_runner.cli import parse_cli_args
from ssh_slurm_runner.ui import RichUI


def main():
    cli_args = parse_cli_args(sys.argv[1:])
    exit_code = 0
    with RichUI() as ui:
        app = Application(ui)

        def on_cancel(*args, **kwargs):
            sys.exit(app.cancel())

        signal.signal(signal.SIGINT, on_cancel)
        exit_code = app.run(cli_args)

    sys.exit(exit_code)

main()