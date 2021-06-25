import sys
import signal

from ssh_slurm_runner.application import Application
from ssh_slurm_runner.cli import parse_cli_args
from ssh_slurm_runner.ui import RichUI

cli_args = parse_cli_args(sys.argv[1:])
with RichUI() as ui:
    app = Application(cli_args, ui)

    def on_cancel(*args, **kwargs):
        sys.exit(app.cancel())

    signal.signal(signal.SIGINT, on_cancel)
    sys.exit(app.run())
