import sys
import signal

from hpclaunch.application import Application
from hpclaunch.cli import parse_cli_args
from hpclaunch.ui import RichUI


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