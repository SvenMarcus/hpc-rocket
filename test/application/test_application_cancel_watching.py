from test.application.launchoptions import launch_options, watch_options_with_proxy
from test.testdoubles.executor import (
    InfiniteSlurmJobExecutor,
    LoggingCommandExecutorSpy,
)
from threading import Event, Thread

import pytest
from hpcrocket.core.application import Application
from hpcrocket.core.launchoptions import Options

from . import make_application


@pytest.mark.timeout(2)
@pytest.mark.parametrize(
    "app_options", (launch_options(watch=True), watch_options_with_proxy())
)
def test__given_infinite_running_job__when_canceling__should_cancel_job_and_exit_with_code_130(
    app_options: Options,
) -> None:
    executor = InfiniteSlurmJobExecutor()

    sut = make_application(executor)
    thread = run_in_background(sut, app_options)

    wait_until_polled(executor)

    actual = sut.cancel()

    thread.join()

    assert actual == 130


def run_in_background(sut: Application, app_options: Options) -> Thread:
    thread = Thread(target=lambda: sut.run(app_options))
    thread.start()
    return thread


def wait_until_polled(executor: LoggingCommandExecutorSpy) -> None:
    def was_polled() -> bool:
        polled = any(
            logged_command.cmd == "sacct" for logged_command in executor.command_log
        )
        return polled

    poll_event = Event()
    while not poll_event.wait(0.1):
        if was_polled():
            poll_event.set()
            break
