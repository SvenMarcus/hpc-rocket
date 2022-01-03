from test.application.launchoptions import (launch_options,
                                            watch_options_with_proxy)
from test.testdoubles.executor import (InfiniteSlurmJobExecutor,
                                       LoggingCommandExecutorSpy)
from test.testdoubles.filesystem import DummyFilesystemFactory
from unittest.mock import Mock

import pytest
from hpcrocket.core.application import Application


@pytest.mark.timeout(2)
@pytest.mark.parametrize("app_options", (launch_options(watch=True), watch_options_with_proxy()))
def test__given_infinite_running_job__when_canceling__should_cancel_job_and_exit_with_code_130(app_options):
    executor = InfiniteSlurmJobExecutor()

    sut = Application(executor, DummyFilesystemFactory(), Mock())
    thread = run_in_background(sut, app_options)

    wait_until_polled(executor)

    actual = sut.cancel()

    thread.join()

    assert actual == 130


def run_in_background(sut, app_options):
    from threading import Thread

    thread = Thread(target=lambda: sut.run(app_options))
    thread.start()
    return thread


def wait_until_polled(executor: LoggingCommandExecutorSpy):
    def was_polled():
        polled = any(logged_command.cmd == "sacct"
                     for logged_command in executor.command_log)
        return polled

    import threading

    poll_event = threading.Event()
    while not poll_event.wait(.1):
        if was_polled():
            poll_event.set()
            break
