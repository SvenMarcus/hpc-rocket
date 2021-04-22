from typing import Callable

import ssh_slurm_runner.watcher.watcherthread as wt

from ssh_slurm_runner.slurmrunner import SlurmRunner


class NotWatchingError(RuntimeError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class JobWatcher:

    def __init__(self, runner: SlurmRunner) -> None:
        self.runner = runner
        self.watching_thread = None

    def watch(self, jobid: str, callback: Callable, poll_interval: float) -> None:
        self.watching_thread = wt.make_watcherthread(
            self.runner, jobid, callback, poll_interval)

        self.watching_thread.start()

    def is_done(self) -> bool:
        if self.watching_thread is None:
            raise NotWatchingError()

        return self.watching_thread.is_done()

    def wait_until_done(self) -> None:
        if self.watching_thread is None:
            raise NotWatchingError()

        self.watching_thread.join()

    def stop(self) -> None:
        if self.watching_thread is None:
            raise NotWatchingError()

        self.watching_thread.stop()
        self.watching_thread.join()
