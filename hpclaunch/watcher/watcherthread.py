import threading

from typing import Callable

from hpclaunch.slurmrunner import SlurmRunner


def make_watcherthread(runner: SlurmRunner, jobid: str, callback: Callable, interval: float):
    return WatcherThread(runner, jobid, callback, interval)


class WatcherThread(threading.Thread):

    def __init__(self, runner: SlurmRunner, jobid: str, callback: Callable, interval: float):
        super(WatcherThread, self).__init__(target=self.poll)
        self.runner = runner
        self.jobid = jobid
        self.callback = callback
        self.interval = interval
        self.stop_event = threading.Event()
        self._done = False

    def poll(self):
        last_job = None
        while not self.stop_event.wait(self.interval):
            job = self.runner.poll_status(self.jobid)
            self._done = not (job.is_running or job.is_pending)

            if job != last_job:
                self.callback(job)
                last_job = job

            if self._done:
                break

    def stop(self):
        self.stop_event.set()

    def is_done(self):
        return self._done
