import threading
from typing import Callable, Union
from ssh_slurm_runner.slurmrunner import SlurmJob, SlurmRunner


class WatchThread(threading.Thread):

    def __init__(self, runner: SlurmRunner, jobid: str, callback: Callable, interval: float):
        super(WatchThread, self).__init__(target=self.poll)
        self.runner = runner
        self.jobid = jobid
        self.callback = callback
        self.interval = interval
        self.stop_event = threading.Event()
        self.is_done_event = threading.Event()
        self.last_job: SlurmJob = None

    def poll(self):
        ticker = threading.Event()
        while not ticker.wait(self.interval) and not self.stop_event.is_set():
            job = self.runner.poll_status(self.jobid)
            if job != self.last_job:
                self.callback(job)

            self.last_job = job

            if job.is_completed:
                self.is_done_event.set()
                break

    def stop(self):
        self.stop_event.set()

    def is_done(self):
        return self.is_done_event.is_set()


class JobWatcher:

    def __init__(self, runner: SlurmRunner) -> None:
        self.runner = runner
        self.watching_thread: Union[WatchThread, None] = None

    def watch(self, jobid: str, callback: Callable, poll_interval: float) -> None:
        self.watching_thread = WatchThread(self.runner, jobid,
                                           callback, poll_interval)

        self.watching_thread.start()

    def is_done(self):
        if self.watching_thread is None:
            return True

        return self.watching_thread.is_done()

    def stop(self):
        if self.watching_thread is not None:
            self.watching_thread.stop()
            self.watching_thread.join()