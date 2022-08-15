from test.application import make_application
from test.application.launchoptions import cancel_options_with_proxy
from test.slurm_assertions import assert_job_canceled
from test.slurmoutput import DEFAULT_JOB_ID
from test.testdoubles.executor import SlurmJobExecutorSpy


def test__given_watch_options__when_running__should_poll_job_until_done() -> None:
    executor = SlurmJobExecutorSpy()
    sut = make_application(executor)

    sut.run(cancel_options_with_proxy())

    assert_job_canceled(executor, DEFAULT_JOB_ID)
