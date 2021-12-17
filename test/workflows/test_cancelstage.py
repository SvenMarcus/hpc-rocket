from test.slurm_assertions import assert_job_canceled
from test.slurmoutput import DEFAULT_JOB_ID
from test.testdoubles.executor import SlurmJobExecutorSpy
from unittest.mock import Mock

from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.stages import CancelStage
from hpcrocket.ui import UI


def make_sut(executor):
    controller = SlurmController(executor)
    sut = CancelStage(controller, DEFAULT_JOB_ID)
    return sut


def test__when_running__should_cancel_job():
    executor = SlurmJobExecutorSpy()
    sut = make_sut(executor)

    actual = sut(Mock(spec=UI))

    assert_job_canceled(executor, DEFAULT_JOB_ID)
    assert actual is True
