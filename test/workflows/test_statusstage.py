from test.slurmoutput import DEFAULT_JOB_ID, completed_slurm_job
from test.testdoubles.executor import (SlurmJobExecutorSpy,
                                       SuccessfulSlurmJobCommandStub)
from unittest.mock import Mock

from hpcrocket.core.slurmcontroller import SlurmController
from hpcrocket.core.workflows.stages import StatusStage
from hpcrocket.ui import UI


def make_sut():
    executor = SlurmJobExecutorSpy(sacct_cmd=SuccessfulSlurmJobCommandStub())
    controller = SlurmController(executor)
    return StatusStage(controller, DEFAULT_JOB_ID)


def test__given_successful_job__when_running__should_update_ui_with_job():
    sut = make_sut()

    ui_mock = Mock(spec=UI)
    sut(ui_mock)

    ui_mock.update.assert_called_with(completed_slurm_job())


def test__when_running__should_return_true():
    sut = make_sut()

    actual = sut(Mock())

    assert actual is True
