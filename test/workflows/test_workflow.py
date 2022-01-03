from typing import List
from unittest.mock import Mock

import pytest
from hpcrocket.core.workflows.workflow import (Stage, Workflow,
                                               WorkflowNotStartedError)
from hpcrocket.ui import UI


class StageSpy:

    def __init__(self) -> None:
        self.was_run = False
        self.was_canceled = False
        self.received_ui: UI = None  # type: ignore[assignment]
        self.run_callback = lambda: None

    def __call__(self, ui: UI) -> bool:
        self.was_run = True
        self.received_ui = ui
        self.run_callback()
        return True

    def cancel(self, ui: UI) -> None:
        self.was_canceled = True

    def __bool__(self):
        return self.was_run


def failing_stage():
    def _stage(ui: UI):
        return False

    return _stage


def ui_dummy():
    return Mock(spec=UI)


def make_sut(stages: List[Stage]):
    return Workflow(stages)


def test__given_workflow__when_running__should_return_true():
    sut = make_sut([])

    actual = sut.run(ui_dummy())

    assert actual is True


def test__given_workflow_with_stage__when_running__should_call_stage_with_ui():
    stage = StageSpy()
    sut = make_sut([stage])

    ui = ui_dummy()
    sut.run(ui)

    assert stage.was_run is True
    assert stage.received_ui is ui


def test__given_workflow_with_stages__when_running__should_call_all_stages():
    stages_to_run = [StageSpy(), StageSpy()]
    sut = make_sut(stages=stages_to_run)

    sut.run(ui_dummy())

    assert all(stages_to_run)


def test__given_workflow_with_stage__when_stage_fails__should_return_false():
    sut = make_sut([failing_stage()])

    actual = sut.run(ui_dummy())

    assert actual is False


def test__given_workflow_with_stages__when_first_stage_fails__should_not_call_second_stage():
    second_stage = StageSpy()
    sut = make_sut([failing_stage(), second_stage])

    sut.run(ui_dummy())

    assert second_stage.was_run is False


def test__given_running_workflow_with_stage__when_canceling__should_call_cancel_on_stage():
    stage = StageSpy()
    sut = make_sut([stage])

    stage.run_callback = cancel_workflow(sut)

    sut.run(ui_dummy())

    assert stage.was_canceled is True


def test__when_canceling_workflow_without_running__should_raise_error():
    sut = make_sut([])

    with pytest.raises(WorkflowNotStartedError):
        sut.cancel(ui_dummy())


def test__given_running_workflow_with_two_stages__when_canceling_during_first_stage__should_not_run_second_stage():
    first_stage = StageSpy()
    second_stage = StageSpy()
    sut = make_sut([first_stage, second_stage])

    first_stage.run_callback = cancel_workflow(sut)

    sut.run(ui_dummy())

    assert second_stage.was_run is False


def cancel_workflow(sut: Workflow):
    return lambda: sut.cancel(ui_dummy())
