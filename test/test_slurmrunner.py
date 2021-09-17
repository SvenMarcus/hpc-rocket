from unittest.mock import MagicMock, Mock

import pytest
from hpcrocket.core.executor import CommandExecutor, RunningCommand
from hpcrocket.core.slurmrunner import (SlurmError, SlurmRunner,
                                        SlurmTask)


@pytest.fixture
def executor_spy():
    cmd = make_running_command_stub()
    executor_spy = Mock(CommandExecutor)
    executor_spy.exec_command.return_value = cmd

    yield executor_spy


def make_sut(executor) -> SlurmRunner:
    return SlurmRunner(executor)


def test__when_running_sbatch__should_execute_sbatch_with_executor(executor_spy: Mock):
    sut = make_sut(executor_spy)

    sut.sbatch("myjobfile.job")

    executor_spy.exec_command.assert_called_with("sbatch myjobfile.job")


def test__when_running_sbatch__should_parse_job_id_from_stdout(executor_spy: Mock):
    sut = make_sut(executor_spy)

    actual = sut.sbatch("myjobfile.job")

    assert actual == "123456"
    assert "123456" in sut.active_jobs


def test__when_sbatch_fails__should_throw_slurm_error(executor_spy: Mock):
    cmd_stub = Mock(RunningCommand)
    cmd_stub.configure_mock(
        exit_status=1,
        stderr=lambda: "failed"
    )
    executor_spy.configure_mock(exec_command=lambda _: cmd_stub)

    sut = make_sut(executor_spy)

    with pytest.raises(SlurmError) as exception_info:
        sut.sbatch("myjobfile.job")

    assert "failed" in str(exception_info.value)


def test__when_running_scancel_on_started_job__should_execute_scancel_with_executor_and_remove_from_active_jobs(
        executor_spy: Mock):
    sut = make_sut(executor_spy)
    sut.sbatch("myjobfile.job")

    sut.scancel("123456")

    executor_spy.exec_command.assert_called_with("scancel 123456")
    assert "123456" not in sut.active_jobs


def test__when_running_scancel_with_unknown_job_id__should_execute_scancel_with_executor(executor_spy: Mock):
    sut = make_sut(executor_spy)

    sut.scancel("123456")

    executor_spy.exec_command.assert_called_with("scancel 123456")
    assert "123456" not in sut.active_jobs


def test__when_scancel_fails__should_raise_slurm_error(executor_spy: Mock):
    cmd = make_failing_running_command_stub()
    executor_spy.exec_command.return_value = cmd

    sut = make_sut(executor_spy)

    with pytest.raises(SlurmError) as exception_info:
        sut.scancel("123456")

    assert "failed" == str(exception_info.value)


def test__when_polling_status__should_execute_sacct_with_id(executor_spy: Mock):
    sut = make_sut(executor_spy)

    sut.poll_status("123456")

    executor_spy.exec_command.assert_called_with(
        "sacct -j 123456 -o jobid,jobname%30,state --noheader")


def test__when_polling_status__should_return_job_status(executor_spy: Mock):
    cmd = make_command_with_output_from_file()
    executor_spy.exec_command.return_value = cmd
    sut = make_sut(executor_spy)

    actual = sut.poll_status("1603353")

    assert actual.id == "1603353"
    assert actual.name == "PyFluidsTest"
    assert actual.state == "COMPLETED"


def test__when_polling_status_job_status_should_contain_all_tasks(executor_spy: Mock):
    cmd = make_command_with_output_from_file()
    executor_spy.exec_command.return_value = cmd
    sut = make_sut(executor_spy)

    actual = sut.poll_status("1603353")

    assert actual.tasks == [
        SlurmTask("1603353", "PyFluidsTest", "COMPLETED"),
        SlurmTask("1603353.bat+", "batch", "COMPLETED"),
        SlurmTask("1603353.ext+",  "extern", "COMPLETED"),
        SlurmTask("1603353.0", "singularity", "COMPLETED"),
        SlurmTask("1603353.1", "singularity", "COMPLETED"),
        SlurmTask("1603353.2", "singularity", "COMPLETED"),
        SlurmTask("1603353.3", "singularity", "COMPLETED")
    ]


def make_running_command_stub():
    cmd_stub = MagicMock(RunningCommand)

    def stdout():
        if cmd_stub.wait_until_exit.called:
            return ["Submitted batch job 123456"]
        raise RuntimeError("Did not wait for exit")

    cmd_stub.configure_mock(stdout=stdout, exit_status=0)
    return cmd_stub


def make_failing_running_command_stub():
    cmd = Mock(RunningCommand)

    def stderr():
        if cmd.wait_until_exit.called:
            return "failed"
        raise RuntimeError("Did not wait for exit")

    cmd.configure_mock(
        exit_status=1,
        stderr=stderr
    )
    return cmd


def make_command_with_output_from_file():
    cmd = Mock(RunningCommand)

    def stdout():
        if not cmd.wait_until_exit.called:
            raise RuntimeError("Did not wait for exit")
        with open("test/slurmoutput/sacct_completed.txt") as f:
            return f.readlines()

    cmd.configure_mock(
        exit_status=0,
        stdout=stdout
    )

    return cmd
