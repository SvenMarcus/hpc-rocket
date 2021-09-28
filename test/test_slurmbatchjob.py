from hpcrocket.watcher.jobwatcher import JobWatcher
from unittest.mock import MagicMock, Mock

import pytest
from hpcrocket.core.executor import CommandExecutor, RunningCommand
from hpcrocket.core.slurmbatchjob import (SlurmError, SlurmBatchJob,
                                          SlurmTaskStatus)


@pytest.fixture
def executor_spy():
    cmd = make_running_command_stub()
    executor_spy = Mock(CommandExecutor)
    executor_spy.exec_command.return_value = cmd

    yield executor_spy


def make_sut(executor, batch_script) -> SlurmBatchJob:
    return SlurmBatchJob(executor, batch_script)


def test__when_submitting__should_execute_sbatch_with_executor(executor_spy: Mock):
    sut = make_sut(executor_spy, "myjobfile.job")

    sut.submit()

    executor_spy.exec_command.assert_called_with("sbatch myjobfile.job")


def test__when_submitting__should_parse_job_id_from_stdout(executor_spy: Mock):
    sut = make_sut(executor_spy, "myjobfile.job")

    actual = sut.submit()

    assert actual == "123456"


def test__when_sbatch_fails__should_throw_slurm_error(executor_spy: Mock):
    cmd_stub = Mock(RunningCommand)
    cmd_stub.configure_mock(
        exit_status=1,
        stderr=lambda: "failed"
    )
    executor_spy.configure_mock(exec_command=lambda _: cmd_stub)

    sut = make_sut(executor_spy, "myjobfile.job")

    with pytest.raises(SlurmError) as exception_info:
        sut.submit()

    assert "failed" in str(exception_info.value)


def test__when_canceling_submitted_job__should_execute_scancel_with_executor_and_remove_from_active_jobs(
        executor_spy: Mock):
    sut = make_sut(executor_spy, "myjobfile.job")
    sut.submit()

    sut.cancel()

    executor_spy.exec_command.assert_called_with("scancel 123456")


def test__when_canceling_unsubmitted_job__should_raise_slurmerror(executor_spy: Mock):
    sut = make_sut(executor_spy, "myjobfile.job")

    with pytest.raises(SlurmError):
        sut.cancel()


def test__given_submitted_job__when_canceling_fails__should_raise_slurm_error(executor_spy: Mock):
    sut = make_sut(executor_spy, "myjobfile.job")
    sut.submit()

    cmd = make_failing_running_command_stub()
    executor_spy.exec_command.return_value = cmd

    with pytest.raises(SlurmError) as exception_info:
        sut.cancel()

    assert "failed" == str(exception_info.value)


def test__given_submitted_job_when_polling_status__should_execute_sacct_with_id(executor_spy: Mock):
    sut = make_sut(executor_spy, "myjobfile.job")
    sut.submit()

    sut.poll_status()

    executor_spy.exec_command.assert_called_with(
        "sacct -j 123456 -o jobid,jobname%30,state --noheader")


def test__when_polling_status__should_return_job_status(executor_spy: Mock):
    sut = make_sut(executor_spy, "myjobfile.job")
    sut.submit()

    cmd = make_command_with_output_from_file()
    executor_spy.exec_command.return_value = cmd

    actual = sut.poll_status()

    assert actual.id == "1603353"
    assert actual.name == "PyFluidsTest"
    assert actual.state == "COMPLETED"


def test__when_polling_status_job_status_should_contain_all_tasks(executor_spy: Mock):
    sut = make_sut(executor_spy, "myjobfile.job")
    sut.submit()

    cmd = make_command_with_output_from_file()
    executor_spy.exec_command.return_value = cmd

    actual = sut.poll_status()

    assert actual.tasks == [
        SlurmTaskStatus("1603353", "PyFluidsTest", "COMPLETED"),
        SlurmTaskStatus("1603353.bat+", "batch", "COMPLETED"),
        SlurmTaskStatus("1603353.ext+",  "extern", "COMPLETED"),
        SlurmTaskStatus("1603353.0", "singularity", "COMPLETED"),
        SlurmTaskStatus("1603353.1", "singularity", "COMPLETED"),
        SlurmTaskStatus("1603353.2", "singularity", "COMPLETED"),
        SlurmTaskStatus("1603353.3", "singularity", "COMPLETED")
    ]


def test__given_unsubmitted_job__when_polling_status__should_raise_slurmerror(executor_spy: Mock):
    sut = make_sut(executor_spy, "myjobfile.job")

    with pytest.raises(SlurmError):
        sut.poll_status()


def test__given_submitted_job__when_gettings_watcher__should_return_jobwatcher(executor_spy: Mock):
    sut = make_sut(executor_spy, "myjobfile.job")

    actual = sut.get_watcher()

    assert isinstance(actual, JobWatcher)


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
