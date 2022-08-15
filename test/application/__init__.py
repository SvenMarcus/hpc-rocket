from test.application.executor_filesystem_callorder import (
    CallOrderVerification,
    VerifierReturningFilesystemFactory,
)
from test.testdoubles.executor import SlurmJobExecutorSpy
from test.testdoubles.filesystem import DummyFilesystemFactory
from typing import List, Optional, Tuple
from unittest.mock import Mock

from hpcrocket.core.application import Application
from hpcrocket.core.executor import CommandExecutor
from hpcrocket.core.filesystem import FilesystemFactory
from hpcrocket.ui import UI


def make_application(
    executor: Optional[CommandExecutor] = None,
    filesystem_factory: Optional[FilesystemFactory] = None,
    ui: Optional[UI] = None,
) -> Application:
    return Application(
        executor or SlurmJobExecutorSpy(),
        filesystem_factory or DummyFilesystemFactory(),
        ui or Mock(),
    )


def make_application_with_call_order_verification(
    expected_calls: List[str],
) -> Tuple[Application, CallOrderVerification]:
    verifier = CallOrderVerification(expected_calls)
    factory = VerifierReturningFilesystemFactory(verifier)
    sut = Application(verifier, factory, Mock())
    return sut, verifier
