import pytest
from eliot import MemoryLogger
from eliot.testing import swap_logger, check_for_errors


@pytest.fixture(autouse=True)
def logger():
    test_logger = MemoryLogger()
    swap_logger(test_logger)
    yield test_logger
    check_for_errors(test_logger)
