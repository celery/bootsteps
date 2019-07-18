import pytest
from eliot import MemoryLogger
from eliot.testing import check_for_errors, swap_logger


@pytest.fixture(autouse=True)
def logger():
    test_logger = MemoryLogger()
    swap_logger(test_logger)
    yield test_logger
    check_for_errors(test_logger)
