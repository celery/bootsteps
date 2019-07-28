from unittest.mock import MagicMock

import pytest
from eliot import MemoryLogger
from eliot.testing import check_for_errors, swap_logger
from networkx import DiGraph


@pytest.fixture(autouse=True)
def logger():
    test_logger = MemoryLogger()
    swap_logger(test_logger)
    yield test_logger
    check_for_errors(test_logger)


@pytest.fixture
def bootsteps_graph():
    mocked_graph = MagicMock(spec_set=DiGraph)
    mocked_graph.__iter__.return_value = []

    return mocked_graph
