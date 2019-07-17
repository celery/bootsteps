from unittest.mock import Mock

from eliot.testing import assertContainsFields, LoggedAction
import pytest
from networkx import DiGraph

from bootsteps import Blueprint, BlueprintContainer
from bootsteps.blueprint import BlueprintState


@pytest.fixture
def bootsteps_graph():
    return Mock(spec_set=DiGraph)


def test_init(bootsteps_graph):
    b = Blueprint(bootsteps_graph, name="Test")

    assert b.name == "Test"
    assert b.steps == bootsteps_graph
    assert b.state == BlueprintState.INITIALIZED


def test_blueprint_container_dependencies_graph():
    mock_step1 = Mock()
    mock_step1.requires = []

    mock_step2 = Mock()
    mock_step2.requires = [mock_step1]

    mock_bootsteps = [mock_step1, mock_step2]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    assert list(MyBlueprintContainer.blueprint.steps.nodes) == mock_bootsteps
    assert list(MyBlueprintContainer.blueprint.steps.edges) == [
        (mock_step2, mock_step1)
    ]
