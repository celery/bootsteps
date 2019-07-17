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
    mock_step1 = Mock(name="step1")
    mock_step1.requires = []
    mock_step1.last = False

    mock_step2 = Mock(name="step2")
    mock_step2.requires = [mock_step1]
    mock_step2.last = False

    mock_step3 = Mock(name="step3")
    mock_step3.requires = []
    mock_step3.last = True

    mock_bootsteps = [mock_step1, mock_step2, mock_step3]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    assert list(MyBlueprintContainer.blueprint.steps.nodes) == mock_bootsteps
    assert list(MyBlueprintContainer.blueprint.steps.edges) == [
        (mock_step2, mock_step1),
        (mock_step3, mock_step1),
        (mock_step3, mock_step2)
    ]


def test_blueprint_container_dependencies_graph_with_two_last_steps():
    mock_step1 = Mock(name="step1")
    mock_step1.requires = []
    mock_step1.last = True

    mock_step2 = Mock(name="step2")
    mock_step2.requires = [mock_step1]
    mock_step2.last = False

    mock_step3 = Mock(name="step3")
    mock_step3.requires = []
    mock_step3.last = True

    mock_bootsteps = [mock_step1, mock_step2, mock_step3]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    with pytest.raises(ValueError, match='Only one boot step can be last. Found 2.'):
        MyBlueprintContainer.blueprint
