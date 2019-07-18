from unittest.mock import Mock, call

import pytest
from eliot.testing import LoggedAction
from networkx import DiGraph

from bootsteps import Blueprint, BlueprintContainer, Step
from bootsteps.blueprint import BlueprintState


@pytest.fixture
def bootsteps_graph():
    return Mock(spec_set=DiGraph)


def test_init(bootsteps_graph):
    b = Blueprint(bootsteps_graph, name="Test")

    assert b.name == "Test"
    assert b.steps == bootsteps_graph
    assert b.state == BlueprintState.INITIALIZED


def test_blueprint_container_dependencies_graph(logger):
    mock_step1 = Mock(name="step1", spec=Step)
    mock_step1.requires = []
    mock_step1.last = False

    mock_step2 = Mock(name="step2", spec=Step)
    mock_step2.requires = [mock_step1]
    mock_step2.last = False

    mock_step3 = Mock(name="step3", spec=Step)
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

    logged_actions = LoggedAction.of_type(logger.messages, 'bootsteps:blueprint:building_dependency_graph')
    logged_action = logged_actions[0]
    assert ('name' in logged_action.start_message
            and logged_action.start_message['name'] == MyBlueprintContainer.blueprint.name)
    assert ('name' in logged_action.end_message
            and logged_action.end_message['name'] == MyBlueprintContainer.blueprint.name)
    assert ('graph' in logged_action.end_message
            and logged_action.end_message['graph'].nodes == MyBlueprintContainer.blueprint.steps.nodes
            and logged_action.end_message['graph'].edges == MyBlueprintContainer.blueprint.steps.edges)
    assert logged_action.end_message['action_status'] == 'succeeded'


def test_blueprint_container_dependencies_graph_with_two_last_steps(logger):
    mock_step1 = Mock(name="step1", spec=Step)
    mock_step1.requires = []
    mock_step1.last = True

    mock_step2 = Mock(name="step2", spec=Step)
    mock_step2.requires = [mock_step1]
    mock_step2.last = False

    mock_step3 = Mock(name="step3", spec=Step)
    mock_step3.requires = []
    mock_step3.last = True

    mock_bootsteps = [mock_step1, mock_step2, mock_step3]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    with pytest.raises(ValueError, match='Only one boot step can be last. Found 2.'):
        MyBlueprintContainer.blueprint

    logged_actions = LoggedAction.of_type(logger.messages, 'bootsteps:blueprint:building_dependency_graph')
    logged_action = logged_actions[0]
    assert ('name' in logged_action.start_message
            and logged_action.start_message['name'] == MyBlueprintContainer.name)
    assert logged_action.end_message['action_status'] == 'failed'
    assert logged_action.end_message['reason'] == 'Only one boot step can be last. Found 2.'
    assert logged_action.end_message['exception'] == 'builtins.ValueError'


def test_blueprint_container_dependencies_graph_with_circular_dependencies(logger):
    mock_step2 = Mock(name="step2", spec=Step)
    mock_step1 = Mock(name="step1", spec=Step)

    mock_step1.requires = [mock_step2]
    mock_step1.last = True

    mock_step2.requires = [mock_step1]
    mock_step2.last = False

    mock_bootsteps = [mock_step1, mock_step2]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    with pytest.raises(ValueError, match='Circular dependencies found.'):
        MyBlueprintContainer.blueprint

    logged_actions = LoggedAction.of_type(logger.messages, 'bootsteps:blueprint:building_dependency_graph')
    logged_action = logged_actions[0]
    assert ('name' in logged_action.start_message
            and logged_action.start_message['name'] == MyBlueprintContainer.name)
    assert logged_action.end_message['action_status'] == 'failed'
    assert logged_action.end_message['reason'] == 'Circular dependencies found.'
    assert logged_action.end_message['exception'] == 'builtins.ValueError'


def test_start_without_last_step(logger):
    # We're using a parent mock simply to record the order of calls to different
    # steps
    m = Mock()

    mock_step1 = Mock(name="step1", spec=Step)
    mock_step1.requires = []
    mock_step1.last = False

    mock_step2 = Mock(name="step2", spec=Step)
    mock_step2.requires = [mock_step1]
    mock_step2.last = False

    mock_step3 = Mock(name="step3", spec=Step)
    mock_step3.requires = [mock_step1]
    mock_step3.last = False

    mock_step4 = Mock(name="step4", spec=Step)
    mock_step4.requires = []
    mock_step4.last = False

    mock_step5 = Mock(name="step5", spec=Step)
    mock_step5.requires = []
    mock_step5.last = False

    m.attach_mock(mock_step1, 'mock_step1')
    m.attach_mock(mock_step2, 'mock_step2')
    m.attach_mock(mock_step3, 'mock_step3')
    m.attach_mock(mock_step4, 'mock_step4')
    m.attach_mock(mock_step5, 'mock_step5')

    mock_bootsteps = [m.mock_step4, m.mock_step5, m.mock_step1, m.mock_step2, m.mock_step3]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    MyBlueprintContainer.blueprint.start()

    m.assert_has_calls([call.mock_step4, call.mock_step5, call.mock_step1, call.mock_step2, call.mock_step3])

    logged_tasks = LoggedAction.of_type(logger.messages, 'bootsteps:blueprint:start')
    logged_task = logged_tasks[0]
    assert ('name' in logged_task.start_message
            and logged_task.start_message['name'] == MyBlueprintContainer.blueprint.name)
    assert logged_task.end_message['action_status'] == 'succeeded'

    logged_actions = LoggedAction.of_type(logger.messages,
                                         'bootsteps:blueprint:resolving_bootsteps_execution_order')
    assert logged_task.children == logged_actions
    logged_action = logged_actions[0]
    assert ('name' in logged_action.start_message
            and logged_action.start_message['name'] == MyBlueprintContainer.blueprint.name)
    assert logged_action.end_message['action_status'] == 'succeeded'
    assert ('name' in logged_action.end_message
            and logged_action.end_message['name'] == MyBlueprintContainer.blueprint.name)
    assert ('bootsteps_execution_order' in logged_action.end_message
            and logged_action.end_message['bootsteps_execution_order'] == mock_bootsteps)


def test_start_with_last_step(logger):
    # We're using a parent mock simply to record the order of calls to different
    # steps
    m = Mock()

    mock_step1 = Mock(name="step1", spec=Step)
    mock_step1.requires = []
    mock_step1.last = False

    mock_step2 = Mock(name="step2", spec=Step)
    mock_step2.requires = [mock_step1]
    mock_step2.last = False

    mock_step3 = Mock(name="step3", spec=Step)
    mock_step3.requires = []
    mock_step3.last = True

    mock_step4 = Mock(name="step4", spec=Step)
    mock_step4.requires = []
    mock_step4.last = False

    mock_step5 = Mock(name="step5", spec=Step)
    mock_step5.requires = []
    mock_step5.last = False

    m.attach_mock(mock_step1, 'mock_step1')
    m.attach_mock(mock_step2, 'mock_step2')
    m.attach_mock(mock_step3, 'mock_step3')
    m.attach_mock(mock_step4, 'mock_step4')
    m.attach_mock(mock_step5, 'mock_step5')

    mock_bootsteps = [m.mock_step4, m.mock_step5, m.mock_step1, m.mock_step2, m.mock_step3]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    MyBlueprintContainer.blueprint.start()

    m.assert_has_calls([call.mock_step4, call.mock_step5, call.mock_step1, call.mock_step2, call.mock_step3])

    logged_tasks = LoggedAction.of_type(logger.messages, 'bootsteps:blueprint:start')
    logged_task = logged_tasks[0]
    assert ('name' in logged_task.start_message
            and logged_task.start_message['name'] == MyBlueprintContainer.blueprint.name)
    assert logged_task.end_message['action_status'] == 'succeeded'

    logged_actions = LoggedAction.of_type(logger.messages,
                                         'bootsteps:blueprint:resolving_bootsteps_execution_order')
    assert logged_task.children == logged_actions
    logged_action = logged_actions[0]
    assert ('name' in logged_action.start_message
            and logged_action.start_message['name'] == MyBlueprintContainer.blueprint.name)
    assert logged_action.end_message['action_status'] == 'succeeded'
    assert ('name' in logged_action.end_message
            and logged_action.end_message['name'] == MyBlueprintContainer.blueprint.name)
    assert ('bootsteps_execution_order' in logged_action.end_message
            and logged_action.end_message['bootsteps_execution_order'] == mock_bootsteps)
