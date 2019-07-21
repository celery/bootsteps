from unittest.mock import Mock, NonCallableMock, call

import pytest
from eliot.testing import LoggedAction
from networkx import DiGraph

from bootsteps import Blueprint, BlueprintContainer, Step
from bootsteps.blueprint import BlueprintState


@pytest.fixture
def bootsteps_graph():
    return Mock(spec_set=DiGraph)


def create_mock_step(name, requires=set(), required_by=set(), last=False):
    mock_step = Mock(name=name, spec=Step)
    mock_step.requires = requires
    mock_step.required_by = required_by
    mock_step.last = last

    return mock_step


def create_start_stop_mock_step(name, requires=set(), required_by=set(), last=False):
    mock_step = NonCallableMock(name=name, spec=Step)
    mock_step.requires = requires
    mock_step.required_by = required_by
    mock_step.last = last
    mock_step.start = Mock()
    mock_step.stop = Mock()

    return mock_step


def assert_log_message_field_equals(log_message, field_name, value):
    __tracebackhide__ = True

    assert (
        field_name in log_message and value(log_message[field_name])
        if callable(value)
        else log_message[field_name] == value
    )


def assert_logged_action_succeeded(logged_action):
    __tracebackhide__ = True

    assert_log_message_field_equals(
        logged_action.end_message, "action_status", "succeeded"
    )


def assert_logged_action_failed(logged_action):
    __tracebackhide__ = True

    assert_log_message_field_equals(
        logged_action.end_message, "action_status", "failed"
    )


def test_init(bootsteps_graph):
    b = Blueprint(bootsteps_graph, name="Test")

    assert b.name == "Test"
    assert b.steps == bootsteps_graph
    assert b.state == BlueprintState.INITIALIZED


def test_blueprint_container_dependencies_graph(logger):
    mock_step1 = create_mock_step("step1")
    mock_step2 = create_mock_step("step2", requires={mock_step1})
    mock_step3 = create_mock_step("step3", last=True)
    mock_step4 = create_mock_step("step4", required_by={mock_step2})

    mock_bootsteps = [mock_step1, mock_step4, mock_step2, mock_step3]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    assert list(MyBlueprintContainer.blueprint.steps.nodes) == mock_bootsteps
    assert set(MyBlueprintContainer.blueprint.steps.edges) == {
        (mock_step2, mock_step1),
        (mock_step2, mock_step4),
        (mock_step3, mock_step1),
        (mock_step3, mock_step4),
        (mock_step3, mock_step2),
    }

    logged_actions = LoggedAction.of_type(
        logger.messages, "bootsteps:blueprint:building_dependency_graph"
    )
    logged_action = logged_actions[0]

    assert_log_message_field_equals(
        logged_action.start_message, "name", MyBlueprintContainer.blueprint.name
    )

    assert_log_message_field_equals(
        logged_action.end_message, "name", MyBlueprintContainer.blueprint.name
    )
    assert_log_message_field_equals(
        logged_action.end_message,
        "graph",
        lambda value: value.nodes == MyBlueprintContainer.blueprint.steps.nodes
        and value.edges == MyBlueprintContainer.blueprint.steps.edges,
    )
    assert_logged_action_succeeded(logged_action)


def test_blueprint_container_dependencies_graph_with_two_last_steps(logger):
    mock_step1 = create_mock_step("step1", last=True)
    mock_step2 = create_mock_step("step2", requires={mock_step1})
    mock_step3 = create_mock_step("step3", last=True)

    mock_bootsteps = [mock_step1, mock_step2, mock_step3]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    with pytest.raises(ValueError, match="Only one boot step can be last. Found 2."):
        MyBlueprintContainer.blueprint

    logged_actions = LoggedAction.of_type(
        logger.messages, "bootsteps:blueprint:building_dependency_graph"
    )
    logged_action = logged_actions[0]

    assert_log_message_field_equals(
        logged_action.start_message, "name", MyBlueprintContainer.name
    )
    assert_logged_action_failed(logged_action)
    assert_log_message_field_equals(
        logged_action.end_message, "reason", "Only one boot step can be last. Found 2."
    )
    assert_log_message_field_equals(
        logged_action.end_message, "exception", "builtins.ValueError"
    )


def test_blueprint_container_dependencies_graph_with_circular_dependencies(logger):
    # Can't use the create_mock_step helper here because of the circular dependency
    mock_step2 = Mock(name="step2", spec=Step)
    mock_step1 = Mock(name="step1", spec=Step)

    mock_step1.requires = {mock_step2}
    mock_step1.required_by = set()
    mock_step1.last = True

    mock_step2.requires = {mock_step1}
    mock_step2.required_by = set()
    mock_step2.last = False

    mock_bootsteps = [mock_step1, mock_step2]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    with pytest.raises(ValueError, match="Circular dependencies found."):
        MyBlueprintContainer.blueprint

    logged_actions = LoggedAction.of_type(
        logger.messages, "bootsteps:blueprint:building_dependency_graph"
    )
    logged_action = logged_actions[0]

    assert_log_message_field_equals(
        logged_action.start_message, "name", MyBlueprintContainer.name
    )

    assert_logged_action_failed(logged_action)
    assert_log_message_field_equals(
        logged_action.end_message, "reason", "Circular dependencies found."
    )
    assert_log_message_field_equals(
        logged_action.end_message, "exception", "builtins.ValueError"
    )


async def test_start_without_last_step(logger):
    # We're using a parent mock simply to record the order of calls to different
    # steps
    m = Mock()

    mock_step1 = create_mock_step("step1")
    mock_step2 = create_mock_step("step2", requires={mock_step1})
    mock_step3 = create_mock_step("step3", requires={mock_step1})
    mock_step4 = create_mock_step("step4")
    mock_step5 = create_mock_step("step5")

    m.attach_mock(mock_step1, "mock_step1")
    m.attach_mock(mock_step2, "mock_step2")
    m.attach_mock(mock_step3, "mock_step3")
    m.attach_mock(mock_step4, "mock_step4")
    m.attach_mock(mock_step5, "mock_step5")

    mock_bootsteps = [
        m.mock_step4,
        m.mock_step5,
        m.mock_step1,
        m.mock_step2,
        m.mock_step3,
    ]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    await MyBlueprintContainer.blueprint.start()

    m.assert_has_calls(
        [
            call.mock_step4,
            call.mock_step5,
            call.mock_step1,
            call.mock_step2,
            call.mock_step3,
        ]
    )

    logged_tasks = LoggedAction.of_type(logger.messages, "bootsteps:blueprint:start")
    logged_task = logged_tasks[0]
    assert_log_message_field_equals(
        logged_task.start_message, "name", MyBlueprintContainer.blueprint.name
    )
    assert_logged_action_succeeded(logged_task)

    logged_actions = LoggedAction.of_type(
        logger.messages, "bootsteps:blueprint:resolving_bootsteps_execution_order"
    )
    assert logged_task.children == logged_actions
    logged_action = logged_actions[0]

    assert_log_message_field_equals(
        logged_action.start_message, "name", MyBlueprintContainer.blueprint.name
    )
    assert_logged_action_succeeded(logged_action)
    assert_log_message_field_equals(
        logged_action.end_message, "name", MyBlueprintContainer.blueprint.name
    )
    assert_log_message_field_equals(
        logged_action.end_message, "bootsteps_execution_order", mock_bootsteps
    )
    assert_log_message_field_equals(logged_action.end_message, "parallelized_steps", 4)


async def test_start_with_last_step(logger):
    # We're using a parent mock simply to record the order of calls to different
    # steps
    m = Mock()

    mock_step1 = create_mock_step("step1")
    mock_step2 = create_mock_step("step2", requires={mock_step1})
    mock_step3 = create_mock_step("step3", last=True)
    mock_step4 = create_mock_step("step4")
    mock_step5 = create_mock_step("step5")

    m.attach_mock(mock_step1, "mock_step1")
    m.attach_mock(mock_step2, "mock_step2")
    m.attach_mock(mock_step3, "mock_step3")
    m.attach_mock(mock_step4, "mock_step4")
    m.attach_mock(mock_step5, "mock_step5")

    mock_bootsteps = [
        m.mock_step4,
        m.mock_step5,
        m.mock_step1,
        m.mock_step2,
        m.mock_step3,
    ]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    await MyBlueprintContainer.blueprint.start()

    m.assert_has_calls(
        [
            call.mock_step4,
            call.mock_step5,
            call.mock_step1,
            call.mock_step2,
            call.mock_step3,
        ]
    )

    logged_tasks = LoggedAction.of_type(logger.messages, "bootsteps:blueprint:start")
    logged_task = logged_tasks[0]
    assert_log_message_field_equals(
        logged_task.start_message, "name", MyBlueprintContainer.blueprint.name
    )
    assert_logged_action_succeeded(logged_task)

    logged_actions = LoggedAction.of_type(
        logger.messages, "bootsteps:blueprint:resolving_bootsteps_execution_order"
    )
    assert logged_task.children == logged_actions
    logged_action = logged_actions[0]

    assert_log_message_field_equals(
        logged_action.start_message, "name", MyBlueprintContainer.blueprint.name
    )
    assert_logged_action_succeeded(logged_action)
    assert_log_message_field_equals(
        logged_action.end_message, "name", MyBlueprintContainer.blueprint.name
    )
    assert_log_message_field_equals(
        logged_action.end_message, "bootsteps_execution_order", mock_bootsteps
    )


async def test_call_step_start():
    mock_step1 = create_start_stop_mock_step("step1")

    mock_bootsteps = {mock_step1}

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    await MyBlueprintContainer.blueprint.start()

    mock_step1.start.assert_called_once()
