import inspect
from unittest.mock import call

import pytest
from asynctest import MagicMock, Mock, NonCallableMock, CoroutineMock
from eliot.testing import LoggedAction
from networkx import DiGraph

from bootsteps import Blueprint, BlueprintContainer, Step, AsyncStep
from bootsteps.blueprint import BlueprintState, ExecutionOrder


from tests.mocks import TrioCoroutineMock


@pytest.fixture
def bootsteps_graph():
    return Mock(spec_set=DiGraph)


@pytest.fixture
def mock_execution_order_strategy_class():
    return MagicMock(name="ExecutionOrder", spec_set=ExecutionOrder)


@pytest.fixture(autouse=True)
def mock_inspect_isawaitable(mocker):
    return mocker.patch(
        "bootsteps.blueprint.inspect.isawaitable",
        side_effect=lambda o: isinstance(o, CoroutineMock),
    )


def create_mock_step(
    name,
    requires=set(),
    required_by=set(),
    last=False,
    include_if=True,
    spec=Step,
    mock_class=Mock,
):
    mock_step = mock_class(name=name, spec=spec)
    mock_step.requires = requires
    mock_step.required_by = required_by
    mock_step.last = last
    if isinstance(include_if, bool):
        mock_step.include_if.return_value = include_if
    else:
        mock_step.include_if.side_effect = include_if

    return mock_step


def create_start_stop_mock_step(
    name, requires=set(), required_by=set(), last=False, include_if=True
):
    mock_step = NonCallableMock(name=name, spec=Step)
    mock_step.requires = requires
    mock_step.required_by = required_by
    mock_step.last = last
    mock_step.start = Mock()
    mock_step.stop = Mock()
    if isinstance(include_if, bool):
        mock_step.include_if.return_value = include_if
    else:
        mock_step.include_if.side_effect = include_if

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


def assert_parallelized_steps_are_in_order(
    actual_execution_order, expected_execution_order
):
    __tracebackhide__ = True

    begin = 0

    # Test that all the steps were parallelized in the same order
    for steps in expected_execution_order:
        end = begin + len(steps)
        assert sorted(steps) == sorted(actual_execution_order[begin:end])
        begin = end

    # Ensure no further calls were made
    assert not actual_execution_order[begin:]


def test_init(bootsteps_graph, mock_execution_order_strategy_class):
    b = Blueprint(
        bootsteps_graph,
        name="Test",
        execution_order_strategy_class=mock_execution_order_strategy_class,
    )

    assert b.name == "Test"
    assert b._steps == bootsteps_graph
    assert b.state == BlueprintState.INITIALIZED
    assert b.execution_order_strategy_class == mock_execution_order_strategy_class


async def test_blueprint_start(bootsteps_graph, mock_execution_order_strategy_class):
    mock_step1 = create_mock_step("step1")
    mock_step2 = create_mock_step("step2")
    mock_step3 = create_mock_step("step3")
    mock_step4 = create_mock_step("step4")
    mock_step5 = create_mock_step("step5")
    mock_step6 = create_mock_step("step6", spec=AsyncStep, mock_class=TrioCoroutineMock)

    m = Mock()
    m.attach_mock(mock_step1, "mock_step1")
    m.attach_mock(mock_step2, "mock_step2")
    m.attach_mock(mock_step3, "mock_step3")
    m.attach_mock(mock_step4, "mock_step4")
    m.attach_mock(mock_step5, "mock_step5")
    m.attach_mock(mock_step6, "mock_step6")

    expected_execution_order = [
        [m.mock_step1, m.mock_step2],
        [m.mock_step3, m.mock_step4, m.mock_step5],
        [m.mock_step6],
    ]
    mock_iterator = MagicMock()
    mock_iterator.__iter__.return_value = expected_execution_order
    mock_execution_order_strategy_class.return_value = mock_iterator

    blueprint = Blueprint(
        bootsteps_graph,
        name="Test",
        execution_order_strategy_class=mock_execution_order_strategy_class,
    )

    await blueprint.start()

    mock_execution_order_strategy_class.assert_called_once_with(blueprint._steps.copy())

    assert_parallelized_steps_are_in_order(
        m.method_calls,
        [
            [call.mock_step1(), call.mock_step2()],
            [call.mock_step3(), call.mock_step4(), call.mock_step5()],
            [call.mock_step6()],
        ],
    )

    m.mock_step6.assert_awaited_once_with()


def test_blueprint_container_dependencies_graph(logger):
    mock_step1 = create_mock_step("step1")
    mock_step2 = create_mock_step("step2", requires={mock_step1})
    mock_step3 = create_mock_step("step3", last=True)
    mock_step4 = create_mock_step("step4", required_by={mock_step2})
    mock_step5 = create_mock_step("step5", include_if=False)

    mock_bootsteps = [mock_step1, mock_step4, mock_step2, mock_step3, mock_step5]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    mock_bootsteps.remove(mock_step5)
    assert list(MyBlueprintContainer.blueprint._steps.nodes) == mock_bootsteps
    assert set(MyBlueprintContainer.blueprint._steps.edges) == {
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
        lambda value: value.nodes == MyBlueprintContainer.blueprint._steps.nodes
        and value.edges == MyBlueprintContainer.blueprint._steps.edges,
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


def test_blueprint_container_dependencies_graph_with_no_circular_dependencies_other_step_not_included(
    logger
):
    # Can't use the create_mock_step helper here because of the circular dependency
    mock_step2 = Mock(name="step2", spec=Step)
    mock_step1 = Mock(name="step1", spec=Step)

    mock_step1.requires = {mock_step2}
    mock_step1.required_by = set()
    mock_step1.last = True
    mock_step2.include_if.return_value = True

    mock_step2.requires = {mock_step1}
    mock_step2.required_by = set()
    mock_step2.last = False
    mock_step2.include_if.return_value = False

    mock_bootsteps = [mock_step1, mock_step2]

    class MyBlueprintContainer(BlueprintContainer):
        bootsteps = mock_bootsteps

    try:
        MyBlueprintContainer.blueprint
    except ValueError:
        pytest.fail("Circular dependencies found")


# async def test_start_without_last_step(logger):
#     # We're using a parent mock simply to record the order of calls to different
#     # steps
#     m = Mock()
#
#     mock_step1 = create_mock_step("step1")
#     mock_step2 = create_mock_step("step2", requires={mock_step1})
#     mock_step3 = create_mock_step("step3", requires={mock_step1})
#     mock_step4 = create_mock_step("step4")
#     mock_step5 = create_mock_step("step5")
#
#     m.attach_mock(mock_step1, "mock_step1")
#     m.attach_mock(mock_step2, "mock_step2")
#     m.attach_mock(mock_step3, "mock_step3")
#     m.attach_mock(mock_step4, "mock_step4")
#     m.attach_mock(mock_step5, "mock_step5")
#
#     mock_bootsteps = [
#         m.mock_step4,
#         m.mock_step5,
#         m.mock_step1,
#         m.mock_step2,
#         m.mock_step3,
#     ]
#
#     class MyBlueprintContainer(BlueprintContainer):
#         bootsteps = mock_bootsteps
#
#     await MyBlueprintContainer.blueprint.start()
#
#     m.assert_has_calls(
#         [
#             call.mock_step4,
#             call.mock_step5,
#             call.mock_step1,
#             call.mock_step2,
#             call.mock_step3,
#         ]
#     )
#
#     logged_tasks = LoggedAction.of_type(logger.messages, "bootsteps:blueprint:start")
#     logged_task = logged_tasks[0]
#     assert_log_message_field_equals(
#         logged_task.start_message, "name", MyBlueprintContainer.blueprint.name
#     )
#     assert_logged_action_succeeded(logged_task)
#
#     logged_actions = LoggedAction.of_type(
#         logger.messages, "bootsteps:blueprint:resolving_bootsteps_execution_order"
#     )
#     assert logged_task.children == logged_actions
#     logged_action = logged_actions[0]
#
#     assert_log_message_field_equals(
#         logged_action.start_message, "name", MyBlueprintContainer.blueprint.name
#     )
#     assert_logged_action_succeeded(logged_action)
#     assert_log_message_field_equals(
#         logged_action.end_message, "name", MyBlueprintContainer.blueprint.name
#     )
#     assert_log_message_field_equals(
#         logged_action.end_message, "bootsteps_execution_order", mock_bootsteps
#     )
#     assert_log_message_field_equals(logged_action.end_message, "parallelized_steps", 5)
#
#
# async def test_start_with_last_step(logger):
#     # We're using a parent mock simply to record the order of calls to different
#     # steps
#     m = Mock()
#
#     mock_step1 = create_mock_step("step1")
#     mock_step2 = create_mock_step("step2", requires={mock_step1})
#     mock_step3 = create_mock_step("step3", last=True)
#     mock_step4 = create_mock_step("step4")
#     mock_step5 = create_mock_step("step5")
#
#     m.attach_mock(mock_step1, "mock_step1")
#     m.attach_mock(mock_step2, "mock_step2")
#     m.attach_mock(mock_step3, "mock_step3")
#     m.attach_mock(mock_step4, "mock_step4")
#     m.attach_mock(mock_step5, "mock_step5")
#
#     mock_bootsteps = [
#         m.mock_step4,
#         m.mock_step5,
#         m.mock_step1,
#         m.mock_step2,
#         m.mock_step3,
#     ]
#
#     class MyBlueprintContainer(BlueprintContainer):
#         bootsteps = mock_bootsteps
#
#     await MyBlueprintContainer.blueprint.start()
#
#     m.assert_has_calls(
#         [
#             call.mock_step4,
#             call.mock_step5,
#             call.mock_step1,
#             call.mock_step2,
#             call.mock_step3,
#         ]
#     )
#
#     logged_tasks = LoggedAction.of_type(logger.messages, "bootsteps:blueprint:start")
#     logged_task = logged_tasks[0]
#     assert_log_message_field_equals(
#         logged_task.start_message, "name", MyBlueprintContainer.blueprint.name
#     )
#     assert_logged_action_succeeded(logged_task)
#
#     logged_actions = LoggedAction.of_type(
#         logger.messages, "bootsteps:blueprint:resolving_bootsteps_execution_order"
#     )
#     assert logged_task.children == logged_actions
#     logged_action = logged_actions[0]
#
#     assert_log_message_field_equals(
#         logged_action.start_message, "name", MyBlueprintContainer.blueprint.name
#     )
#     assert_logged_action_succeeded(logged_action)
#     assert_log_message_field_equals(
#         logged_action.end_message, "name", MyBlueprintContainer.blueprint.name
#     )
#     assert_log_message_field_equals(
#         logged_action.end_message, "bootsteps_execution_order", mock_bootsteps
#     )
#     assert_log_message_field_equals(logged_action.end_message, "parallelized_steps", 3)
#
#
# async def test_call_step_start():
#     mock_step1 = create_start_stop_mock_step("step1")
#
#     mock_bootsteps = {mock_step1}
#
#     class MyBlueprintContainer(BlueprintContainer):
#         bootsteps = mock_bootsteps
#
#     await MyBlueprintContainer.blueprint.start()
#
#     mock_step1.start.assert_called_once()
