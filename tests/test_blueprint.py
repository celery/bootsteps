from unittest.mock import call

import pytest
import trio
from asynctest import MagicMock, Mock
from eliot.testing import LoggedAction, LoggedMessage

from bootsteps import AsyncStep, Blueprint
from bootsteps.blueprint import BlueprintState, ExecutionOrder
from tests.assertions import (
    assert_log_message_field_equals,
    assert_logged_action_failed,
    assert_logged_action_succeeded,
    assert_field_equals_in_any_message
)
from tests.mocks import TrioCoroutineMock, create_mock_step, create_start_stop_mock_step


@pytest.fixture
def mock_execution_order_strategy_class():
    return MagicMock(name="ExecutionOrder", spec_set=ExecutionOrder)


@pytest.fixture(autouse=True)
def mock_inspect_isawaitable(mocker):
    return mocker.patch(
        "bootsteps.blueprint.inspect.isawaitable",
        side_effect=lambda o: isinstance(o, TrioCoroutineMock),
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


async def test_blueprint_start(
    bootsteps_graph, mock_execution_order_strategy_class, logger
):
    mock_step1 = create_mock_step("step1")
    mock_step2 = create_start_stop_mock_step("step2")
    mock_step3 = create_mock_step("step3")
    mock_step4 = create_start_stop_mock_step("step4", mock_class=TrioCoroutineMock)
    mock_step5 = create_mock_step("step5")
    mock_step6 = create_mock_step("step6", spec=AsyncStep, mock_class=TrioCoroutineMock)

    # We're using a parent mock simply to record the order of calls to different
    # steps
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

    async with trio.open_nursery() as nursery:
        nursery.start_soon(blueprint.start)

        with trio.fail_after(1):
            assert (
                await blueprint.state_changes_receive_channel.receive()
                == BlueprintState.RUNNING
            )
        with trio.fail_after(1):
            assert (
                await blueprint.state_changes_receive_channel.receive()
                == BlueprintState.COMPLETED
            )

    mock_execution_order_strategy_class.assert_called_once_with(blueprint._steps)

    assert_parallelized_steps_are_in_order(
        m.method_calls,
        [
            [call.mock_step1(), call.mock_step2.start()],
            [call.mock_step3(), call.mock_step4.start(), call.mock_step5()],
            [call.mock_step6()],
        ],
    )

    mock_step6.assert_awaited_once_with()
    mock_step4.start.assert_awaited_once_with()

    logged_actions = LoggedAction.of_type(logger.messages, "bootsteps:blueprint:start")
    assert len(logged_actions) == 1
    logged_action = logged_actions[0]
    assert_log_message_field_equals(logged_action.start_message, "name", blueprint.name)
    assert_logged_action_succeeded(logged_action)

    messages = LoggedMessage.of_type(
        logger.messages, "bootsteps:blueprint:next_bootsteps"
    )
    assert len(messages) == 3

    assert_log_message_field_equals(messages[0].message, "name", blueprint.name)
    assert_log_message_field_equals(
        messages[0].message, "next_bootsteps", [m.mock_step1, m.mock_step2]
    )

    assert_log_message_field_equals(messages[1].message, "name", blueprint.name)
    assert_log_message_field_equals(
        messages[1].message,
        "next_bootsteps",
        [m.mock_step3, m.mock_step4, m.mock_step5],
    )

    assert_log_message_field_equals(messages[2].message, "name", blueprint.name)
    assert_log_message_field_equals(
        messages[2].message, "next_bootsteps", [m.mock_step6]
    )

    logged_actions = LoggedAction.of_type(logger.messages, "bootsteps:blueprint:executing_bootstep")
    assert len(logged_actions) == 6

    start_messages = [logged_action.start_message for logged_action in logged_actions]

    assert_field_equals_in_any_message(start_messages, "bootstep", m.mock_step1)
    assert_field_equals_in_any_message(start_messages, "bootstep", m.mock_step2.start)
    assert_field_equals_in_any_message(start_messages, "bootstep", m.mock_step3)
    assert_field_equals_in_any_message(start_messages, "bootstep", m.mock_step4.start)
    assert_field_equals_in_any_message(start_messages, "bootstep", m.mock_step5)
    assert_field_equals_in_any_message(start_messages, "bootstep", m.mock_step6)

    assert_logged_action_succeeded(logged_actions[0])
    assert_logged_action_succeeded(logged_actions[1])
    assert_logged_action_succeeded(logged_actions[2])
    assert_logged_action_succeeded(logged_actions[3])
    assert_logged_action_succeeded(logged_actions[4])
    assert_logged_action_succeeded(logged_actions[5])


async def test_blueprint_start_failure(
    bootsteps_graph, mock_execution_order_strategy_class, logger
):
    mock_step1 = create_mock_step("step1")
    mock_step1.side_effect = expected_exception = RuntimeError("Expected Failure")
    mock_step2 = create_start_stop_mock_step("step2")
    mock_step3 = create_mock_step("step3")
    mock_step4 = create_start_stop_mock_step("step4", mock_class=TrioCoroutineMock)
    mock_step5 = create_mock_step("step5")
    mock_step6 = create_mock_step("step6", spec=AsyncStep, mock_class=TrioCoroutineMock)

    # We're using a parent mock simply to record the order of calls to different
    # steps
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

    with pytest.raises(RuntimeError):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(blueprint.start)

    with trio.fail_after(1):
        assert (
            await blueprint.state_changes_receive_channel.receive()
            == BlueprintState.RUNNING
        )

    with trio.fail_after(1):
        assert await blueprint.state_changes_receive_channel.receive() == (
            BlueprintState.FAILED,
            expected_exception,
        )

    mock_execution_order_strategy_class.assert_called_once_with(blueprint._steps)

    assert_parallelized_steps_are_in_order(
        m.method_calls, [[call.mock_step1(), call.mock_step2.start()]]
    )
    mock_step3.assert_not_called()
    mock_step4.start.assert_not_called()
    mock_step5.assert_not_called()
    mock_step6.assert_not_called()

    logged_actions = LoggedAction.of_type(logger.messages, "bootsteps:blueprint:start")
    assert len(logged_actions) == 1
    logged_action = logged_actions[0]
    assert_log_message_field_equals(logged_action.start_message, "name", blueprint.name)
    assert_logged_action_failed(logged_action)

    messages = LoggedMessage.of_type(
        logger.messages, "bootsteps:blueprint:next_bootsteps"
    )
    assert len(messages) == 1

    assert_log_message_field_equals(messages[0].message, "name", blueprint.name)
    assert_log_message_field_equals(
        messages[0].message, "next_bootsteps", [m.mock_step1, m.mock_step2]
    )

    logged_actions = LoggedAction.of_type(logger.messages, "bootsteps:blueprint:executing_bootstep")
    assert len(logged_actions) == 2

    mock_step1.side_effect = None

    assert_log_message_field_equals(logged_actions[0].start_message, "bootstep", m.mock_step1)
    assert_logged_action_failed(logged_actions[0])

    assert_log_message_field_equals(logged_actions[1].start_message, "bootstep", m.mock_step2.start)
    assert_logged_action_succeeded(logged_actions[1])


async def test_blueprint_stop(
    bootsteps_graph, mock_execution_order_strategy_class, logger
):
    mock_step1 = create_mock_step("step1")
    mock_step2 = create_start_stop_mock_step("step2")
    mock_step3 = create_mock_step("step3")
    mock_step4 = create_start_stop_mock_step("step4", mock_class=TrioCoroutineMock)
    mock_step5 = create_mock_step("step5")
    mock_step6 = create_mock_step("step6", spec=AsyncStep, mock_class=TrioCoroutineMock)

    # We're using a parent mock simply to record the order of calls to different
    # steps
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
    reversed_func = Mock(return_value=reversed(expected_execution_order))
    mock_iterator.__reversed__ = reversed_func
    mock_iterator.__iter__.return_value = expected_execution_order
    mock_execution_order_strategy_class.return_value = mock_iterator

    blueprint = Blueprint(
        bootsteps_graph,
        name="Test",
        execution_order_strategy_class=mock_execution_order_strategy_class,
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(blueprint.stop)

        with trio.fail_after(1):
            assert (
                await blueprint.state_changes_receive_channel.receive()
                == BlueprintState.TERMINATING
            )
        with trio.fail_after(1):
            assert (
                await blueprint.state_changes_receive_channel.receive()
                == BlueprintState.TERMINATED
            )

    mock_execution_order_strategy_class.assert_called_once_with(blueprint._steps)

    assert_parallelized_steps_are_in_order(
        m.method_calls, [[call.mock_step4.stop()], [call.mock_step2.stop()]]
    )

    mock_step4.stop.assert_awaited_once_with()

    mock_step1.assert_not_called()
    mock_step3.assert_not_called()
    mock_step5.assert_not_called()
    mock_step6.assert_not_called()

    logged_actions = LoggedAction.of_type(logger.messages, "bootsteps:blueprint:stop")
    assert len(logged_actions) == 1
    logged_action = logged_actions[0]
    assert_log_message_field_equals(logged_action.start_message, "name", blueprint.name)
    assert_logged_action_succeeded(logged_action)

    messages = LoggedMessage.of_type(
        logger.messages, "bootsteps:blueprint:next_bootsteps"
    )
    assert len(messages) == 2

    assert_log_message_field_equals(messages[0].message, "name", blueprint.name)
    assert_log_message_field_equals(
        messages[0].message, "next_bootsteps", [m.mock_step4]
    )

    assert_log_message_field_equals(messages[1].message, "name", blueprint.name)
    assert_log_message_field_equals(
        messages[1].message,
        "next_bootsteps",
        [m.mock_step2],
    )

    logged_actions = LoggedAction.of_type(logger.messages, "bootsteps:blueprint:executing_bootstep")
    assert len(logged_actions) == 2

    start_messages = [logged_action.start_message for logged_action in logged_actions]

    assert_field_equals_in_any_message(start_messages, "bootstep", m.mock_step4.stop)
    assert_field_equals_in_any_message(start_messages, "bootstep", m.mock_step2.stop)

    assert_logged_action_succeeded(logged_actions[0])
    assert_logged_action_succeeded(logged_actions[1])


async def test_blueprint_stop_failure(
    bootsteps_graph, mock_execution_order_strategy_class, logger
):
    mock_step1 = create_mock_step("step1")
    mock_step2 = create_start_stop_mock_step("step2")
    mock_step3 = create_mock_step("step3")
    mock_step4 = create_start_stop_mock_step("step4", mock_class=TrioCoroutineMock)
    mock_step4.stop.side_effect = expected_exception = RuntimeError("Expected Failure")
    mock_step5 = create_mock_step("step5")
    mock_step6 = create_mock_step("step6", spec=AsyncStep, mock_class=TrioCoroutineMock)

    # We're using a parent mock simply to record the order of calls to different
    # steps
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
    reversed_func = Mock(return_value=reversed(expected_execution_order))
    mock_iterator.__reversed__ = reversed_func
    mock_execution_order_strategy_class.return_value = mock_iterator

    blueprint = Blueprint(
        bootsteps_graph,
        name="Test",
        execution_order_strategy_class=mock_execution_order_strategy_class,
    )

    with pytest.raises(RuntimeError):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(blueprint.stop)

    with trio.fail_after(1):
        assert (
            await blueprint.state_changes_receive_channel.receive()
            == BlueprintState.TERMINATING
        )

    with trio.fail_after(1):
        assert await blueprint.state_changes_receive_channel.receive() == (
            BlueprintState.FAILED,
            expected_exception,
        )

    mock_execution_order_strategy_class.assert_called_once_with(blueprint._steps)

    assert_parallelized_steps_are_in_order(m.method_calls, [[call.mock_step4.stop()]])

    mock_step1.assert_not_called()
    mock_step2.stop.assert_not_called()
    mock_step3.assert_not_called()
    mock_step5.assert_not_called()
    mock_step6.assert_not_called()

    logged_actions = LoggedAction.of_type(logger.messages, "bootsteps:blueprint:stop")
    assert len(logged_actions) == 1
    logged_action = logged_actions[0]
    assert_log_message_field_equals(logged_action.start_message, "name", blueprint.name)
    assert_logged_action_failed(logged_action)

    messages = LoggedMessage.of_type(
        logger.messages, "bootsteps:blueprint:next_bootsteps"
    )
    assert len(messages) == 1, messages

    assert_log_message_field_equals(messages[0].message, "name", blueprint.name)
    assert_log_message_field_equals(
        messages[0].message, "next_bootsteps", [m.mock_step4]
    )

    logged_actions = LoggedAction.of_type(logger.messages, "bootsteps:blueprint:executing_bootstep")
    assert len(logged_actions) == 1

    start_messages = [logged_action.start_message for logged_action in logged_actions]

    assert_field_equals_in_any_message(start_messages, "bootstep", m.mock_step4.stop)

    assert_logged_action_failed(logged_actions[0])


async def test_blueprint_async_context_manager(
    bootsteps_graph, mock_execution_order_strategy_class
):
    mock_step1 = create_mock_step("step1")
    mock_step2 = create_start_stop_mock_step("step2")
    mock_step3 = create_mock_step("step3")
    mock_step4 = create_start_stop_mock_step("step4", mock_class=TrioCoroutineMock)
    mock_step5 = create_mock_step("step5")
    mock_step6 = create_mock_step("step6", spec=AsyncStep, mock_class=TrioCoroutineMock)

    # We're using a parent mock simply to record the order of calls to different
    # steps
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
    reversed_func = Mock(return_value=reversed(expected_execution_order))
    mock_iterator.__reversed__ = reversed_func
    mock_iterator.__iter__.return_value = expected_execution_order
    mock_execution_order_strategy_class.return_value = mock_iterator

    blueprint = Blueprint(
        bootsteps_graph,
        name="Test",
        execution_order_strategy_class=mock_execution_order_strategy_class,
    )

    async with blueprint:
        with trio.fail_after(1):
            assert (
                await blueprint.state_changes_receive_channel.receive()
                == BlueprintState.RUNNING
            )
        with trio.fail_after(1):
            assert (
                await blueprint.state_changes_receive_channel.receive()
                == BlueprintState.COMPLETED
            )

    with trio.fail_after(1):
        assert (
            await blueprint.state_changes_receive_channel.receive()
            == BlueprintState.TERMINATING
        )
    with trio.fail_after(1):
        assert (
            await blueprint.state_changes_receive_channel.receive()
            == BlueprintState.TERMINATED
        )

    assert_parallelized_steps_are_in_order(
        m.method_calls,
        [
            [call.mock_step1(), call.mock_step2.start()],
            [call.mock_step3(), call.mock_step4.start(), call.mock_step5()],
            [call.mock_step6()],
            [call.mock_step4.stop()],
            [call.mock_step2.stop()],
        ],
    )

    mock_step6.assert_awaited_once_with()
    mock_step4.start.assert_awaited_once_with()
    mock_step4.stop.assert_awaited_once_with()
