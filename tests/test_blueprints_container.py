from unittest.mock import Mock

import pytest
from eliot.testing import LoggedAction

from bootsteps import BlueprintContainer, Step
from tests.assertions import (
    assert_log_message_field_equals,
    assert_logged_action_failed,
    assert_logged_action_succeeded,
)
from tests.mocks import TrioCoroutineMock, create_mock_step, create_start_stop_mock_step


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
