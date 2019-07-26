import itertools
from unittest.mock import call

import pytest
import trio
from asynctest import CoroutineMock, MagicMock, Mock, NonCallableMock
from bootsteps import AsyncStep, Blueprint, BlueprintContainer, Step
from bootsteps.blueprint import BlueprintState, ExecutionOrder
from eliot.testing import LoggedAction
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from hypothesis_networkx import graph_builder
from multiprocessing_generator import ParallelGenerator
from networkx import (
    DiGraph,
    all_topological_sorts,
    is_directed_acyclic_graph,
    is_isomorphic,
)
from tests.mocks import TrioCoroutineMock


class DebuggableDiGraph(DiGraph):
    def __repr__(self):
        return f"""
Nodes: {self.nodes}
Edges: {self.edges}
        """

    def __eq__(self, other):
        return self.nodes == other.nodes and self.edges == other.edges


@st.composite
def non_isomorphic_graph_builder(
    draw,
    previous_graphs=None,
    node_data=st.fixed_dictionaries({}),
    edge_data=st.fixed_dictionaries({}),
    node_keys=None,
    min_nodes=0,
    max_nodes=25,
    min_edges=0,
    max_edges=None,
    graph_type=DiGraph,
    self_loops=False,
    connected=True,
):
    if not isinstance(previous_graphs, list):
        raise ValueError("previous_graphs must be a list")

    strategy = graph_builder(
        node_data=node_data,
        edge_data=edge_data,
        node_keys=node_keys,
        min_nodes=min_nodes,
        max_nodes=max_nodes,
        min_edges=min_edges,
        max_edges=max_edges,
        graph_type=graph_type,
        self_loops=self_loops,
        connected=connected,
    )
    graph = draw(strategy)
    retries = 0
    while any(is_isomorphic(graph, G) for G in previous_graphs):
        graph = draw(strategy)
        retries += 1
        if retries % 2 == 0:
            min_nodes = min(min_nodes + 2, max_nodes)
            min_edges = min_edges + 2
            strategy = graph_builder(
                node_data=node_data,
                edge_data=edge_data,
                node_keys=node_keys,
                min_nodes=min_nodes,
                max_nodes=max_nodes,
                min_edges=min_edges,
                max_edges=max_edges,
                graph_type=graph_type,
                self_loops=self_loops,
                connected=connected,
            )

    previous_graphs.append(graph)

    return graph


previous_graphs = []
steps_dependency_graph_builder = non_isomorphic_graph_builder(
    graph_type=DebuggableDiGraph,
    min_nodes=1,
    max_nodes=10,
    min_edges=0,
    self_loops=False,
    connected=True,
    previous_graphs=previous_graphs,
).filter(lambda g: is_directed_acyclic_graph(g))


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
    name,
    requires=set(),
    required_by=set(),
    last=False,
    include_if=True,
    mock_class=Mock,
):
    mock_step = NonCallableMock(name=name, spec=Step)
    mock_step.requires = requires
    mock_step.required_by = required_by
    mock_step.last = last
    mock_step.start = mock_class()
    mock_step.stop = mock_class()
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

    mock_execution_order_strategy_class.assert_called_once_with(blueprint._steps.copy())

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


async def test_blueprint_start_failure(
    bootsteps_graph, mock_execution_order_strategy_class
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

    mock_execution_order_strategy_class.assert_called_once_with(blueprint._steps.copy())

    assert_parallelized_steps_are_in_order(
        m.method_calls, [[call.mock_step1(), call.mock_step2.start()]]
    )
    mock_step3.assert_not_called()
    mock_step4.start.assert_not_called()
    mock_step5.assert_not_called()
    mock_step6.assert_not_called()


async def test_blueprint_stop(bootsteps_graph, mock_execution_order_strategy_class):
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

    mock_execution_order_strategy_class.assert_called_once_with(blueprint._steps.copy())

    assert_parallelized_steps_are_in_order(
        m.method_calls, [[call.mock_step4.stop()], [call.mock_step2.stop()]]
    )

    mock_step4.stop.assert_awaited_once_with()

    mock_step1.assert_not_called()
    mock_step3.assert_not_called()
    mock_step5.assert_not_called()
    mock_step6.assert_not_called()


async def test_blueprint_stop_failure(
    bootsteps_graph, mock_execution_order_strategy_class
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

    mock_execution_order_strategy_class.assert_called_once_with(blueprint._steps.copy())

    assert_parallelized_steps_are_in_order(m.method_calls, [[call.mock_step4.stop()]])

    mock_step1.assert_not_called()
    mock_step2.stop.assert_not_called()
    mock_step3.assert_not_called()
    mock_step5.assert_not_called()
    mock_step6.assert_not_called()


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


@given(steps_dependency_graph=steps_dependency_graph_builder)
@settings(
    suppress_health_check=(HealthCheck.too_slow,), max_examples=1000, deadline=1200000
)
@pytest.mark.run(order=1)
def test_execution_order(steps_dependency_graph, request):
    g = steps_dependency_graph.copy()
    cached_topsorts = request.config.cache.get(repr(g), default=[])
    expected = (
        cached_topsorts
        if cached_topsorts
        else ParallelGenerator(
            (tuple(reversed(topsort)) for topsort in all_topological_sorts(g)),
            max_lookahead=2,
        )
    )
    execution_order = ExecutionOrder(steps_dependency_graph)

    for steps in execution_order:
        assert all(step not in steps_dependency_graph.nodes for step in steps)

    actual = tuple(itertools.chain.from_iterable(execution_order))

    if cached_topsorts:
        assert any(
            list(actual) == topsort for topsort in cached_topsorts
        ), f"{actual} not found in {cached_topsorts}"
    else:
        with expected as topsorts:
            done = False
            for topsort in topsorts:
                cached_topsorts.append(topsort)

                if actual == topsort:
                    done = True
                    break

            request.config.cache.set(repr(g), tuple(cached_topsorts))
            assert done
