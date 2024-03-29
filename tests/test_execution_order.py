import itertools

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st
from multiprocessing_generator import ParallelGenerator
from networkx import all_topological_sorts

from bootsteps.blueprint import ExecutionOrder
from tests.strategies import non_isomorphic_graph_builder

previous_graphs = []
steps_dependency_graph_builder = non_isomorphic_graph_builder(
    min_nodes=1,
    max_nodes=10,
    min_edges=0,
    self_loops=False,
    connected=True,
    previous_graphs=previous_graphs,
)


def test_initialization(bootsteps_graph):
    execution_order = ExecutionOrder(bootsteps_graph)

    assert (
        execution_order._steps_dependency_graph
        == execution_order._current_steps_dependency_graph
        == bootsteps_graph
    )


@given(
    steps_dependency_graph=steps_dependency_graph_builder,
    number_of_pending_nodes=st.integers(min_value=0, max_value=9),
)
@settings(suppress_health_check=(HealthCheck.too_slow, HealthCheck.filter_too_much))
def test_mark_as_pending_execution(steps_dependency_graph, number_of_pending_nodes):
    assume(number_of_pending_nodes <= len(steps_dependency_graph.nodes))
    execution_order = ExecutionOrder(steps_dependency_graph)
    pending_steps = {
        list(steps_dependency_graph.nodes)[i] for i in range(number_of_pending_nodes)
    }

    execution_order.mark_as_pending_execution(pending_steps)

    assert all(
        step not in execution_order._current_steps_dependency_graph.nodes
        for step in pending_steps
    )
    assert [pending_steps] == execution_order._execution_order


@given(
    steps_dependency_graph=steps_dependency_graph_builder.filter(
        lambda g: any(not any(g.neighbors(n)) for n in g.nodes)
    )
)
@settings(suppress_health_check=(HealthCheck.too_slow, HealthCheck.filter_too_much))
def test_steps_without_dependencies(steps_dependency_graph):
    execution_order = ExecutionOrder(steps_dependency_graph)

    steps_without_dependencies = execution_order.steps_without_dependencies()

    assert steps_without_dependencies == {
        step
        for step in steps_dependency_graph
        if not any(steps_dependency_graph.neighbors(step))
    }


@given(steps_dependency_graph=steps_dependency_graph_builder)
@settings(
    suppress_health_check=(HealthCheck.too_slow, HealthCheck.filter_too_much),
    max_examples=1000,
    deadline=1200000,
)
@pytest.mark.run(order=-1)
def test_execution_order(steps_dependency_graph, request):
    cached_topsorts = request.config.cache.get(repr(steps_dependency_graph), default=[])
    expected = (
        cached_topsorts
        if cached_topsorts
        else ParallelGenerator(
            (
                tuple(reversed(topsort))
                for topsort in all_topological_sorts(steps_dependency_graph)
            ),
            max_lookahead=2,
        )
    )
    execution_order = ExecutionOrder(steps_dependency_graph)

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

            request.config.cache.set(
                repr(steps_dependency_graph), tuple(cached_topsorts)
            )
            assert done
