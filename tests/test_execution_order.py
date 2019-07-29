import itertools

import pytest
from hypothesis import HealthCheck, given, settings
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

    assert execution_order._steps_dependency_graph == bootsteps_graph


@given(steps_dependency_graph=steps_dependency_graph_builder)
@settings(
    suppress_health_check=(HealthCheck.too_slow,), max_examples=1000, deadline=1200000
)
@pytest.mark.run(order=-1)
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
