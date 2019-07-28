import itertools

import pytest
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

from bootsteps.blueprint import ExecutionOrder


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
