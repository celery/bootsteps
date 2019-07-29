import pytest
from hypothesis import strategies as st

from bootsteps import Blueprint
from tests.strategies import non_isomorphic_graph_builder

previous_graphs = []
steps_dependency_graph_builder = non_isomorphic_graph_builder(
    node_keys=st.functions(lambda: None),
    min_nodes=20,
    max_nodes=20,
    min_edges=10,
    self_loops=False,
    connected=True,
    previous_graphs=previous_graphs,
)


def test_benchmark_blueprint_start(benchmark):
    graph = steps_dependency_graph_builder.example()
    blueprint = Blueprint(graph)

    benchmark(blueprint.start)


def test_benchmark_blueprint_stop(benchmark):
    graph = steps_dependency_graph_builder.example()
    blueprint = Blueprint(graph)

    benchmark(blueprint.stop)
