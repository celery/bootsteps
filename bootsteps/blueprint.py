"""
Blueprint is a directed acyclic graph of Bootsteps executed according to their dependency order.

All non-dependent Bootsteps will be executed in parallel.
"""

from enum import Enum

import attr
from cached_property import cached_property
from dependencies import Injector, value
from eliot import ActionType, Field
from networkx import DiGraph, is_directed_acyclic_graph, topological_sort
from networkx.readwrite import json_graph

from bootsteps.steps import Step


def _serialize_graph(graph):
    graph = json_graph.adjacency_data(graph)
    graph['nodes'] = [
        {
            'id': n['id'] if not isinstance(n['id'], Step) else repr(n['id'])
         } for n in graph['nodes']
    ]
    graph['adjacency'] = [
        [
            {
                'id': a['id'] if not isinstance(a['id'], Step) else repr(a['id'])
            } for a in adj
        ] for adj in graph['adjacency']
    ]

    return graph


BUILDING_DEPENDENCY_GRAPH = ActionType(
    "bootsteps:blueprint:building_dependency_graph",
    [Field("name", str, "The name of the blueprint")],
    [Field("name", str, "The name of the blueprint"), Field("graph", _serialize_graph, "The resulting graph")]
)


class BlueprintState(Enum):
    """An enum represeting the different lifecycle stages of a Blueprint."""

    INITIALIZED = "initialized"
    RUNNING = "running"
    COMPLETED = "completed"
    TERMINATING = "terminating"
    TERMINATED = "terminated"
    RESTARTING = "restarting"
    FAILED = "failed"


@attr.s(auto_attribs=True, cmp=False)
class Blueprint:
    """A directed acyclic graph of Bootsteps."""

    steps: DiGraph = attr.ib(default=attr.NOTHING)
    name: str = "Blueprint"
    state: BlueprintState = attr.ib(default=BlueprintState.INITIALIZED, init=False)

    def start(self):
        """Start executing the blueprint."""
        for step in reversed(self._ordered_steps):
            step()

    def stop(self):
        """Stop the blueprint."""

    @cached_property
    def _ordered_steps(self):
        return tuple(topological_sort(self.steps))


class BlueprintContainer(Injector):
    """A container which constructs a dependency graph of Bootsteps."""

    bootsteps = []
    name = "Blueprint"

    @value
    def steps(name, bootsteps):
        """Initialize a directed acyclic graph of steps."""
        with BUILDING_DEPENDENCY_GRAPH(name=name) as action:
            last_bootsteps = sum(True for bootstep in bootsteps if bootstep.last)
            if last_bootsteps > 1:
                raise ValueError(f"Only one boot step can be last. Found {last_bootsteps}.")

            graph = DiGraph({
                bootstep: bootstep.requires for bootstep in bootsteps
            })

            try:
                last = next(bootstep for bootstep in bootsteps if bootstep.last)
            except StopIteration:
                pass
            else:
                for bootstep in graph:
                    if bootstep != last:
                        graph.add_edge(last, bootstep)

            if not is_directed_acyclic_graph(graph):
                raise ValueError("Circular dependencies found.")

            action.addSuccessFields(name=name, graph=graph)

        return graph

    blueprint = Blueprint
