"""
Blueprint is a directed acyclic graph of Bootsteps executed according to their dependency order.

All non-dependent Bootsteps will be executed in parallel.
"""

import inspect
import typing
from collections.abc import Iterator
from enum import Enum

import attr
import trio
from cached_property import cached_property
from dependencies import Injector, value
from eliot import ActionType, Field, MessageType
from networkx import DiGraph, is_directed_acyclic_graph
from networkx.readwrite import json_graph

from bootsteps.steps import Step


def _serialize_graph(graph):
    graph = json_graph.adjacency_data(graph)
    graph["nodes"] = [
        {"id": n["id"] if not isinstance(n["id"], Step) else repr(n["id"])}
        for n in graph["nodes"]
    ]
    graph["adjacency"] = [
        [
            {"id": a["id"] if not isinstance(a["id"], Step) else repr(a["id"])}
            for a in adj
        ]
        for adj in graph["adjacency"]
    ]

    return graph


BUILDING_DEPENDENCY_GRAPH = ActionType(
    "bootsteps:blueprint:building_dependency_graph",
    [Field("name", str, "The name of the blueprint")],
    [
        Field("name", str, "The name of the blueprint"),
        Field("graph", _serialize_graph, "The resulting graph"),
    ],
)

START_BLUEPRINT = ActionType(
    "bootsteps:blueprint:start", [Field("name", str, "The name of the blueprint")], []
)

RESOLVING_BOOTSTEPS_EXECUTION_ORDER = ActionType(
    "bootsteps:blueprint:resolving_bootsteps_execution_order",
    [Field("name", str, "The name of the blueprint")],
    [
        Field("name", str, "The name of the blueprint"),
        Field("bootsteps_execution_order", lambda steps: [repr(s) for s in steps]),
        Field("parallelized_steps", int),
    ],
)

NEXT_BOOTSTEPS = MessageType(
    "bootsteps:blueprint:next_bootsteps",
    [
        Field("name", str, "The name of the blueprint"),
        Field("next_bootsteps", lambda steps: [repr(s) for s in steps]),
    ],
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


@attr.s(auto_attribs=True, cmp=False, slots=True)
class ExecutionOrder(Iterator):
    _steps_dependency_graph: DiGraph = attr.ib(default=attr.NOTHING)
    _execution_order: typing.List[Step] = attr.ib(
        default=attr.Factory(list), init=False
    )
    _steps_without_dependencies: typing.List[Step] = attr.ib(
        default=attr.Factory(list), init=False
    )

    def __attrs_post_init__(self):
        self._steps_without_dependencies = self.steps_without_dependencies()

    def steps_without_dependencies(self):
        return {
            step
            for step in self._steps_dependency_graph
            if not any(self._steps_dependency_graph.neighbors(step))
        }

    def mark_as_done(self, steps):
        self._steps_dependency_graph.remove_nodes_from(steps)
        self._execution_order.append(steps)

    def is_steps_dependency_graph_empty(self) -> bool:
        return not self._steps_dependency_graph.order()

    def __next__(self):
        assert is_directed_acyclic_graph(self._steps_dependency_graph)

        # Continue looping while the graph is not empty.
        if self.is_steps_dependency_graph_empty():
            raise StopIteration

        # Execute all nodes without dependencies since they can now run.
        steps = self._steps_without_dependencies
        self.mark_as_done(steps)

        self._steps_without_dependencies = self.steps_without_dependencies()

        return steps

    def __iter__(self):
        return iter(self._execution_order) if self._execution_order else self


@attr.s(auto_attribs=True, cmp=False)
class Blueprint:
    """A directed acyclic graph of Bootsteps."""

    _steps: DiGraph
    execution_order_strategy_class: Iterator = attr.ib(
        default=ExecutionOrder, kw_only=True
    )
    name: str = attr.ib(default="Blueprint", kw_only=True)
    state: BlueprintState = attr.ib(default=BlueprintState.INITIALIZED, init=False)

    async def start(self):
        """Start executing the blueprint."""
        with START_BLUEPRINT.as_task(name=self.name):
            for steps in self.execution_order:
                async with trio.open_nursery() as nursery:
                    for step in steps:
                        if callable(step):
                            if inspect.isawaitable(step):
                                nursery.start_soon(step)
                            else:
                                nursery.start_soon(trio.run_sync_in_worker_thread, step)
                        else:
                            if inspect.isawaitable(step.start):
                                nursery.start_soon(step.start)
                            else:
                                nursery.start_soon(
                                    trio.run_sync_in_worker_thread, step.start
                                )

    def stop(self):
        """Stop the blueprint."""

    @cached_property
    def execution_order(self):
        """Calculate the order of execution of steps.

        The algorithm searches for nodes with no neighbors, that is steps without dependencies
        and schedules all of them for execution at once.

        If there are none, the algorithm attempts to search a step or a number of steps
        which most steps are dependent on and executes it.
        """
        return self.execution_order_strategy_class(self._steps.copy())


class BlueprintContainer(Injector):
    """A container which constructs a dependency graph of Bootsteps."""

    bootsteps = []
    name = "Blueprint"

    @value
    def steps(name, bootsteps):
        """Initialize a directed acyclic graph of steps."""
        with BUILDING_DEPENDENCY_GRAPH(name=name) as action:
            last_bootsteps = [bootstep for bootstep in bootsteps if bootstep.last]
            if len(last_bootsteps) > 1:
                raise ValueError(
                    f"Only one boot step can be last. Found {len(last_bootsteps)}."
                )

            dependencies = {
                bootstep: bootstep.requires
                for bootstep in bootsteps
                if bootstep.include_if()
            }
            for bootstep in bootsteps:
                for dependet_bootstep in bootstep.required_by:
                    if dependet_bootstep.include_if():
                        dependencies[dependet_bootstep].add(bootstep)
            graph = DiGraph(dependencies)

            if last_bootsteps:
                last = last_bootsteps[0]
                for bootstep in graph:
                    if bootstep != last:
                        graph.add_edge(last, bootstep)

            if not is_directed_acyclic_graph(graph):
                raise ValueError("Circular dependencies found.")

            action.addSuccessFields(name=name, graph=graph)

        return graph

    blueprint = Blueprint
