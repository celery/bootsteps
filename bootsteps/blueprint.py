"""
Blueprint is a directed acyclic graph of Bootsteps executed according to their dependency order.

All non-dependent Bootsteps will be executed in parallel.
"""

import inspect
import math
import typing
from collections import abc
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

STOP_BLUEPRINT = ActionType(
    "bootsteps:blueprint:stop", [Field("name", str, "The name of the blueprint")], []
)

NEXT_BOOTSTEPS = MessageType(
    "bootsteps:blueprint:next_bootsteps",
    [
        Field("name", str, "The name of the blueprint"),
        Field("next_bootsteps", lambda steps: [repr(s) for s in steps]),
    ],
)

EXECUTING_BOOTSTEP = ActionType(
    "bootsteps:blueprint:executing_bootstep",
    [Field("bootstep", repr, "The name of the step")],
    [],
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
class ExecutionOrder(abc.Iterator):
    _steps_dependency_graph: DiGraph = attr.ib(default=attr.NOTHING)
    _execution_order: typing.List[Step] = attr.ib(
        default=attr.Factory(list), init=False
    )
    _steps_without_dependencies: typing.List[Step] = attr.ib(
        default=attr.Factory(list), init=False
    )
    _current_steps_dependency_graph: DiGraph = attr.ib(default=attr.NOTHING, init=False)

    def __attrs_post_init__(self):
        self._current_steps_dependency_graph = self._steps_dependency_graph
        self._steps_without_dependencies = self.steps_without_dependencies()

    def steps_without_dependencies(self):
        return {
            step
            for step in self._current_steps_dependency_graph
            if not any(self._current_steps_dependency_graph.neighbors(step))
        }

    def mark_as_pending_execution(self, steps):
        self._current_steps_dependency_graph = self._current_steps_dependency_graph.subgraph(
            self._current_steps_dependency_graph.nodes - steps
        )
        self._execution_order.append(steps)

    def is_steps_dependency_graph_empty(self) -> bool:
        return not self._current_steps_dependency_graph.order()

    def __next__(self):
        assert is_directed_acyclic_graph(self._current_steps_dependency_graph)

        # Continue looping while the graph is not empty.
        if self.is_steps_dependency_graph_empty():
            raise StopIteration

        # Execute all nodes without dependencies since they can now run.
        steps = self._steps_without_dependencies
        self.mark_as_pending_execution(steps)

        self._steps_without_dependencies = self.steps_without_dependencies()

        return steps

    def __iter__(self):
        return iter(self._execution_order) if self._execution_order else self


def _apply_step(nursery, step):
    if inspect.isawaitable(step):

        async def _inner():
            with EXECUTING_BOOTSTEP(bootstep=step):
                await step()

        nursery.start_soon(_inner)
    else:

        def _inner():
            with EXECUTING_BOOTSTEP(bootstep=step):
                step()

        nursery.start_soon(trio.run_sync_in_worker_thread, _inner)


@attr.s(auto_attribs=True, cmp=False)
class Blueprint:
    """A directed acyclic graph of Bootsteps."""

    _steps: DiGraph
    execution_order_strategy_class: abc.Iterator = attr.ib(
        default=ExecutionOrder, kw_only=True
    )
    name: str = attr.ib(default="Blueprint", kw_only=True)
    state: BlueprintState = attr.ib(default=BlueprintState.INITIALIZED, init=False)

    def __attrs_post_init__(self):
        """Initialize the state changes memory channel."""
        self.state_changes_send_channel, self.state_changes_receive_channel = trio.open_memory_channel(
            math.inf
        )
        self._nursery = self._nursery_manager = None

    def _change_blueprint_state(self, state: BlueprintState) -> None:
        self.state = state
        self.state_changes_send_channel.send_nowait(state)

    async def start(self) -> None:
        """Execte the blueprint's steps start method."""
        with START_BLUEPRINT.as_task(name=self.name):
            self._change_blueprint_state(BlueprintState.RUNNING)

            try:
                for steps in self.execution_order:
                    NEXT_BOOTSTEPS.log(name=self.name, next_bootsteps=steps)
                    async with trio.open_nursery() as nursery:
                        for step in steps:
                            if callable(step):
                                _apply_step(nursery, step)
                            else:
                                _apply_step(nursery, step.start)
            except Exception as e:
                self._change_blueprint_state((BlueprintState.FAILED, e))
                raise
            else:
                self._change_blueprint_state(BlueprintState.COMPLETED)

    async def stop(self) -> None:
        """Execte the blueprint's steps stop method."""
        with STOP_BLUEPRINT.as_task(name=self.name):
            self._change_blueprint_state(BlueprintState.TERMINATING)

            try:
                for steps in reversed(self.execution_order):
                    stop_steps = [step for step in steps if hasattr(step, "stop")]
                    if stop_steps:
                        NEXT_BOOTSTEPS.log(name=self.name, next_bootsteps=stop_steps)
                        async with trio.open_nursery() as nursery:
                            for step in stop_steps:
                                _apply_step(nursery, step.stop)
            except Exception as e:
                self._change_blueprint_state((BlueprintState.FAILED, e))
                raise
            else:
                self._change_blueprint_state(BlueprintState.TERMINATED)

    @cached_property
    def execution_order(self) -> typing.Iterator:
        """Initialize the execution order iterator."""
        return self.execution_order_strategy_class(self._steps)

    async def __aenter__(self) -> "Blueprint":
        """Start the blueprint."""
        self._nursery_manager = trio.open_nursery()
        self._nursery = await self._nursery_manager.__aenter__()
        self._nursery.start_soon(self.start)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> "Blueprint":
        """Stop the blueprint."""
        try:
            self._nursery.start_soon(self.stop)
        finally:
            await self._nursery_manager.__aexit__(exc_type, exc, tb)


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
