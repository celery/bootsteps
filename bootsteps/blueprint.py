"""
Blueprint is a directed acyclic graph of Bootsteps executed according to their dependency order.

All non-dependent Bootsteps will be executed in parallel.
"""

import inspect
from enum import Enum

import attr
import trio
from dependencies import Injector, value
from eliot import ActionType, Field, MessageType
from networkx import (
    DiGraph,
    is_directed_acyclic_graph,
    isolates,
    strongly_connected_components,
)
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


@attr.s(auto_attribs=True, cmp=False)
class Blueprint:
    """A directed acyclic graph of Bootsteps."""

    steps: DiGraph = attr.ib(default=attr.NOTHING)
    name: str = "Blueprint"
    state: BlueprintState = attr.ib(default=BlueprintState.INITIALIZED, init=False)

    async def start(self):
        """Start executing the blueprint."""
        with START_BLUEPRINT.as_task(name=self.name):
            async with trio.open_nursery() as nursery:
                worker_threads = []
                for steps in self._ordered_steps:
                    for step in steps:
                        if callable(step):
                            if inspect.iscoroutinefunction(step):
                                nursery.start_soon(step)
                            else:
                                worker_threads.append(
                                    trio.run_sync_in_worker_thread(step)
                                )
                        else:
                            if inspect.iscoroutinefunction(step.start):
                                nursery.start_soon(step.start)
                            else:
                                worker_threads.append(
                                    trio.run_sync_in_worker_thread(step.start)
                                )

                for worker_thread in worker_threads:
                    await worker_thread

    def stop(self):
        """Stop the blueprint."""

    @property
    def _ordered_steps(self):
        # TODO: Rename this property
        # TODO: Figure out how to paralelize boot steps when a last step is present
        with RESOLVING_BOOTSTEPS_EXECUTION_ORDER(name=self.name) as action:
            execution_order = []
            steps = self.steps.copy()

            parallelized_steps = 0

            # Find all the bootsteps without dependencies.
            steps_without_dependencies = list(isolates(steps))
            # Continue looping while the graph is not empty.
            while steps.order():
                if not steps_without_dependencies:
                    # Find the bootstep(s) that have the most dependencies
                    # and execute them so that they'll be free for parallel execution.
                    most_dependent_steps = max(strongly_connected_components(steps))
                    if len(most_dependent_steps) > 1:
                        parallelized_steps += len(most_dependent_steps)
                    # TODO: Add an assert that ensures this message is emitted
                    NEXT_BOOTSTEPS.log(
                        name=self.name, next_bootsteps=most_dependent_steps
                    )
                    yield most_dependent_steps
                    steps.remove_nodes_from(most_dependent_steps)
                    execution_order.extend(most_dependent_steps)
                    # Find all the bootsteps without dependencies.
                    steps_without_dependencies = list(isolates(steps))
                else:
                    if len(steps_without_dependencies) > 1:
                        parallelized_steps += len(steps_without_dependencies)
                    # TODO: Add an assert that ensures this message is emitted
                    NEXT_BOOTSTEPS.log(
                        name=self.name, next_bootsteps=steps_without_dependencies
                    )
                    # Execute all nodes without dependencies since they can now run.
                    yield steps_without_dependencies
                    steps.remove_nodes_from(steps_without_dependencies)
                    execution_order.extend(steps_without_dependencies)
                    steps_without_dependencies = None
            action.addSuccessFields(
                name=self.name,
                bootsteps_execution_order=execution_order,
                parallelized_steps=parallelized_steps,
            )


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

            dependencies = {bootstep: bootstep.requires for bootstep in bootsteps}
            for bootstep in bootsteps:
                for dependet_bootstep in bootstep.required_by:
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
