"""
Blueprint is a directed acyclic graph of Bootsteps executed according to their dependency order.

All non-dependent Bootsteps will be executed in parallel.
"""

import attr
from enum import Enum
from networkx import DiGraph, topological_sort
from eliot import Message, start_action, start_task
from dependencies import Injector, value


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

    def stop(self):
        """Stop the blueprint."""


class BlueprintContainer(Injector):
    """A container which constructs a dependency graph of Bootsteps."""

    bootsteps = []

    @value
    def steps(bootsteps):
        """Initialize a directed acyclic graph of steps."""
        return DiGraph({
            bootstep: bootstep.requires for bootstep in bootsteps
        })

    blueprint = Blueprint
