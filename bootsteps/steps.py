"""Steps are objects which define how to run a step during initialization."""
import typing
from collections import abc

import attr


@attr.s(auto_attribs=True, cmp=True)
class Step(abc.Callable):
    """A step in the initialization process of the program."""

    requires: typing.Set["Step"] = set()
    required_by: typing.Set["Step"] = set()
    last: bool = False

    def include_if(self) -> bool:
        """Conditionally execute this step according to some criteria."""
        return True


@attr.s(auto_attribs=True, cmp=True)
class AsyncStep(abc.Callable):
    """A step in the initialization process of the program."""

    requires: typing.Set["Step"] = set()
    required_by: typing.Set["Step"] = set()
    last: bool = False

    def include_if(self) -> bool:
        """Conditionally execute this step according to some criteria."""
        return True
