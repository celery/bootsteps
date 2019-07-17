"""Steps are objects which define how to run a step during initialization."""
import typing

import attr


@attr.s(auto_attribs=True, cmp=True)
class Step:
    """A step in the initialization process of the program."""

    requires: typing.List['Step']
