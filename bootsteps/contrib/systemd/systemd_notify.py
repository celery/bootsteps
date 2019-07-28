"""Steps to execute SystemD notification actions."""

import os
import typing

import attr

from bootsteps import Step

try:
    from systemd.daemon import notify
except ImportError:
    notify = None


@attr.s(auto_attribs=True, cmp=True)
class SystemDNotify(Step):
    """Notify the status of a SystemD service."""

    state: typing.Dict[str, str] = {}

    def __call__(self) -> None:
        """Update the SystemD service's status."""
        notify(
            status="\n".join(f"{name}={value}" for name, value in self.state.items())
        )

    def include_if(self) -> bool:
        """Only execute if the NOTIFY_SOCKET environment variable is set and if the systemd library is installed."""
        # TODO: Warn when notify socket is set and the SystemD library is not installed
        return "NOTIFY_SOCKET" in os.environ and bool(notify)
