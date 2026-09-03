"""The process exit codes the controller gives meaning to.

Only the codes relevant to a Python adapter live here; the full list is
``EXIT_CODES`` in ``@iobroker/js-controller-common-db``. See ``doc/PYTHON.md``,
section "Stopping, exit codes, restarts", for how the controller reacts to each.
"""

from __future__ import annotations

from enum import IntEnum

__all__ = ["ExitCode"]


class ExitCode(IntEnum):
    """Exit codes an adapter process may end with."""

    #: Clean end. A ``schedule``/``once`` instance is done; an enabled ``daemon``
    #: is restarted after 30 s (a daemon is not supposed to exit).
    NO_ERROR = 0

    #: Unhandled exception. The instance is restarted and the exit is counted
    #: towards restart-loop detection (three without a quiet spell stop further
    #: restarts). ``run()`` exits with this on an exception it did not expect.
    UNCAUGHT_EXCEPTION = 6

    #: Planned stop requested by the adapter itself; the instance is **not**
    #: restarted. This is what :meth:`Adapter.terminate` uses.
    ADAPTER_REQUESTED_TERMINATION = 11

    #: Ask the controller to restart the instance after 1 s.
    START_IMMEDIATELY_AFTER_STOP = 156
