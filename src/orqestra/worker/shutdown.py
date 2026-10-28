"""Process-level graceful shutdown: SIGTERM and SIGINT become stop requests (DEC-049).

POSIX only. ``run_until_signalled`` runs a worker on the main thread with its
own handler for SIGTERM and SIGINT:

- the first signal it processes asks the worker to stop (``request_stop``):
  the worker drains the task it holds and ``run`` returns, or the drain guard
  ends the process (exit 9);
- any later signal it processes before ``run`` has returned ends the process
  at once (exit 9, ``shutdown_forced``).

The handler takes no lock. Python runs it on the main thread between two
bytecodes of whatever that thread is doing, possibly while it holds a lock,
so a handler that waited for one could deadlock. It only numbers its runs
with ``itertools.count`` (one C call, so a handler nested in another still
gets its own number), sets the worker's flag and writes to the worker's wake
pipe. That pipe is also the process's wakeup fd: a signal the kernel delivers
to another thread cannot interrupt the main thread, but its byte still ends
the supervisor's wait at once. Identical signals that arrive before the
handler runs are one signal to POSIX, so two quick signals may count once; a
later one still forces the exit.

The previous handlers and wakeup fd are restored when ``run`` ends, however
it ends; a later signal reaches what handled it before, never the worker.
"""

import itertools
import signal
import threading
from collections.abc import Callable
from types import FrameType

from orqestra.worker.fence import ExitCode, end_process
from orqestra.worker.runtime import Terminate, Worker

SIGNALS = (signal.SIGTERM, signal.SIGINT)

type SignalHandler = Callable[[int, FrameType | None], None]


def stop_on_signal(worker: Worker, terminate: Terminate = end_process) -> SignalHandler:
    """The handler: the first signal requests a stop, a later one ends the process."""
    ordinals = itertools.count()

    def handle(signum: int, frame: FrameType | None) -> None:
        if next(ordinals) == 0:
            worker.request_stop()
        elif not worker.finished:
            terminate(
                ExitCode.SHUTDOWN,
                "shutdown_forced",
                f"a second termination signal ({signum}) ended the process",
                {"worker_id": worker.worker_id},
            )

    return handle


def run_until_signalled(worker: Worker, *, terminate: Terminate = end_process) -> None:
    """Run ``worker`` on this, the main, thread until SIGTERM or SIGINT stops it.

    It refuses, changing nothing, off the main thread or when a previous
    handler was installed outside Python and so cannot be restored. It
    refuses, after putting it back, when another wakeup fd is set (an event
    loop's, say). ``run``'s exceptions propagate, so the process exits nonzero.
    """
    if threading.current_thread() is not threading.main_thread():
        msg = "signal handlers can only be installed on the main thread"
        raise RuntimeError(msg)
    previous = {sig: signal.getsignal(sig) for sig in SIGNALS}
    if any(handler is None for handler in previous.values()):
        msg = "a SIGTERM or SIGINT handler installed outside Python cannot be restored"
        raise RuntimeError(msg)
    handle = stop_on_signal(worker, terminate)
    installed: list[signal.Signals] = []
    old_fd = signal.set_wakeup_fd(worker.wakeup_fd, warn_on_full_buffer=False)
    try:
        if old_fd != -1:
            msg = f"another wakeup fd ({old_fd}) is set; it is left in place"
            raise RuntimeError(msg)
        for sig in SIGNALS:
            signal.signal(sig, handle)
            installed.append(sig)
        worker.run()
    finally:
        try:
            for sig in reversed(installed):
                signal.signal(sig, previous[sig])
        finally:
            signal.set_wakeup_fd(old_fd)
