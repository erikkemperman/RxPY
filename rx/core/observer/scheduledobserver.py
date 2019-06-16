import threading
from collections import deque
from typing import Any, Deque, Optional

from rx.core import typing
from rx.disposable import SerialDisposable

from .observer import Observer


class ScheduledObserver(Observer):
    def __init__(self,
                 scheduler: typing.Scheduler,
                 on_next: Optional[typing.OnNext] = None,
                 on_error: Optional[typing.OnError] = None,
                 on_completed: Optional[typing.OnCompleted] = None,
                 ) -> None:
        super().__init__(on_next, on_error, on_completed)

        self.scheduler = scheduler

        self.lock = threading.RLock()
        self.is_acquired = False
        self.has_faulted = False
        self.queue: Deque[typing.Action] = deque()
        self.disposable = SerialDisposable()

        # Note to self: list append is thread safe
        # http://effbot.org/pyfaq/what-kinds-of-global-value-mutation-are-thread-safe.htm

    def _on_next_core(self, value: Any) -> None:
        sup = super()._on_next_core

        def action():
            sup(value)
        self.queue.append(action)

    def _on_error_core(self, error: Exception) -> None:
        sup = super()._on_error_core

        def action():
            sup(error)
        self.queue.append(action)

    def _on_completed_core(self) -> None:
        sup = super()._on_completed_core

        def action():
            sup()
        self.queue.append(action)

    def ensure_active(self) -> None:
        is_owner = False

        with self.lock:
            if not self.has_faulted and self.queue:
                is_owner = not self.is_acquired
                self.is_acquired = True

        if is_owner:
            self.disposable.disposable = self.scheduler.schedule(self.run)

    def run(self, scheduler: typing.Scheduler, state: typing.TState) -> None:
        parent = self

        with self.lock:
            if parent.queue:
                work = parent.queue.popleft()
            else:
                parent.is_acquired = False
                return

        try:
            work()
        except Exception:
            with self.lock:
                parent.queue.clear()
                parent.has_faulted = True
            raise

        self.scheduler.schedule(self.run)

    def dispose(self) -> None:
        super().dispose()
        self.disposable.dispose()
