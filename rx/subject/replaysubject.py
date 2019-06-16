import sys

from collections import deque
from datetime import datetime
from typing import cast, Any, Deque, Dict, NamedTuple, Optional
from datetime import timedelta

from rx.core import typing
from rx.scheduler import current_thread_scheduler
from rx.core.observer.scheduledobserver import ScheduledObserver

from .subject import Subject


class RemovableDisposable(typing.Disposable):
    def __init__(self, subject, observer, obs_id):
        self.subject = subject
        self.observer = observer
        self.obs_id = obs_id

    def dispose(self):
        self.observer.dispose()
        if not self.subject.is_disposed and self.observer in self.subject.observers:
            try:
                del self.subject.observers[self.obs_id]
            except KeyError:
                pass


class QueueItem(NamedTuple):
    interval: datetime
    value: Any


class ReplaySubject(Subject):
    """Represents an object that is both an observable sequence as well
    as an observer. Each notification is broadcasted to all subscribed
    and future observers, subject to buffer trimming policies.
    """

    def __init__(self,
                 buffer_size: int = None,
                 window: typing.RelativeTime = None,
                 scheduler: Optional[typing.Scheduler] = None
                 ) -> None:
        """Initializes a new instance of the ReplaySubject class with
        the specified buffer size, window and scheduler.

        Args:
            buffer_size: [Optional] Maximum element count of the replay
                buffer.
            window [Optional]: Maximum time length of the replay buffer.
            scheduler: [Optional] Scheduler the observers are invoked on.
        """

        super().__init__()
        self.buffer_size = sys.maxsize if buffer_size is None else buffer_size
        self.scheduler = scheduler or current_thread_scheduler
        self.window = timedelta.max if window is None else self.scheduler.to_timedelta(window)
        self.queue: Deque[QueueItem] = deque()

    def _subscribe_core(self,
                        on_next: Optional[typing.OnNext] = None,
                        on_error: Optional[typing.OnError] = None,
                        on_completed: Optional[typing.OnCompleted] = None,
                        scheduler: Optional[typing.Scheduler] = None
                        ) -> typing.Disposable:
        so = ScheduledObserver(self.scheduler, on_next, on_error, on_completed)

        with self.lock:
            self.check_disposed()
            self._trim(self.scheduler.now)
            obs_id = self._gen_id()
            self.observers[obs_id] = so
            subscription = RemovableDisposable(self, so, obs_id)

            for item in self.queue:
                so.on_next(item.value)

            if self.exception is not None:
                so.on_error(self.exception)
            elif self.is_stopped:
                so.on_completed()

        so.ensure_active()
        return subscription

    def _trim(self, now: datetime):
        while len(self.queue) > self.buffer_size:
            self.queue.popleft()

        while self.queue and (now - self.queue[0].interval) > self.window:
            self.queue.popleft()

    def _on_next_core(self, value: Any) -> None:
        """Notifies all subscribed observers with the value."""

        with self.lock:
            observers = self.observers.copy().values()
            now = self.scheduler.now
            self.queue.append(QueueItem(interval=now, value=value))
            self._trim(now)

        for observer in observers:
            observer.on_next(value)

        for observer in observers:
            cast(ScheduledObserver, observer).ensure_active()

    def _on_error_core(self, error: Exception) -> None:
        """Notifies all subscribed observers with the exception."""

        with self.lock:
            observers = self.observers.copy().values()
            self.observers.clear()
            self.exception = error
            now = self.scheduler.now
            self._trim(now)

        for observer in observers:
            observer.on_error(error)
            cast(ScheduledObserver, observer).ensure_active()

    def _on_completed_core(self) -> None:
        """Notifies all subscribed observers of the end of the sequence."""

        with self.lock:
            observers = self.observers.copy().values()
            self.observers.clear()
            now = self.scheduler.now
            self._trim(now)

        for observer in observers:
            observer.on_completed()
            cast(ScheduledObserver, observer).ensure_active()

    def dispose(self) -> None:
        """Releases all resources used by the current instance of the
        ReplaySubject class and unsubscribe all observers."""

        with self.lock:
            self.queue.clear()
            super().dispose()
