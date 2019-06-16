from typing import Any, Optional

from rx.core import typing
from rx.disposable import Disposable

from .subject import Subject
from .innersubscription import InnerSubscription


class BehaviorSubject(Subject):
    """Represents a value that changes over time. Observers can
    subscribe to the subject to receive the last (or initial) value and
    all subsequent notifications.
    """

    def __init__(self, value) -> None:
        """Initializes a new instance of the BehaviorSubject class which
        creates a subject that caches its last value and starts with the
        specified value.

        Args:
            value: Initial value sent to observers when no other value has been
                received by the subject yet.
        """

        super().__init__()

        self.value = value

    def _subscribe_core(self,
                        on_next: Optional[typing.OnNext] = None,
                        on_error: Optional[typing.OnError] = None,
                        on_completed: Optional[typing.OnCompleted] = None,
                        scheduler: Optional[typing.Scheduler] = None
                        ) -> typing.Disposable:
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                obs_id = self._gen_id()
                self.observers[obs_id] = on_next, on_error, on_completed
                if on_next is not None:
                    on_next(self.value)
                return InnerSubscription(self, obs_id)
            ex = self.exception

        if ex:
            if on_error is not None:
                on_error(ex)
        else:
            if on_completed is not None:
                on_completed()

        return Disposable()

    def _on_next_core(self, value: Any) -> None:
        """Notifies all subscribed observers with the value."""
        with self.lock:
            observers = self.observers.copy().values()
            self.value = value

        for on_next, on_error, on_completed in observers:
            if on_next is not None:
                on_next(value)

    def dispose(self) -> None:
        """Release all resources.

        Releases all resources used by the current instance of the
        BehaviorSubject class and unsubscribe all observers.
        """

        with self.lock:
            self.value = None
            super().dispose()
