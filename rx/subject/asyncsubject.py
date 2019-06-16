from typing import Any, Optional

from rx.disposable import Disposable
from rx.core import typing

from .subject import Subject
from .innersubscription import InnerSubscription


class AsyncSubject(Subject):
    """Represents the result of an asynchronous operation. The last value
    before the close notification, or the error received through
    on_error, is sent to all subscribed observers."""

    def __init__(self) -> None:
        """Creates a subject that can only receive one value and that value is
        cached for all future observations."""

        super().__init__()

        self.value = None
        self.has_value = False

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
                return InnerSubscription(self, obs_id)

            ex = self.exception
            has_value = self.has_value
            value = self.value

        if ex:
            if on_error is not None:
                on_error(ex)
        elif has_value:
            if on_next is not None:
                on_next(value)
            if on_completed is not None:
                on_completed()
        elif on_completed is not None:
            on_completed()

        return Disposable()

    def _on_next_core(self, value: Any) -> None:
        """Remember the value. Upon completion, the most recently received value
        will be passed on to all subscribed observers.

        Args:
            value: The value to remember until completion
        """
        with self.lock:
            self.value = value
            self.has_value = True

    def _on_completed_core(self) -> None:
        """Notifies all subscribed observers of the end of the sequence. The
        most recently received value, if any, will now be passed on to all
        subscribed observers."""

        with self.lock:
            observers = self.observers.copy().values()
            self.observers.clear()
            value = self.value
            has_value = self.has_value

        if has_value:
            for on_next, on_error, on_completed in observers:
                if on_next is not None:
                    on_next(value)
                if on_completed is not None:
                    on_completed()
        else:
            for on_next, on_error, on_completed in observers:
                if on_completed is not None:
                    on_completed()

    def dispose(self) -> None:
        """Unsubscribe all observers and release resources."""

        with self.lock:
            self.value = None
            super().dispose()
