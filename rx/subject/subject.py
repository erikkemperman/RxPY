from sys import maxsize
from threading import RLock
from typing import Any, Dict, Optional

from rx.disposable import Disposable
from rx.core import Observable, Observer, typing
from rx.internal import DisposedException

from .innersubscription import InnerSubscription


class Subject(Observable, Observer, typing.Subject):
    """Represents an object that is both an observable sequence as well
    as an observer. Each notification is broadcasted to all subscribed
    observers.
    """

    def __init__(self) -> None:
        """Creates a new instance of Subject."""

        super().__init__()

        self.is_disposed = False
        self.exception: Optional[Exception] = None
        self.gen_id = ~maxsize
        self.observers: Dict[int, Any] = {}

        self.lock = RLock()

    def check_disposed(self) -> None:
        if self.is_disposed:
            raise DisposedException()

    def _gen_id(self):
        with self.lock:
            obs_id = self.gen_id
            self.gen_id += 1
        return obs_id

    def _subscribe_core(self,
                        on_next: Optional[typing.OnNext] = None,
                        on_error: Optional[typing.OnError] = None,
                        on_completed: Optional[typing.OnCompleted] = None,
                        scheduler: Optional[typing.Scheduler] = None
                        ) -> typing.Disposable:
        with self.lock:
            obs_id = self._gen_id()
            self.check_disposed()
            if not self.is_stopped:
                self.observers[obs_id] = on_next, on_error, on_completed
                return InnerSubscription(self, obs_id)

            if self.exception is not None:
                if on_error is not None:
                    on_error(self.exception)
            elif on_completed is not None:
                on_completed()
            return Disposable()

    def on_next(self, value: Any) -> None:
        """Notifies all subscribed observers with the value.

        Args:
            value: The value to send to all subscribed observers.
        """

        with self.lock:
            self.check_disposed()
        super().on_next(value)

    def _on_next_core(self, value: Any) -> None:
        with self.lock:
            observers = self.observers.copy().values()

        for on_next, on_error, on_completed in observers:
            if on_next is not None:
                on_next(value)

    def on_error(self, error: Exception) -> None:
        """Notifies all subscribed observers with the exception.

        Args:
            error: The exception to send to all subscribed observers.
        """

        with self.lock:
            self.check_disposed()
        super().on_error(error)

    def _on_error_core(self, error: Exception) -> None:
        with self.lock:
            observers = self.observers.copy().values()
            self.observers.clear()
            self.exception = error

        for on_next, on_error, on_completed in observers:
            if on_error is not None:
                on_error(error)

    def on_completed(self) -> None:
        """Notifies all subscribed observers of the end of the sequence."""

        with self.lock:
            self.check_disposed()
        super().on_completed()

    def _on_completed_core(self) -> None:
        with self.lock:
            observers = self.observers.copy().values()
            self.observers.clear()

        for on_next, on_error, on_completed in observers:
            if on_completed is not None:
                on_completed()

    def dispose(self) -> None:
        """Unsubscribe all observers and release resources."""

        with self.lock:
            self.is_disposed = True
            self.observers.clear()
            self.exception = None
            super().dispose()
