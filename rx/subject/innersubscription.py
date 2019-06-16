from threading import RLock
from typing import Optional

from rx.core import typing


class InnerSubscription(typing.Disposable):
    def __init__(self,
                 subject,
                 obs_id: int
                 ) -> None:
        self.subject = subject
        self.obs_id: Optional[int] = obs_id
        self.lock = RLock()

    def dispose(self) -> None:
        with self.lock:
            if not self.subject.is_disposed and self.obs_id is not None:
                try:
                    del self.subject.observers[self.obs_id]
                except KeyError:
                    pass
                self.obs_id = None
